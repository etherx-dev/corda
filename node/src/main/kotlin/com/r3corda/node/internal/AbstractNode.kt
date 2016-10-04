package com.r3corda.node.internal

import com.codahale.metrics.MetricRegistry
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import com.google.common.util.concurrent.SettableFuture
import com.r3corda.core.RunOnCallerThread
import com.r3corda.core.crypto.Party
import com.r3corda.core.crypto.X509Utilities
import com.r3corda.core.messaging.SingleMessageRecipient
import com.r3corda.core.messaging.createMessage
import com.r3corda.core.messaging.runOnNextMessage
import com.r3corda.core.node.CityDatabase
import com.r3corda.core.node.CordaPluginRegistry
import com.r3corda.core.node.NodeInfo
import com.r3corda.core.node.PhysicalLocation
import com.r3corda.core.node.services.*
import com.r3corda.core.node.services.NetworkMapCache.MapChangeType
import com.r3corda.core.protocols.ProtocolLogic
import com.r3corda.core.protocols.ProtocolLogicRefFactory
import com.r3corda.core.random63BitValue
import com.r3corda.core.seconds
import com.r3corda.core.serialization.SingletonSerializeAsToken
import com.r3corda.core.serialization.deserialize
import com.r3corda.core.serialization.serialize
import com.r3corda.core.transactions.SignedTransaction
import com.r3corda.node.api.APIServer
import com.r3corda.node.services.api.*
import com.r3corda.node.services.config.NodeConfiguration
import com.r3corda.node.services.events.NodeSchedulerService
import com.r3corda.node.services.events.ScheduledActivityObserver
import com.r3corda.node.services.identity.InMemoryIdentityService
import com.r3corda.node.services.keys.PersistentKeyManagementService
import com.r3corda.node.services.messaging.CordaRPCOps
import com.r3corda.node.services.network.InMemoryNetworkMapCache
import com.r3corda.node.services.network.NetworkMapService
import com.r3corda.node.services.network.NetworkMapService.Companion.REGISTER_PROTOCOL_TOPIC
import com.r3corda.node.services.network.NodeRegistration
import com.r3corda.node.services.network.PersistentNetworkMapService
import com.r3corda.node.services.persistence.*
import com.r3corda.node.services.statemachine.StateMachineManager
import com.r3corda.node.services.transactions.NotaryService
import com.r3corda.node.services.transactions.SimpleNotaryService
import com.r3corda.node.services.transactions.ValidatingNotaryService
import com.r3corda.node.services.vault.CashBalanceAsMetricsObserver
import com.r3corda.node.services.vault.NodeVaultService
import com.r3corda.node.utilities.*
import org.jetbrains.exposed.sql.Database
import org.slf4j.Logger
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyPair
import java.time.Clock
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

/**
 * A base node implementation that can be customised either for production (with real implementations that do real
 * I/O), or a mock implementation suitable for unit test environments.
 *
 * Marked as SingletonSerializeAsToken to prevent the invisible reference to AbstractNode in the ServiceHub accidentally
 * sweeping up the Node into the Kryo checkpoint serialization via any protocols holding a reference to ServiceHub.
 */
// TODO: Where this node is the initial network map service, currently no networkMapService is provided.
// In theory the NodeInfo for the node should be passed in, instead, however currently this is constructed by the
// AbstractNode. It should be possible to generate the NodeInfo outside of AbstractNode, so it can be passed in.
abstract class AbstractNode(val configuration: NodeConfiguration, val networkMapService: SingleMessageRecipient?,
                            val advertisedServices: Set<ServiceType>, val platformClock: Clock) : SingletonSerializeAsToken() {
    companion object {
        val PRIVATE_KEY_FILE_NAME = "identity-private-key"
        val PUBLIC_IDENTITY_FILE_NAME = "identity-public"
    }

    // TODO: Persist this, as well as whether the node is registered.
    /**
     * Sequence number of changes sent to the network map service, when registering/de-registering this node.
     */
    var networkMapSeq: Long = 1

    protected abstract val log: Logger

    // We will run as much stuff in this single thread as possible to keep the risk of thread safety bugs low during the
    // low-performance prototyping period.
    protected abstract val serverThread: AffinityExecutor

    // Objects in this list will be scanned by the DataUploadServlet and can be handed new data via HTTP.
    // Don't mutate this after startup.
    protected val _servicesThatAcceptUploads = ArrayList<AcceptsFileUpload>()
    val servicesThatAcceptUploads: List<AcceptsFileUpload> = _servicesThatAcceptUploads

    val services = object : ServiceHubInternal() {
        override val networkService: MessagingServiceInternal get() = net
        override val networkMapCache: NetworkMapCache get() = netMapCache
        override val storageService: TxWritableStorageService get() = storage
        override val vaultService: VaultService get() = vault
        override val keyManagementService: KeyManagementService get() = keyManagement
        override val identityService: IdentityService get() = identity
        override val schedulerService: SchedulerService get() = scheduler
        override val clock: Clock = platformClock

        // Internal only
        override val monitoringService: MonitoringService = MonitoringService(MetricRegistry())
        override val protocolLogicRefFactory: ProtocolLogicRefFactory get() = protocolLogicFactory

        override fun <T> startProtocol(loggerName: String, logic: ProtocolLogic<T>): ListenableFuture<T> {
            return smm.add(loggerName, logic).resultFuture
        }

        override fun recordTransactions(txs: Iterable<SignedTransaction>) = recordTransactionsInternal(storage, txs)
    }

    val info: NodeInfo by lazy {
        NodeInfo(net.myAddress, storage.myLegalIdentity, advertisedServices, findMyLocation())
    }

    open fun findMyLocation(): PhysicalLocation? = CityDatabase[configuration.nearestCity]

    lateinit var storage: TxWritableStorageService
    lateinit var checkpointStorage: CheckpointStorage
    lateinit var smm: StateMachineManager
    lateinit var vault: VaultService
    lateinit var keyManagement: KeyManagementService
    var inNodeNetworkMapService: NetworkMapService? = null
    var inNodeNotaryService: NotaryService? = null
    var uniquenessProvider: UniquenessProvider? = null
    lateinit var identity: IdentityService
    lateinit var net: MessagingServiceInternal
    lateinit var netMapCache: NetworkMapCache
    lateinit var api: APIServer
    lateinit var scheduler: SchedulerService
    lateinit var protocolLogicFactory: ProtocolLogicRefFactory
    val customServices: ArrayList<Any> = ArrayList()
    protected val runOnStop: ArrayList<Runnable> = ArrayList()
    lateinit var database: Database

    /** Locates and returns a service of the given type if loaded, or throws an exception if not found. */
    inline fun <reified T : Any> findService() = customServices.filterIsInstance<T>().single()

    var isPreviousCheckpointsPresent = false
        private set

    /** Completes once the node has successfully registered with the network map service */
    private val _networkMapRegistrationFuture: SettableFuture<Unit> = SettableFuture.create()
    val networkMapRegistrationFuture: ListenableFuture<Unit>
        get() = _networkMapRegistrationFuture

    /** Fetch CordaPluginRegistry classes registered in META-INF/services/com.r3corda.core.node.CordaPluginRegistry files that exist in the classpath */
    protected val pluginRegistries: List<CordaPluginRegistry> by lazy {
        ServiceLoader.load(CordaPluginRegistry::class.java).toList()
    }

    /** Set to true once [start] has been successfully called. */
    @Volatile var started = false
        private set

    open fun start(): AbstractNode {
        require(!started) { "Node has already been started" }

        if (configuration.devMode) {
            log.warn("Corda node is running in dev mode.")
            configuration.configureWithDevSSLCertificate()
        }
        require(hasSSLCertificates()) { "SSL certificates not found." }

        log.info("Node starting up ...")

        // Do all of this in a database transaction so anything that might need a connection has one.
        initialiseDatabasePersistence() {
            val storageServices = initialiseStorageService(configuration.basedir)
            storage = storageServices.first
            checkpointStorage = storageServices.second
            netMapCache = InMemoryNetworkMapCache()
            net = makeMessagingService()
            vault = makeVaultService()

            identity = makeIdentityService()

            // Place the long term identity key in the KMS. Eventually, this is likely going to be separated again because
            // the KMS is meant for derived temporary keys used in transactions, and we're not supposed to sign things with
            // the identity key. But the infrastructure to make that easy isn't here yet.
            keyManagement = makeKeyManagementService()
            api = APIServerImpl(this@AbstractNode)
            scheduler = NodeSchedulerService(services)

            protocolLogicFactory = initialiseProtocolLogicFactory()

            val tokenizableServices = mutableListOf(storage, net, vault, keyManagement, identity, platformClock, scheduler)

            customServices.clear()
            customServices.addAll(buildPluginServices(tokenizableServices))

            // TODO: uniquenessProvider creation should be inside makeNotaryService(), but notary service initialisation
            //       depends on smm, while smm depends on tokenizableServices, which uniquenessProvider is part of
            advertisedServices.singleOrNull { it.isSubTypeOf(NotaryService.Type) }?.let {
                uniquenessProvider = makeUniquenessProvider()
                tokenizableServices.add(uniquenessProvider!!)
            }

            smm = StateMachineManager(services,
                    listOf(tokenizableServices),
                    checkpointStorage,
                    serverThread,
                    database)
            if (serverThread is ExecutorService) {
                runOnStop += Runnable {
                    // We wait here, even though any in-flight messages should have been drained away because the
                    // server thread can potentially have other non-messaging tasks scheduled onto it. The timeout value is
                    // arbitrary and might be inappropriate.
                    MoreExecutors.shutdownAndAwaitTermination(serverThread as ExecutorService, 50, TimeUnit.SECONDS)
                }
            }

            buildAdvertisedServices()

            // TODO: this model might change but for now it provides some de-coupling
            // Add SMM observers
            ANSIProgressObserver(smm)
            // Add vault observers
            CashBalanceAsMetricsObserver(services)
            ScheduledActivityObserver(services)
        }

        startMessagingService(ServerRPCOps(services, smm, database))
        runOnStop += Runnable { net.stop() }
        _networkMapRegistrationFuture.setFuture(registerWithNetworkMap())
        isPreviousCheckpointsPresent = checkpointStorage.checkpoints.any()
        smm.start()
        started = true
        return this
    }

    private fun hasSSLCertificates(): Boolean {
        val keyStore = try {
            // This will throw exception if key file not found or keystore password is incorrect.
            X509Utilities.loadKeyStore(configuration.keyStorePath, configuration.keyStorePassword)
        } catch (e: Exception) {
            null
        }
        return keyStore?.containsAlias(X509Utilities.CORDA_CLIENT_CA) ?: false
    }

    // Specific class so that MockNode can catch it.
    class DatabaseConfigurationException(msg: String) : Exception(msg)

    protected open fun initialiseDatabasePersistence(insideTransaction: () -> Unit) {
        val props = configuration.dataSourceProperties
        if (props.isNotEmpty()) {
            val (toClose, database) = configureDatabase(props)
            this.database = database
            // Now log the vendor string as this will also cause a connection to be tested eagerly.
            log.info("Connected to ${database.vendor} database.")
            runOnStop += Runnable { toClose.close() }
            databaseTransaction(database) {
                insideTransaction()
            }
        } else {
            throw DatabaseConfigurationException("There must be a database configured.")
        }
    }

    private fun initialiseProtocolLogicFactory(): ProtocolLogicRefFactory {
        val protocolWhitelist = HashMap<String, Set<String>>()
        for (plugin in pluginRegistries) {
            for ((className, classWhitelist) in plugin.requiredProtocols) {
                protocolWhitelist.merge(className, classWhitelist, { x, y -> x + y })
            }
        }

        return ProtocolLogicRefFactory(protocolWhitelist)
    }

    private fun buildPluginServices(tokenizableServices: MutableList<Any>): List<Any> {
        val pluginServices = pluginRegistries.flatMap { x -> x.servicePlugins }
        val serviceList = mutableListOf<Any>()
        for (serviceClass in pluginServices) {
            val service = serviceClass.getConstructor(ServiceHubInternal::class.java).newInstance(services)
            serviceList.add(service)
            tokenizableServices.add(service)
            if (service is AcceptsFileUpload) {
                _servicesThatAcceptUploads += service
            }
        }
        return serviceList
    }


    /**
     * Run any tasks that are needed to ensure the node is in a correct state before running start().
     */
    open fun setup(): AbstractNode {
        createNodeDir()
        return this
    }

    private fun buildAdvertisedServices() {
        val serviceTypes = info.advertisedServices
        if (NetworkMapService.Type in serviceTypes) makeNetworkMapService()

        val notaryServiceType = serviceTypes.singleOrNull { it.isSubTypeOf(NotaryService.Type) }
        if (notaryServiceType != null) {
            inNodeNotaryService = makeNotaryService(notaryServiceType)
        }
    }

    /**
     * Register this node with the network map cache, and load network map from a remote service (and register for
     * updates) if one has been supplied.
     */
    private fun registerWithNetworkMap(): ListenableFuture<Unit> {
        require(networkMapService != null || NetworkMapService.Type in advertisedServices) {
            "Initial network map address must indicate a node that provides a network map service"
        }
        services.networkMapCache.addNode(info)
        // In the unit test environment, we may run without any network map service sometimes.
        if (networkMapService == null && inNodeNetworkMapService == null) {
            services.networkMapCache.runWithoutMapService()
            return noNetworkMapConfigured()
        }
        return registerWithNetworkMap(networkMapService ?: info.address)
    }

    private fun registerWithNetworkMap(networkMapServiceAddress: SingleMessageRecipient): ListenableFuture<Unit> {
        // Register for updates, even if we're the one running the network map.
        updateRegistration(networkMapServiceAddress, AddOrRemove.ADD)
        return services.networkMapCache.addMapService(net, networkMapServiceAddress, true, null)
    }

    /** This is overriden by the mock node implementation to enable operation without any network map service */
    protected open fun noNetworkMapConfigured(): ListenableFuture<Unit> {
        // TODO: There should be a consistent approach to configuration error exceptions.
        throw IllegalStateException("Configuration error: this node isn't being asked to act as the network map, nor " +
                "has any other map node been configured.")
    }

    private fun updateRegistration(networkMapAddr: SingleMessageRecipient, type: AddOrRemove): ListenableFuture<NetworkMapService.RegistrationResponse> {
        // Register this node against the network
        val instant = platformClock.instant()
        val expires = instant + NetworkMapService.DEFAULT_EXPIRATION_PERIOD
        val reg = NodeRegistration(info, instant.toEpochMilli(), type, expires)
        val sessionID = random63BitValue()
        val request = NetworkMapService.RegistrationRequest(reg.toWire(storage.myLegalIdentityKey.private), net.myAddress, sessionID)
        val message = net.createMessage(REGISTER_PROTOCOL_TOPIC, DEFAULT_SESSION_ID, request.serialize().bits)
        val future = SettableFuture.create<NetworkMapService.RegistrationResponse>()

        net.runOnNextMessage(REGISTER_PROTOCOL_TOPIC, sessionID, RunOnCallerThread) { message ->
            future.set(message.data.deserialize())
        }
        net.send(message, networkMapAddr)

        return future
    }

    protected open fun makeKeyManagementService(): KeyManagementService = PersistentKeyManagementService(setOf(storage.myLegalIdentityKey))

    open protected fun makeNetworkMapService() {
        inNodeNetworkMapService = PersistentNetworkMapService(services)
    }

    open protected fun makeNotaryService(type: ServiceType): NotaryService {
        val timestampChecker = TimestampChecker(platformClock, 30.seconds)

        return when (type) {
            SimpleNotaryService.Type -> SimpleNotaryService(services, timestampChecker, uniquenessProvider!!)
            ValidatingNotaryService.Type -> ValidatingNotaryService(services, timestampChecker, uniquenessProvider!!)
            else -> {
                throw IllegalArgumentException("Notary type ${type.id} is not handled by makeNotaryService.")
            }
        }
    }

    protected abstract fun makeUniquenessProvider(): UniquenessProvider

    protected open fun makeIdentityService(): IdentityService {
        val service = InMemoryIdentityService()

        service.registerIdentity(storage.myLegalIdentity)

        services.networkMapCache.partyNodes.forEach { service.registerIdentity(it.identity) }

        netMapCache.changed.subscribe { mapChange ->
            if (mapChange.type == MapChangeType.Added) {
                service.registerIdentity(mapChange.node.identity)
            }
        }

        return service
    }

    // TODO: sort out ordering of open & protected modifiers of functions in this class.
    protected open fun makeVaultService(): VaultService = NodeVaultService(services)

    open fun stop() {
        // TODO: We need a good way of handling "nice to have" shutdown events, especially those that deal with the
        // network, including unsubscribing from updates from remote services. Possibly some sort of parameter to stop()
        // to indicate "Please shut down gracefully" vs "Shut down now".
        // Meanwhile, we let the remote service send us updates until the acknowledgment buffer overflows and it
        // unsubscribes us forcibly, rather than blocking the shutdown process.

        // Run shutdown hooks in opposite order to starting
        for (toRun in runOnStop.reversed()) {
            toRun.run()
        }
        runOnStop.clear()
    }

    protected abstract fun makeMessagingService(): MessagingServiceInternal

    protected abstract fun startMessagingService(cordaRPCOps: CordaRPCOps?)

    protected open fun initialiseStorageService(dir: Path): Pair<TxWritableStorageService, CheckpointStorage> {
        val attachments = makeAttachmentStorage(dir)
        val checkpointStorage = PerFileCheckpointStorage(dir.resolve("checkpoints"))
        val transactionStorage = PerFileTransactionStorage(dir.resolve("transactions"))
        _servicesThatAcceptUploads += attachments
        val (identity, keyPair) = obtainKeyPair(dir)
        val stateMachineTransactionMappingStorage = InMemoryStateMachineRecordedTransactionMappingStorage()
        return Pair(
                constructStorageService(attachments, transactionStorage, stateMachineTransactionMappingStorage, keyPair, identity),
                checkpointStorage
        )
    }

    protected open fun constructStorageService(attachments: NodeAttachmentService,
                                               transactionStorage: TransactionStorage,
                                               stateMachineRecordedTransactionMappingStorage: StateMachineRecordedTransactionMappingStorage,
                                               keyPair: KeyPair,
                                               identity: Party) =
            StorageServiceImpl(attachments, transactionStorage, stateMachineRecordedTransactionMappingStorage, keyPair, identity)

    private fun obtainKeyPair(dir: Path): Pair<Party, KeyPair> {
        // Load the private identity key, creating it if necessary. The identity key is a long term well known key that
        // is distributed to other peers and we use it (or a key signed by it) when we need to do something
        // "permissioned". The identity file is what gets distributed and contains the node's legal name along with
        // the public key. Obviously in a real system this would need to be a certificate chain of some kind to ensure
        // the legal name is actually validated in some way.
        val privKeyFile = dir.resolve(PRIVATE_KEY_FILE_NAME)
        val pubIdentityFile = dir.resolve(PUBLIC_IDENTITY_FILE_NAME)

        return if (!Files.exists(privKeyFile)) {
            log.info("Identity key not found, generating fresh key!")
            val keyPair: KeyPair = generateKeyPair()
            keyPair.serialize().writeToFile(privKeyFile)
            val myIdentity = Party(configuration.myLegalName, keyPair.public)
            // We include the Party class with the file here to help catch mixups when admins provide files of the
            // wrong type by mistake.
            myIdentity.serialize().writeToFile(pubIdentityFile)
            Pair(myIdentity, keyPair)
        } else {
            // Check that the identity in the config file matches the identity file we have stored to disk.
            // This is just a sanity check. It shouldn't fail unless the admin has fiddled with the files and messed
            // things up for us.
            val myIdentity = Files.readAllBytes(pubIdentityFile).deserialize<Party>()
            if (myIdentity.name != configuration.myLegalName)
                throw ConfigurationException("The legal name in the config file doesn't match the stored identity file:" +
                        "${configuration.myLegalName} vs ${myIdentity.name}")
            // Load the private key.
            val keyPair = Files.readAllBytes(privKeyFile).deserialize<KeyPair>()
            Pair(myIdentity, keyPair)
        }
    }

    protected open fun generateKeyPair() = com.r3corda.core.crypto.generateKeyPair()

    protected fun makeAttachmentStorage(dir: Path): NodeAttachmentService {
        val attachmentsDir = dir.resolve("attachments")
        try {
            Files.createDirectory(attachmentsDir)
        } catch (e: FileAlreadyExistsException) {
        }
        return NodeAttachmentService(attachmentsDir, services.monitoringService.metrics)
    }

    protected fun createNodeDir() {
        if (!Files.exists(configuration.basedir)) {
            Files.createDirectories(configuration.basedir)
        }
    }
}
