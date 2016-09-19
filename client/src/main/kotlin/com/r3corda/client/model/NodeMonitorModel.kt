package com.r3corda.client.model

import com.r3corda.client.NodeMonitorClient
import com.r3corda.client.CordaRPCClient
import com.r3corda.core.contracts.ClientToServiceCommand
import com.r3corda.core.node.NodeInfo
import com.r3corda.node.services.messaging.ArtemisMessagingComponent
import com.r3corda.node.services.messaging.StateMachineUpdate
import com.r3corda.node.services.monitor.ServiceToClientEvent
import com.r3corda.node.services.monitor.StateSnapshotMessage
import com.r3corda.node.utilities.AddOrRemove
import rx.Observable
import rx.subjects.PublishSubject
import java.nio.file.Path
import java.time.Instant

/**
 * This model exposes raw event streams to and from the [NodeMonitorService] through a [NodeMonitorClient]
 */
class NodeMonitorModel {
    private val clientToServiceSource = PublishSubject.create<ClientToServiceCommand>()
    val clientToService: PublishSubject<ClientToServiceCommand> = clientToServiceSource

    private val serviceToClientSource = PublishSubject.create<ServiceToClientEvent>()
    val serviceToClient: Observable<ServiceToClientEvent> = serviceToClientSource

    private val snapshotSource = PublishSubject.create<StateSnapshotMessage>()
    val snapshot: Observable<StateSnapshotMessage> = snapshotSource

    /**
     * Register for updates to/from a given vault.
     * @param messagingService The messaging to use for communication.
     * @param monitorNodeInfo the [Node] to connect to.
     * TODO provide an unsubscribe mechanism
     */
    fun register(vaultMonitorNodeInfo: NodeInfo, certificatesPath: Path) {

        val client = CordaRPCClient(ArtemisMessagingComponent.toHostAndPort(vaultMonitorNodeInfo.address), certificatesPath)
        client.start()
        val proxy = client.proxy()

        // TODO Do this properly instead of translation to/from Events/Commands
        val (stateMachines, stateMachineUpdates) = proxy.stateMachinesAndUpdates()
        stateMachines.forEach {
            serviceToClientSource.onNext(
                    ServiceToClientEvent.StateMachine(
                            Instant.now(),
                            it.fiberId,
                            it.protocolLogicClassName,
                            AddOrRemove.ADD
                    )
            )
        }
        val stateMachineEvents = stateMachineUpdates.flatMap { update ->
            when (update) {
                is StateMachineUpdate.Added -> {
                    val progressObservable: Observable<ServiceToClientEvent>? = update.stateMachineInfo.progressTrackerStepAndUpdates?.let {
                        serviceToClientSource.onNext(ServiceToClientEvent.Progress(Instant.now(), update.stateMachineInfo.fiberId, it.first))
                        it.second.map {
                            ServiceToClientEvent.Progress(Instant.now(), update.stateMachineInfo.fiberId, it)
                        }
                    }
                    val added = ServiceToClientEvent.StateMachine(
                            Instant.now(),
                            update.stateMachineInfo.fiberId,
                            update.stateMachineInfo.protocolLogicClassName,
                            AddOrRemove.ADD
                    )
                    progressObservable?.startWith(added) ?: Observable.just(added)
                }
                is StateMachineUpdate.Removed -> {
                    Observable.just(
                            ServiceToClientEvent.StateMachine(
                                    Instant.now(),
                                    update.fiberId,
                                    "Everyone has a plumbus in their home",
                                    AddOrRemove.REMOVE
                            )
                    )
                }
            }
        }

        val (vault, vaultUpdates) = proxy.vaultAndUpdates()
        serviceToClientSource.onNext(
                ServiceToClientEvent.OutputState(
                        Instant.now(),
                        setOf(),
                        vault.toSet()
                )
        )
        val outputStateEvents = vaultUpdates.map {
            ServiceToClientEvent.OutputState(
                    Instant.now(),
                    it.consumed,
                    it.produced
            )
        }

        val (transactions, newTransactions) = proxy.verifiedTransactions()
        transactions.forEach {
            serviceToClientSource.onNext(ServiceToClientEvent.Transaction(Instant.now(), it))
        }
        val transactionEvents = newTransactions.map {
            ServiceToClientEvent.Transaction(Instant.now(), it)
        }

        Observable.merge(
                arrayOf(
                        stateMachineEvents,
                        outputStateEvents,
                        transactionEvents
                )
        ).subscribe(serviceToClientSource)
    }
}
