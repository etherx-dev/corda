package com.r3corda.explorer

import com.r3corda.client.NodeMonitorClient
import com.r3corda.client.mock.EventGenerator
import com.r3corda.client.mock.Generator
import com.r3corda.client.mock.oneOf
import com.r3corda.client.model.Models
import com.r3corda.client.model.NodeMonitorModel
import com.r3corda.client.model.observer
import com.r3corda.client.model.subject
import com.r3corda.core.contracts.Amount
import com.r3corda.core.contracts.ClientToServiceCommand
import com.r3corda.core.contracts.Issued
import com.r3corda.core.contracts.PartyAndReference
import com.r3corda.explorer.model.IdentityModel
import com.r3corda.node.driver.PortAllocation
import com.r3corda.node.driver.driver
import com.r3corda.node.driver.startClient
import com.r3corda.node.services.monitor.ServiceToClientEvent
import com.r3corda.node.services.transactions.SimpleNotaryService
import javafx.stage.Stage
import rx.subjects.PublishSubject
import rx.subjects.Subject
import tornadofx.App
import java.util.*

class Main : App() {
    override val primaryView = MainWindow::class
    val aliceOutStream: Subject<ClientToServiceCommand, ClientToServiceCommand> by subject(NodeMonitorModel::clientToService)

    override fun start(stage: Stage) {

        Thread.setDefaultUncaughtExceptionHandler { thread, throwable ->
            throwable.printStackTrace()
            System.exit(1)
        }

        super.start(stage)

        // start the driver on another thread
        // TODO Change this to connecting to an actual node (specified on cli/in a config) once we're happy with the code
        Thread({

            val portAllocation = PortAllocation.Incremental(20000)
            driver(portAllocation = portAllocation) {

                val aliceNodeFuture = startNode("Alice")
                val bobNodeFuture = startNode("Bob")
                val notaryNodeFuture = startNode("Notary", advertisedServices = setOf(SimpleNotaryService.Type))

                val aliceNode = aliceNodeFuture.get()
                val bobNode = bobNodeFuture.get()
                val notaryNode = notaryNodeFuture.get()

                val aliceClient = startClient(aliceNode).get()

                Models.get<IdentityModel>(Main::class).myIdentity.set(aliceNode.identity)
                Models.get<NodeMonitorModel>(Main::class).register(aliceNode, aliceClient.certificatePath)
                val aliceMonitorClient = NodeMonitorClient(aliceClient, aliceNode, aliceOutStream, PublishSubject.create(), PublishSubject.create())
                assert(aliceMonitorClient.register().get())

                val bobInStream = PublishSubject.create<ServiceToClientEvent>()
                val bobOutStream = PublishSubject.create<ClientToServiceCommand>()

                val bobClient = startClient(bobNode).get()
                val bobMonitorClient = NodeMonitorClient(bobClient, bobNode, bobOutStream, bobInStream, PublishSubject.create())
                assert(bobMonitorClient.register().get())

                val aliceGenerator = EventGenerator(
                        parties = listOf(aliceNode.identity),
                        notary = notaryNode.identity
                )

                val asd = aliceGenerator.issueCashGenerator.generate(Random()).getOrThrow()
                aliceOutStream.onNext(
                        asd
                )
                aliceOutStream.onNext(
                        ClientToServiceCommand.PayCash(Amount(1, Issued(PartyAndReference(aliceNode.identity, asd.issueRef), asd.amount.token)), aliceNode.identity)
                )
                Thread.sleep(2000)
                aliceOutStream.onNext(
                        aliceGenerator.issueCashGenerator.generate(Random()).getOrThrow()
                )

//                for (i in 0 .. 10000) {
//                    Thread.sleep(500)
//
//                    val eventGenerator = EventGenerator(
//                            parties = listOf(aliceNode.identity, bobNode.identity),
//                            notary = notaryNode.identity
//                    )
//
//                    eventGenerator.clientToServiceCommandGenerator.combine(Generator.oneOf(listOf(aliceOutStream, bobOutStream))) {
//                        command, stream -> stream.onNext(command)
//                    }.generate(Random())
//                }

                waitForAllNodesToFinish()
            }

        }).start()
    }
}

