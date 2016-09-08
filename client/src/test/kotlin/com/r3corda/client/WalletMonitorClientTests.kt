package com.r3corda.client

import com.r3corda.core.contracts.*
import com.r3corda.core.serialization.OpaqueBytes
import com.r3corda.node.driver.driver
import com.r3corda.node.driver.startClient
import com.r3corda.node.services.monitor.ServiceToClientEvent
import com.r3corda.node.services.monitor.TransactionBuildResult
import com.r3corda.node.services.transactions.SimpleNotaryService
import com.r3corda.node.utilities.AddOrRemove
import com.r3corda.testing.*
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import rx.subjects.PublishSubject
import kotlin.test.fail

val log: Logger = LoggerFactory.getLogger(WalletMonitorClientTests::class.java)

class WalletMonitorClientTests {
    @Test
    fun cashIssueWorksEndToEnd() {
        driver {
            val aliceNodeFuture = startNode("Alice")
            val notaryNodeFuture = startNode("Notary", advertisedServices = setOf(SimpleNotaryService.Type))

            val aliceNode = aliceNodeFuture.get()
            val notaryNode = notaryNodeFuture.get()
            val client = startClient(aliceNode).get()

            log.info("Alice is ${aliceNode.identity}")
            log.info("Notary is ${notaryNode.identity}")

            val aliceInStream = PublishSubject.create<ServiceToClientEvent>()
            val aliceOutStream = PublishSubject.create<ClientToServiceCommand>()

            val aliceMonitorClient = WalletMonitorClient(client, aliceNode, aliceOutStream, aliceInStream, PublishSubject.create())
            require(aliceMonitorClient.register().get())

            aliceOutStream.onNext(ClientToServiceCommand.IssueCash(
                    amount = Amount(100, USD),
                    issueRef = OpaqueBytes(ByteArray(1, { 1 })),
                    recipient = aliceNode.identity,
                    notary = notaryNode.identity
            ))

            aliceInStream.expectEvents(isStrict = false) {
                    parallel(
                            expect { build: ServiceToClientEvent.TransactionBuild ->
                                val state = build.state
                                if (state is TransactionBuildResult.Failed) {
                                    fail(state.message)
                                }
                            },
                            expect { output: ServiceToClientEvent.OutputState ->
                                require(output.consumed.size == 0)
                                require(output.produced.size == 1)
                            }
                    )
            }
        }
    }

    @Test
    fun issueAndMoveWorks() {
        driver {
            val aliceNodeFuture = startNode("Alice")
            val notaryNodeFuture = startNode("Notary", advertisedServices = setOf(SimpleNotaryService.Type))

            val aliceNode = aliceNodeFuture.get()
            val notaryNode = notaryNodeFuture.get()
            val client = startClient(aliceNode).get()

            log.info("Alice is ${aliceNode.identity}")
            log.info("Notary is ${notaryNode.identity}")

            val aliceInStream = PublishSubject.create<ServiceToClientEvent>()
            val aliceOutStream = PublishSubject.create<ClientToServiceCommand>()

            val aliceMonitorClient = WalletMonitorClient(client, aliceNode, aliceOutStream, aliceInStream, PublishSubject.create())
            require(aliceMonitorClient.register().get())

            aliceOutStream.onNext(ClientToServiceCommand.IssueCash(
                    amount = Amount(100, USD),
                    issueRef = OpaqueBytes(ByteArray(1, { 1 })),
                    recipient = aliceNode.identity,
                    notary = notaryNode.identity
            ))

            aliceOutStream.onNext(ClientToServiceCommand.PayCash(
                    amount = Amount(100, Issued(PartyAndReference(aliceNode.identity, OpaqueBytes(ByteArray(1, { 1 }))), USD)),
                    recipient = aliceNode.identity
            ))

            aliceInStream.expectEvents {
                sequence(
                        // ISSUE
                        parallel(
                                sequence(
                                        expect { add: ServiceToClientEvent.StateMachine ->
                                            require(add.addOrRemove == AddOrRemove.ADD)
                                        },
                                        expect { remove: ServiceToClientEvent.StateMachine ->
                                            require(remove.addOrRemove == AddOrRemove.REMOVE)
                                        }
                                ),
                                expect { tx: ServiceToClientEvent.Transaction ->
                                    require(tx.transaction.inputs.isEmpty())
                                    require(tx.transaction.outputs.size == 1)
                                    val signaturePubKeys = tx.transaction.mustSign.toSet()
                                    // Only Alice signed
                                    require(signaturePubKeys.size == 1)
                                    require(signaturePubKeys.contains(aliceNode.identity.owningKey))
                                },
                                expect { build: ServiceToClientEvent.TransactionBuild ->
                                    val state = build.state
                                    when (state) {
                                        is TransactionBuildResult.ProtocolStarted -> {
                                        }
                                        is TransactionBuildResult.Failed -> fail(state.message)
                                    }
                                },
                                expect { output: ServiceToClientEvent.OutputState ->
                                    require(output.consumed.size == 0)
                                    require(output.produced.size == 1)
                                }
                        ),

                        // MOVE
                        parallel(
                                sequence(
                                        expect { add: ServiceToClientEvent.StateMachine ->
                                            require(add.addOrRemove == AddOrRemove.ADD)
                                        },
                                        expect { add: ServiceToClientEvent.StateMachine ->
                                            require(add.addOrRemove == AddOrRemove.REMOVE)
                                        }
                                ),
                                expect { tx: ServiceToClientEvent.Transaction ->
                                    require(tx.transaction.inputs.size == 1)
                                    require(tx.transaction.outputs.size == 1)
                                    val signaturePubKeys = tx.transaction.mustSign.toSet()
                                    // Alice and Notary signed
                                    require(signaturePubKeys.size == 2)
                                    require(signaturePubKeys.contains(aliceNode.identity.owningKey))
                                    require(signaturePubKeys.contains(notaryNode.identity.owningKey))
                                },
                                sequence(
                                        expect { build: ServiceToClientEvent.TransactionBuild ->
                                            val state = build.state
                                            when (state) {
                                                is TransactionBuildResult.ProtocolStarted -> {
                                                    log.info("${state.message}")
                                                }
                                                is TransactionBuildResult.Failed -> fail(state.message)
                                            }
                                        },
                                        replicate(7) {
                                            expect { build: ServiceToClientEvent.Progress -> }
                                        }
                                ),
                                expect { output: ServiceToClientEvent.OutputState ->
                                    require(output.consumed.size == 1)
                                    require(output.produced.size == 1)
                                }
                        )
                )
            }
        }
    }

    @Test
    fun movingCashOfDifferentIssueRefsFails() {
        driver {
            val aliceNodeFuture = startNode("Alice")
            val notaryNodeFuture = startNode("Notary", advertisedServices = setOf(SimpleNotaryService.Type))

            val aliceNode = aliceNodeFuture.get()
            val notaryNode = notaryNodeFuture.get()
            val client = startClient(aliceNode).get()

            log.info("Alice is ${aliceNode.identity}")
            log.info("Notary is ${notaryNode.identity}")

            val aliceInStream = PublishSubject.create<ServiceToClientEvent>()
            val aliceOutStream = PublishSubject.create<ClientToServiceCommand>()

            val aliceMonitorClient = WalletMonitorClient(client, aliceNode, aliceOutStream, aliceInStream, PublishSubject.create())
            require(aliceMonitorClient.register().get())

            aliceOutStream.onNext(ClientToServiceCommand.IssueCash(
                    amount = Amount(100, USD),
                    issueRef = OpaqueBytes(ByteArray(1, { 1 })),
                    recipient = aliceNode.identity,
                    notary = notaryNode.identity
            ))

            aliceOutStream.onNext(ClientToServiceCommand.IssueCash(
                    amount = Amount(100, USD),
                    issueRef = OpaqueBytes(ByteArray(1, { 2 })),
                    recipient = aliceNode.identity,
                    notary = notaryNode.identity
            ))

            aliceOutStream.onNext(ClientToServiceCommand.PayCash(
                    amount = Amount(200, Issued(PartyAndReference(aliceNode.identity, OpaqueBytes(ByteArray(1, { 1 }))), USD)),
                    recipient = aliceNode.identity
            ))

            aliceInStream.expectEvents {
                sequence(
                        // ISSUE 1
                        parallel(
                                sequence(
                                        expect { add: ServiceToClientEvent.StateMachine ->
                                            require(add.addOrRemove == AddOrRemove.ADD)
                                        },
                                        expect { remove: ServiceToClientEvent.StateMachine ->
                                            require(remove.addOrRemove == AddOrRemove.REMOVE)
                                        }
                                ),
                                expect { tx: ServiceToClientEvent.Transaction -> },
                                expect { build: ServiceToClientEvent.TransactionBuild -> },
                                expect { output: ServiceToClientEvent.OutputState -> }
                        ),

                        // ISSUE 2
                        parallel(
                                sequence(
                                        expect { add: ServiceToClientEvent.StateMachine ->
                                            require(add.addOrRemove == AddOrRemove.ADD)
                                        },
                                        expect { remove: ServiceToClientEvent.StateMachine ->
                                            require(remove.addOrRemove == AddOrRemove.REMOVE)
                                        }
                                ),
                                expect { tx: ServiceToClientEvent.Transaction -> },
                                expect { build: ServiceToClientEvent.TransactionBuild -> },
                                expect { output: ServiceToClientEvent.OutputState -> }
                        ),

                        // MOVE, should fail
                        expect { build: ServiceToClientEvent.TransactionBuild ->
                            val state = build.state
                            require(state is TransactionBuildResult.Failed)
                        }
                )
            }
        }
    }
}
