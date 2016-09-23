package com.r3corda.core.node.services

import com.r3corda.core.crypto.SecureHash
import com.r3corda.core.protocols.StateMachineRunId
import rx.Observable

data class StateMachineTransactionMapping(val stateMachineRunId: StateMachineRunId, val transactionId: SecureHash)

/**
 * This is the interface to storage storing state machine -> recorded tx mappings. Any time a transaction is recorded
 * during a protocol run [addMapping] should be called.
 */
interface StateMachineRecordedTransactionMappingStorage {
    fun addMapping(stateMachineRunId: StateMachineRunId, transactionId: SecureHash)
    fun track(): Pair<List<StateMachineTransactionMapping>, Observable<StateMachineTransactionMapping>>
}
