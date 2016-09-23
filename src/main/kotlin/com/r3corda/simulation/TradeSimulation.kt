package com.r3corda.simulation

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.r3corda.contracts.CommercialPaper
import com.r3corda.contracts.asset.DUMMY_CASH_ISSUER
import com.r3corda.contracts.testing.fillWithSomeTestCash
import com.r3corda.core.contracts.DOLLARS
import com.r3corda.core.contracts.OwnableState
import com.r3corda.core.contracts.`issued by`
import com.r3corda.core.days
import com.r3corda.core.node.recordTransactionsAsFakeStateMachine
import com.r3corda.core.seconds
import com.r3corda.core.transactions.SignedTransaction
import com.r3corda.protocols.TwoPartyTradeProtocol
import com.r3corda.protocols.TwoPartyTradeProtocol.TOPIC
import com.r3corda.testing.connectProtocols
import com.r3corda.testing.node.InMemoryMessagingNetwork
import java.time.Instant

/**
 * Simulates a never ending series of trades that go pair-wise through the banks (e.g. A and B trade with each other,
 * then B and C trade with each other, then C and A etc).
 */
class TradeSimulation(runAsync: Boolean, latencyInjector: InMemoryMessagingNetwork.LatencyCalculator?) : Simulation(false, runAsync, latencyInjector) {
    override fun startMainSimulation(): ListenableFuture<Unit> {
        startTradingCircle { i, j -> tradeBetween(i, j) }
        return Futures.immediateFailedFuture(UnsupportedOperationException("This future never completes"))
    }

    private fun tradeBetween(buyerBankIndex: Int, sellerBankIndex: Int): ListenableFuture<MutableList<SignedTransaction>> {
        val buyer = banks[buyerBankIndex]
        val seller = banks[sellerBankIndex]

        buyer.services.fillWithSomeTestCash(1500.DOLLARS, notary.info.identity)

        val issuance = run {
            val tx = CommercialPaper().generateIssue(seller.info.identity.ref(1, 2, 3), 1100.DOLLARS `issued by` DUMMY_CASH_ISSUER,
                    Instant.now() + 10.days, notary.info.identity)
            tx.setTime(Instant.now(), 30.seconds)
            tx.signWith(notary.storage.myLegalIdentityKey)
            tx.signWith(seller.storage.myLegalIdentityKey)
            tx.toSignedTransaction(true)
        }
        seller.services.recordTransactionsAsFakeStateMachine(issuance)

        val amount = 1000.DOLLARS
        val buyerProtocol = TwoPartyTradeProtocol.Buyer(
                seller.info.identity,
                notary.info.identity,
                amount,
                CommercialPaper.State::class.java)
        val sellerProtocol = TwoPartyTradeProtocol.Seller(
                buyer.info.identity,
                notary.info,
                issuance.tx.outRef<OwnableState>(0),
                amount,
                seller.storage.myLegalIdentityKey)
        connectProtocols(buyerProtocol, sellerProtocol)

        showConsensusFor(listOf(buyer, seller, notary))
        showProgressFor(listOf(buyer, seller))

        val buyerFuture = buyer.services.startProtocol("bank.$buyerBankIndex.$TOPIC.buyer", buyerProtocol)
        val sellerFuture = seller.services.startProtocol("bank.$sellerBankIndex.$TOPIC.seller", sellerProtocol)

        return Futures.successfulAsList(buyerFuture, sellerFuture)
    }
}
