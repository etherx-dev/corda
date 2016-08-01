package com.r3corda.contracts.asset

import com.google.common.annotations.VisibleForTesting
import com.r3corda.contracts.clause.*
import com.r3corda.core.contracts.*
import com.r3corda.core.contracts.clauses.*
import com.r3corda.core.crypto.NullPublicKey
import com.r3corda.core.crypto.Party
import com.r3corda.core.crypto.SecureHash
import com.r3corda.core.crypto.toStringShort
import com.r3corda.core.random63BitValue
import com.r3corda.core.testing.MINI_CORP
import com.r3corda.core.testing.TEST_TX_TIME
import com.r3corda.core.utilities.Emoji
import com.r3corda.core.utilities.NonEmptySet
import com.r3corda.core.utilities.nonEmptySetOf
import java.security.PublicKey
import java.time.Duration
import java.time.Instant
import java.util.*

// Just a fake program identifier for now. In a real system it could be, for instance, the hash of the program bytecode.
val OBLIGATION_PROGRAM_ID = Obligation<Currency>()

/**
 * An obligation contract commits the obligor to delivering a specified amount of a fungible asset (for example the
 * [Cash] contract) at a specified future point in time. Settlement transactions may split and merge contracts across
 * multiple input and output states. The goal of this design is to handle amounts owed, and these contracts are expected
 * to be netted/merged, with settlement only for any remainder amount.
 *
 * @param P the product the obligation is for payment of.
 */
class Obligation<P> : ClauseVerifier() {

    /**
     * TODO:
     * 1) hash should be of the contents, not the URI
     * 2) allow the content to be specified at time of instance creation?
     *
     * Motivation: it's the difference between a state object referencing a programRef, which references a
     * legalContractReference and a state object which directly references both.  The latter allows the legal wording
     * to evolve without requiring code changes. But creates a risk that users create objects governed by a program
     * that is inconsistent with the legal contract.
     */
    override val legalContractReference: SecureHash = SecureHash.sha256("https://www.big-book-of-banking-law.example.gov/cash-settlement.html")
    override val clauses: List<SingleClause>
        get() = listOf(InterceptorClause(Clauses.VerifyLifecycle<P>(), Clauses.Net<P>()),
                    Clauses.Group<P>())

    interface Clauses {
        /**
         * Parent clause for clauses that operate on grouped states (those which are fungible).
         */
        class Group<P> : GroupClauseVerifier<State<P>, Issued<Terms<P>>>() {
            override val ifMatched: MatchBehaviour
                get() = MatchBehaviour.END
            override val ifNotMatched: MatchBehaviour
                get() = MatchBehaviour.ERROR
            override val clauses: List<GroupClause<State<P>, Issued<Terms<P>>>>
                get() = listOf(
                        NoZeroSizedOutputs<State<P>, Terms<P>>(),
                        SetLifecycle<P>(),
                        VerifyLifecycle<P>(),
                        Settle<P>(),
                        Issue(),
                        ConserveAmount())

            override fun extractGroups(tx: TransactionForContract): List<TransactionForContract.InOutGroup<Obligation.State<P>, Issued<Terms<P>>>>
                    = tx.groupStates<Obligation.State<P>, Issued<Terms<P>>> { it.issuanceDef }
        }

        /**
         * Generic issuance clause
         */
        class Issue<P> : AbstractIssue<State<P>, Terms<P>>({ -> sumObligations() }, { token: Issued<Terms<P>> -> sumObligationsOrZero(token) }) {
            override val requiredCommands: Set<Class<out CommandData>>
                get() = setOf(Obligation.Commands.Issue::class.java)
        }

        /**
         * Generic move/exit clause for fungible assets
         */
        class ConserveAmount<P> : AbstractConserveAmount<State<P>, Terms<P>>()

        /**
         * Clause for supporting netting of obligations.
         */
        class Net<P> : NetClause<P>()

        /**
         * Obligation-specific clause for changing the lifecycle of one or more states.
         */
        class SetLifecycle<P> : GroupClause<State<P>, Issued<Terms<P>>> {
            override val requiredCommands: Set<Class<out CommandData>>
                get() = setOf(Commands.SetLifecycle::class.java)
            override val ifMatched: MatchBehaviour
                get() = MatchBehaviour.END
            override val ifNotMatched: MatchBehaviour
                get() = MatchBehaviour.CONTINUE

            override fun verify(tx: TransactionForContract,
                                inputs: List<State<P>>,
                                outputs: List<State<P>>,
                                commands: Collection<AuthenticatedObject<CommandData>>,
                                token: Issued<Terms<P>>): Set<CommandData> {
                val command = commands.requireSingleCommand<Commands.SetLifecycle>()
                Obligation<P>().verifySetLifecycleCommand(inputs, outputs, tx, command)
                return setOf(command.value)
            }
        }

        /**
         * Obligation-specific clause for settling an outstanding obligation by witnessing
         * change of ownership of other states to fulfil
         */
        class Settle<P> : GroupClause<State<P>, Issued<Terms<P>>> {
            override val requiredCommands: Set<Class<out CommandData>>
                get() = setOf(Commands.Settle::class.java)
            override val ifMatched: MatchBehaviour
                get() = MatchBehaviour.END
            override val ifNotMatched: MatchBehaviour
                get() = MatchBehaviour.CONTINUE

            override fun verify(tx: TransactionForContract,
                                inputs: List<State<P>>,
                                outputs: List<State<P>>,
                                commands: Collection<AuthenticatedObject<CommandData>>,
                                token: Issued<Terms<P>>): Set<CommandData> {
                val command = commands.requireSingleCommand<Commands.Settle<P>>()
                val obligor = token.issuer.party
                val template = token.product
                val inputAmount: Amount<Issued<Terms<P>>> = inputs.sumObligationsOrNull<P>() ?: throw IllegalArgumentException("there is at least one obligation input for this group")
                val outputAmount: Amount<Issued<Terms<P>>> = outputs.sumObligationsOrZero(token)

                // Sum up all asset state objects that are moving and fulfil our requirements

                // The fungible asset contract verification handles ensuring there's inputs enough to cover the output states,
                // we only care about counting how much is output in this transaction. We then calculate the difference in
                // settlement amounts between the transaction inputs and outputs, and the two must match. No elimination is
                // done of amounts paid in by each beneficiary, as it's presumed the beneficiaries have enough sense to do that
                // themselves. Therefore if someone actually signed the following transaction (using cash just for an example):
                //
                // Inputs:
                //  £1m cash owned by B
                //  £1m owed from A to B
                // Outputs:
                //  £1m cash owned by B
                // Commands:
                //  Settle (signed by A)
                //  Move (signed by B)
                //
                // That would pass this check. Ensuring they do not is best addressed in the transaction generation stage.
                val assetStates = tx.outputs.filterIsInstance<FungibleAsset<*>>()
                val acceptableAssetStates = assetStates
                        // TODO: This filter is nonsense, because it just checks there is an asset contract loaded, we need to
                        // verify the asset contract is the asset contract we expect.
                        // Something like:
                        //    attachments.mustHaveOneOf(key.acceptableAssetContract)
                        .filter { it.contract.legalContractReference in template.acceptableContracts }
                        // Restrict the states to those of the correct issuance definition (this normally
                        // covers issued product and obligor, but is opaque to us)
                        .filter { it.issuanceDef in template.acceptableIssuedProducts }
                // Catch that there's nothing useful here, so we can dump out a useful error
                requireThat {
                    "there are fungible asset state outputs" by (assetStates.size > 0)
                    "there are defined acceptable fungible asset states" by (acceptableAssetStates.size > 0)
                }

                val amountReceivedByOwner = acceptableAssetStates.groupBy { it.owner }
                // Note we really do want to search all commands, because we want move commands of other contracts, not just
                // this one.
                val moveCommands = tx.commands.select<MoveCommand>()
                var totalPenniesSettled = 0L
                val requiredSigners = inputs.map { it.deposit.party.owningKey }.toSet()

                for ((beneficiary, obligations) in inputs.groupBy { it.owner }) {
                    val settled = amountReceivedByOwner[beneficiary]?.sumFungibleOrNull<P>()
                    if (settled != null) {
                        val debt = obligations.sumObligationsOrZero(token)
                        require(settled.quantity <= debt.quantity) { "Payment of $settled must not exceed debt $debt" }
                        totalPenniesSettled += settled.quantity
                    }
                }

                val totalAmountSettled = Amount(totalPenniesSettled, command.value.amount.token)
                requireThat {
                    // Insist that we can be the only contract consuming inputs, to ensure no other contract can think it's being
                    // settled as well
                    "all move commands relate to this contract" by (moveCommands.map { it.value.contractHash }
                            .all { it == null || it == Obligation<P>().legalContractReference })
                    // Settle commands exclude all other commands, so we don't need to check for contracts moving at the same
                    // time.
                    "amounts paid must match recipients to settle" by inputs.map { it.owner }.containsAll(amountReceivedByOwner.keys)
                    "amount in settle command ${command.value.amount} matches settled total ${totalAmountSettled}" by (command.value.amount == totalAmountSettled)
                    "signatures are present from all obligors" by command.signers.containsAll(requiredSigners)
                    "there are no zero sized inputs" by inputs.none { it.amount.quantity == 0L }
                    "at obligor ${obligor.name} the obligations after settlement balance" by
                            (inputAmount == outputAmount + Amount(totalPenniesSettled, token))
                }
                return setOf(command.value)
            }
        }

        /**
         * Obligation-specific clause for verifying that all states are in
         * normal lifecycle. In a group clause set, this must be run after
         * any lifecycle change clause, which is the only clause that involve
         * non-standard lifecycle states on input/output.
         */
        class VerifyLifecycle<P> : SingleClause, GroupClause<State<P>, Issued<Terms<P>>> {
            override val requiredCommands: Set<Class<out CommandData>>
                get() = emptySet()
            override val ifMatched: MatchBehaviour
                get() = MatchBehaviour.CONTINUE
            override val ifNotMatched: MatchBehaviour
                get() = MatchBehaviour.ERROR

            override fun verify(tx: TransactionForContract, commands: Collection<AuthenticatedObject<CommandData>>): Set<CommandData>
                = verify(
                    tx.inputs.filterIsInstance<State<P>>(),
                    tx.outputs.filterIsInstance<State<P>>()
            )

            override fun verify(tx: TransactionForContract,
                                inputs: List<State<P>>,
                                outputs: List<State<P>>,
                                commands: Collection<AuthenticatedObject<CommandData>>,
                                token: Issued<Terms<P>>): Set<CommandData>
                = verify(inputs, outputs)

            fun verify(inputs: List<State<P>>,
                       outputs: List<State<P>>): Set<CommandData> {
                requireThat {
                    "all inputs are in the normal state " by inputs.all { it.lifecycle == Lifecycle.NORMAL }
                    "all outputs are in the normal state " by outputs.all { it.lifecycle == Lifecycle.NORMAL }
                }
                return emptySet()
            }
        }
    }

    /**
     * Represents where in its lifecycle a contract state is, which in turn controls the commands that can be applied
     * to the state. Most states will not leave the [NORMAL] lifecycle. Note that settled (as an end lifecycle) is
     * represented by absence of the state on transaction output.
     */
    enum class Lifecycle {
        /** Default lifecycle state for a contract, in which it can be settled normally */
        NORMAL,
        /**
         * Indicates the contract has not been settled by its due date. Once in the defaulted state,
         * it can only be reverted to [NORMAL] state by the beneficiary.
         */
        DEFAULTED
    }

    /**
     * Subset of state, containing the elements specified when issuing a new settlement contract.
     *
     * @param P the product the obligation is for payment of.
     */
    data class Terms<P>(
            /** The hash of the asset contract we're willing to accept in payment for this debt. */
            val acceptableContracts: NonEmptySet<SecureHash>,
            /** The parties whose assets we are willing to accept in payment for this debt. */
            val acceptableIssuedProducts: NonEmptySet<Issued<P>>,

            /** When the contract must be settled by. */
            val dueBefore: Instant,
            val timeTolerance: Duration = Duration.ofSeconds(30)
    ) {
        val product: P
            get() = acceptableIssuedProducts.map { it.product }.toSet().single()
    }

    /**
     * A state representing the obligation of one party (obligor) to deliver a specified number of
     * units of an underlying asset (described as token.acceptableIssuedProducts) to the beneficiary
     * no later than the specified time.
     *
     * @param P the product the obligation is for payment of.
     */
    data class State<P>(
            var lifecycle: Lifecycle = Lifecycle.NORMAL,
            /** Where the debt originates from (obligor) */
            val obligor: Party,
            val template: Terms<P>,
            val quantity: Long,
            /** The public key of the entity the contract pays to */
            val beneficiary: PublicKey
    ) : FungibleAsset<Obligation.Terms<P>>, NettableState<State<P>, MultilateralNetState<P>> {
        override val amount: Amount<Issued<Terms<P>>>
            get() = Amount(quantity, issuanceDef)
        override val contract = OBLIGATION_PROGRAM_ID
        override val deposit: PartyAndReference
            get() = amount.token.issuer
        override val exitKeys: Collection<PublicKey>
            get() = setOf(owner)
        val dueBefore: Instant
            get() = template.dueBefore
        override val issuanceDef: Issued<Terms<P>>
            get() = Issued(obligor.ref(0), template)
        override val participants: List<PublicKey>
            get() = listOf(obligor.owningKey, beneficiary)
        override val owner: PublicKey
            get() = beneficiary

        override fun move(newAmount: Amount<Issued<Terms<P>>>, newOwner: PublicKey): State<P>
                = copy(quantity = newAmount.quantity, beneficiary = newOwner)

        override fun toString() = when (lifecycle) {
            Lifecycle.NORMAL -> "${Emoji.bagOfCash}Debt($amount due $dueBefore to ${beneficiary.toStringShort()})"
            Lifecycle.DEFAULTED -> "${Emoji.bagOfCash}Debt($amount unpaid by $dueBefore to ${beneficiary.toStringShort()})"
        }

        override val bilateralNetState: BilateralNetState<P>
            get() {
                check(lifecycle == Lifecycle.NORMAL)
                return BilateralNetState(setOf(obligor.owningKey, beneficiary), template)
            }
        override val multilateralNetState: MultilateralNetState<P>
            get() {
                check(lifecycle == Lifecycle.NORMAL)
                return MultilateralNetState(template)
            }

        override fun net(other: State<P>): State<P> {
            val netA = bilateralNetState
            val netB = other.bilateralNetState
            require(netA == netB) { "net substates of the two state objects must be identical" }

            if (obligor.owningKey == other.obligor.owningKey) {
                // Both sides are from the same obligor to beneficiary
                return copy(quantity = quantity + other.quantity)
            } else {
                // Issuer and beneficiary are backwards
                return copy(quantity = quantity - other.quantity)
            }
        }

        override fun withNewOwner(newOwner: PublicKey) = Pair(Commands.Move(), copy(beneficiary = newOwner))
    }

    // Just for grouping
    interface Commands : FungibleAsset.Commands {
        /**
         * Net two or more obligation states together in a close-out netting style. Limited to bilateral netting
         * as only the beneficiary (not the obligor) needs to sign.
         */
        data class Net(val type: NetType) : Obligation.Commands

        /**
         * A command stating that a debt has been moved, optionally to fulfil another contract.
         *
         * @param contractHash the contract this move is for the attention of. Only that contract's verify function
         * should take the moved states into account when considering whether it is valid. Typically this will be
         * null.
         */
        data class Move(override val contractHash: SecureHash? = null) : Commands, FungibleAsset.Commands.Move

        /**
         * Allows new obligation states to be issued into existence: the nonce ("number used once") ensures the
         * transaction has a unique ID even when there are no inputs.
         */
        data class Issue(override val nonce: Long = random63BitValue()) : FungibleAsset.Commands.Issue, Commands

        /**
         * A command stating that the obligor is settling some or all of the amount owed by transferring a suitable
         * state object to the beneficiary. If this reduces the balance to zero, the state object is destroyed.
         * @see [MoveCommand].
         */
        data class Settle<P>(val amount: Amount<Issued<Terms<P>>>) : Commands

        /**
         * A command stating that the beneficiary is moving the contract into the defaulted state as it has not been settled
         * by the due date, or resetting a defaulted contract back to the issued state.
         */
        data class SetLifecycle(val lifecycle: Lifecycle) : Commands {
            val inverse: Lifecycle
                get() = when (lifecycle) {
                    Lifecycle.NORMAL -> Lifecycle.DEFAULTED
                    Lifecycle.DEFAULTED -> Lifecycle.NORMAL
                }
        }

        /**
         * A command stating that the debt is being released by the beneficiary. Normally would indicate
         * either settlement outside of the ledger, or that the obligor is unable to pay.
         */
        data class Exit<P>(override val amount: Amount<Issued<Terms<P>>>) : Commands, FungibleAsset.Commands.Exit<Terms<P>>
    }

    override fun extractCommands(tx: TransactionForContract): List<AuthenticatedObject<FungibleAsset.Commands>>
            = tx.commands.select<Obligation.Commands>()

    /**
     * A default command mutates inputs and produces identical outputs, except that the lifecycle changes.
     */
    @VisibleForTesting
    protected fun verifySetLifecycleCommand(inputs: List<FungibleAsset<Terms<P>>>,
                                            outputs: List<FungibleAsset<Terms<P>>>,
                                            tx: TransactionForContract,
                                            setLifecycleCommand: AuthenticatedObject<Commands.SetLifecycle>) {
        // Default must not change anything except lifecycle, so number of inputs and outputs must match
        // exactly.
        require(inputs.size == outputs.size) { "Number of inputs and outputs must match" }

        // If we have an default command, perform special processing: issued contracts can only be defaulted
        // after the due date, and default/reset can only be done by the beneficiary
        val expectedInputLifecycle: Lifecycle = setLifecycleCommand.value.inverse
        val expectedOutputLifecycle: Lifecycle = setLifecycleCommand.value.lifecycle

        // Check that we're past the deadline for ALL involved inputs, and that the output states correspond 1:1
        for ((stateIdx, input) in inputs.withIndex()) {
            if (input is State<P>) {
                val actualOutput = outputs[stateIdx]
                val deadline = input.dueBefore
                val timestamp: TimestampCommand? = tx.timestamp
                val expectedOutput: State<P> = input.copy(lifecycle = expectedOutputLifecycle)

                requireThat {
                    "there is a timestamp from the authority" by (timestamp != null)
                    "the due date has passed" by (timestamp!!.after?.isAfter(deadline) ?: false)
                    "input state lifecycle is correct" by (input.lifecycle == expectedInputLifecycle)
                    "output state corresponds exactly to input state, with lifecycle changed" by (expectedOutput == actualOutput)
                }
            }
        }
        val owningPubKeys = inputs.filter { it is State<P> }.map { (it as State<P>).beneficiary }.toSet()
        val keysThatSigned = setLifecycleCommand.signers.toSet()
        requireThat {
            "the owning keys are the same as the signing keys" by keysThatSigned.containsAll(owningPubKeys)
        }
    }

    /**
     * Generate a transaction performing close-out netting of two or more states.
     *
     * @param signer the party who will sign the transaction. Must be one of the obligor or beneficiary.
     * @param states two or more states, which must be compatible for bilateral netting (same issuance definitions,
     * and same parties involved).
     */
    fun generateCloseOutNetting(tx: TransactionBuilder,
                                signer: PublicKey,
                                vararg states: State<P>) {
        val netState = states.firstOrNull()?.bilateralNetState

        requireThat {
            "at least two states are provided" by (states.size >= 2)
            "all states are in the normal lifecycle state " by (states.all { it.lifecycle == Lifecycle.NORMAL })
            "all states must be bilateral nettable" by (states.all { it.bilateralNetState == netState })
            "signer is in the state parties" by (signer in netState!!.partyKeys)
        }

        val out = states.reduce { stateA, stateB -> stateA.net(stateB) }
        if (out.quantity > 0L)
            tx.addOutputState(out)
        tx.addCommand(Commands.Net(NetType.PAYMENT), signer)
    }

    /**
     * Puts together an issuance transaction for the specified amount that starts out being owned by the given pubkey.
     */
    fun generateIssue(tx: TransactionBuilder,
                      obligor: Party,
                      issuanceDef: Terms<P>,
                      pennies: Long,
                      beneficiary: PublicKey,
                      notary: Party) {
        check(tx.inputStates().isEmpty())
        check(tx.outputStates().map { it.data }.sumObligationsOrNull<P>() == null)
        tx.addOutputState(State(Lifecycle.NORMAL, obligor, issuanceDef, pennies, beneficiary), notary)
        tx.addCommand(Commands.Issue(), obligor.owningKey)
    }

    fun generatePaymentNetting(tx: TransactionBuilder,
                               issued: Issued<Obligation.Terms<P>>,
                               notary: Party,
                               vararg states: State<P>) {
        requireThat {
            "all states are in the normal lifecycle state " by (states.all { it.lifecycle == Lifecycle.NORMAL })
        }
        val groups = states.groupBy { it.multilateralNetState }
        val partyLookup = HashMap<PublicKey, Party>()
        val signers = states.map { it.beneficiary }.union(states.map { it.obligor.owningKey }).toSet()

        // Create a lookup table of the party that each public key represents.
        states.map { it.obligor }.forEach { partyLookup.put(it.owningKey, it) }

        for ((netState, groupStates) in groups) {
            // Extract the net balances
            val netBalances = netAmountsDue(extractAmountsDue(issued.product, states.asIterable()))

            netBalances
                    // Convert the balances into obligation state objects
                    .map { entry ->
                        State(Lifecycle.NORMAL, partyLookup[entry.key.first]!!,
                                netState.template, entry.value.quantity, entry.key.second)
                    }
                    // Add the new states to the TX
                    .forEach { tx.addOutputState(it, notary) }
            tx.addCommand(Commands.Net(NetType.PAYMENT), signers.toList())
        }

    }

    /**
     * Generate a transaction changing the lifecycle of one or more state objects.
     *
     * @param statesAndRefs a list of state objects, which MUST all have the same issuance definition. This avoids
     * potential complications arising from different deadlines applying to different states.
     */
    fun generateSetLifecycle(tx: TransactionBuilder,
                             statesAndRefs: List<StateAndRef<State<P>>>,
                             lifecycle: Lifecycle,
                             notary: Party) {
        val states = statesAndRefs.map { it.state.data }
        val issuanceDef = getTemplateOrThrow(states)
        val existingLifecycle = when (lifecycle) {
            Lifecycle.DEFAULTED -> Lifecycle.NORMAL
            Lifecycle.NORMAL -> Lifecycle.DEFAULTED
        }
        require(states.all { it.lifecycle == existingLifecycle }) { "initial lifecycle must be ${existingLifecycle} for all input states" }

        // Produce a new set of states
        val groups = statesAndRefs.groupBy { it.state.data.issuanceDef }
        for ((aggregateState, stateAndRefs) in groups) {
            val partiesUsed = ArrayList<PublicKey>()
            stateAndRefs.forEach { stateAndRef ->
                val outState = stateAndRef.state.data.copy(lifecycle = lifecycle)
                tx.addInputState(stateAndRef)
                tx.addOutputState(outState, notary)
                partiesUsed.add(stateAndRef.state.data.beneficiary)
            }
            tx.addCommand(Commands.SetLifecycle(lifecycle), partiesUsed.distinct())
        }
        tx.setTime(issuanceDef.dueBefore, notary, issuanceDef.timeTolerance)
    }

    /**
     * @param statesAndRefs a list of state objects, which MUST all have the same aggregate state. This is done as
     * only a single settlement command can be present in a transaction, to avoid potential problems with allocating
     * assets to different obligation issuances.
     * @param assetStatesAndRefs a list of fungible asset state objects, which MUST all be of the same issued product.
     * It is strongly encouraged that these all have the same beneficiary.
     * @param moveCommand the command used to move the asset state objects to their new owner.
     */
    fun generateSettle(tx: TransactionBuilder,
                       statesAndRefs: Iterable<StateAndRef<State<P>>>,
                       assetStatesAndRefs: Iterable<StateAndRef<FungibleAsset<P>>>,
                       moveCommand: MoveCommand,
                       notary: Party) {
        val states = statesAndRefs.map { it.state }
        val obligationIssuer = states.first().data.obligor
        val obligationOwner = states.first().data.beneficiary

        requireThat {
            "all fungible asset states use the same notary" by (assetStatesAndRefs.all { it.state.notary == notary })
            "all obligation states are in the normal state" by (statesAndRefs.all { it.state.data.lifecycle == Lifecycle.NORMAL })
            "all obligation states use the same notary" by (statesAndRefs.all { it.state.notary == notary })
            "all obligation states have the same obligor" by (statesAndRefs.all { it.state.data.obligor == obligationIssuer })
            "all obligation states have the same beneficiary" by (statesAndRefs.all { it.state.data.beneficiary == obligationOwner })
        }

        // TODO: A much better (but more complex) solution would be to have two iterators, one for obligations,
        // one for the assets, and step through each in a semi-synced manner. For now however we just bundle all the states
        // on each side together

        val issuanceDef = getIssuanceDefinitionOrThrow(statesAndRefs.map { it.state.data })
        val template: Terms<P> = issuanceDef.product
        val obligationTotal: Amount<P> = Amount(states.map { it.data }.sumObligations<P>().quantity, template.product)
        var obligationRemaining: Amount<P> = obligationTotal
        val assetSigners = HashSet<PublicKey>()

        statesAndRefs.forEach { tx.addInputState(it) }

        // Move the assets to the new beneficiary
        assetStatesAndRefs.forEach { ref ->
            if (obligationRemaining.quantity > 0L) {
                tx.addInputState(ref)

                val assetState = ref.state.data
                val amount: Amount<P> = Amount(assetState.amount.quantity, assetState.amount.token.product)
                if (obligationRemaining >= amount) {
                    tx.addOutputState(assetState.move(assetState.amount, obligationOwner), notary)
                    obligationRemaining -= amount
                } else {
                    val change = Amount(obligationRemaining.quantity, assetState.amount.token)
                    // Split the state in two, sending the change back to the previous beneficiary
                    tx.addOutputState(assetState.move(change, obligationOwner), notary)
                    tx.addOutputState(assetState.move(assetState.amount - change, assetState.owner), notary)
                    obligationRemaining -= Amount(0L, obligationRemaining.token)
                }
                assetSigners.add(assetState.owner)
            }
        }

        // If we haven't cleared the full obligation, add the remainder as an output
        if (obligationRemaining.quantity > 0L) {
            tx.addOutputState(State(Lifecycle.NORMAL, obligationIssuer, template, obligationRemaining.quantity, obligationOwner), notary)
        } else {
            // Destroy all of the states
        }

        // Add the asset move command and obligation settle
        tx.addCommand(moveCommand, assetSigners.toList())
        tx.addCommand(Commands.Settle(Amount((obligationTotal - obligationRemaining).quantity, issuanceDef)), obligationIssuer.owningKey)
    }

    /** Get the common issuance definition for one or more states, or throw an IllegalArgumentException. */
    private fun getIssuanceDefinitionOrThrow(states: Iterable<State<P>>): Issued<Terms<P>> =
            states.map { it.issuanceDef }.distinct().single()

    /** Get the common issuance definition for one or more states, or throw an IllegalArgumentException. */
    private fun getTemplateOrThrow(states: Iterable<State<P>>): Terms<P> =
            states.map { it.template }.distinct().single()
}


/**
 * Convert a list of settlement states into total from each obligor to a beneficiary.
 *
 * @return a map of obligor/beneficiary pairs to the balance due.
 */
fun <P> extractAmountsDue(product: Obligation.Terms<P>, states: Iterable<Obligation.State<P>>): Map<Pair<PublicKey, PublicKey>, Amount<Obligation.Terms<P>>> {
    val balances = HashMap<Pair<PublicKey, PublicKey>, Amount<Obligation.Terms<P>>>()

    states.forEach { state ->
        val key = Pair(state.obligor.owningKey, state.beneficiary)
        val balance = balances[key] ?: Amount(0L, product)
        balances[key] = balance + Amount(state.amount.quantity, state.amount.token.product)
    }

    return balances
}

/**
 * Net off the amounts due between parties.
 */
fun <P> netAmountsDue(balances: Map<Pair<PublicKey, PublicKey>, Amount<P>>): Map<Pair<PublicKey, PublicKey>, Amount<P>> {
    val nettedBalances = HashMap<Pair<PublicKey, PublicKey>, Amount<P>>()

    balances.forEach { balance ->
        val (obligor, beneficiary) = balance.key
        val oppositeKey = Pair(beneficiary, obligor)
        val opposite = (balances[oppositeKey] ?: Amount(0L, balance.value.token))
        // Drop zero balances
        if (balance.value > opposite) {
            nettedBalances[balance.key] = (balance.value - opposite)
        } else if (opposite > balance.value) {
            nettedBalances[oppositeKey] = (opposite - balance.value)
        }
    }

    return nettedBalances
}

/**
 * Calculate the total balance movement for each party in the transaction, based off a summary of balances between
 * each obligor and beneficiary.
 *
 * @param balances payments due, indexed by obligor and beneficiary. Zero balances are stripped from the map before being
 * returned.
 */
fun <P> sumAmountsDue(balances: Map<Pair<PublicKey, PublicKey>, Amount<P>>): Map<PublicKey, Long> {
    val sum = HashMap<PublicKey, Long>()

    // Fill the map with zeroes initially
    balances.keys.forEach {
        sum[it.first] = 0L
        sum[it.second] = 0L
    }

    for ((key, amount) in balances) {
        val (obligor, beneficiary) = key
        // Subtract it from the obligor
        sum[obligor] = sum[obligor]!! - amount.quantity
        // Add it to the beneficiary
        sum[beneficiary] = sum[beneficiary]!! + amount.quantity
    }

    // Strip zero balances
    val iterator = sum.iterator()
    while (iterator.hasNext()) {
        val amount = iterator.next().value
        if (amount == 0L) {
            iterator.remove()
        }
    }

    return sum
}

/** Sums the obligation states in the list, throwing an exception if there are none. All state objects in the list are presumed to be nettable. */
fun <P> Iterable<ContractState>.sumObligations(): Amount<Issued<Obligation.Terms<P>>>
        = filterIsInstance<Obligation.State<P>>().map { it.amount }.sumOrThrow()

/** Sums the obligation states in the list, returning null if there are none. */
fun <P> Iterable<ContractState>.sumObligationsOrNull(): Amount<Issued<Obligation.Terms<P>>>?
        = filterIsInstance<Obligation.State<P>>().filter { it.lifecycle == Obligation.Lifecycle.NORMAL }.map { it.amount }.sumOrNull()

/** Sums the obligation states in the list, returning zero of the given product if there are none. */
fun <P> Iterable<ContractState>.sumObligationsOrZero(issuanceDef: Issued<Obligation.Terms<P>>): Amount<Issued<Obligation.Terms<P>>>
        = filterIsInstance<Obligation.State<P>>().filter { it.lifecycle == Obligation.Lifecycle.NORMAL }.map { it.amount }.sumOrZero(issuanceDef)

infix fun <T> Obligation.State<T>.at(dueBefore: Instant) = copy(template = template.copy(dueBefore = dueBefore))
infix fun <T> Obligation.State<T>.between(parties: Pair<Party, PublicKey>) = copy(obligor = parties.first, beneficiary = parties.second)
infix fun <T> Obligation.State<T>.`owned by`(owner: PublicKey) = copy(beneficiary = owner)
infix fun <T> Obligation.State<T>.`issued by`(party: Party) = copy(obligor = party)
// For Java users:
fun <T> Obligation.State<T>.ownedBy(owner: PublicKey) = copy(beneficiary = owner)
fun <T> Obligation.State<T>.issuedBy(party: Party) = copy(obligor = party)

val Issued<Currency>.OBLIGATION_DEF: Obligation.Terms<Currency>
    get() = Obligation.Terms(nonEmptySetOf(Cash().legalContractReference), nonEmptySetOf(this), TEST_TX_TIME)
val Amount<Issued<Currency>>.OBLIGATION: Obligation.State<Currency>
    get() = Obligation.State(Obligation.Lifecycle.NORMAL, MINI_CORP, token.OBLIGATION_DEF, quantity, NullPublicKey)