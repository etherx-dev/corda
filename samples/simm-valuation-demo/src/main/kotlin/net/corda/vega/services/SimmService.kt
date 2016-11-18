package net.corda.vega.services

import net.corda.core.contracts.StateRef
import net.corda.core.crypto.Party
import net.corda.core.node.PluginServiceHub
import net.corda.core.node.CordaPluginRegistry
import net.corda.vega.api.PortfolioApi
import net.corda.vega.contracts.SwapData
import net.corda.vega.flows.IRSTradeFlow
import net.corda.vega.flows.SimmFlow
import net.corda.vega.flows.SimmRevaluation
import java.time.LocalDate

/**
 * [SimmService] is the object that makes available the flows and services for the Simm agreement / evaluation flow
 * It also enables a human usable web service for demo purposes - if available.
 * It is loaded via discovery - see [CordaPluginRegistry]
 */
object SimmService {
    class Plugin : CordaPluginRegistry() {
        override val webApis = listOf(::PortfolioApi)
        override val requiredFlows: Map<String, Set<String>> = mapOf(
                SimmFlow.Requester::class.java.name to setOf(Party::class.java.name, LocalDate::class.java.name),
                SimmRevaluation.Initiator::class.java.name to setOf(StateRef::class.java.name, LocalDate::class.java.name),
                IRSTradeFlow.Requester::class.java.name to setOf(SwapData::class.java.name, Party::class.java.name))
        override val staticServeDirs: Map<String, String> = mapOf("simmvaluationdemo" to javaClass.classLoader.getResource("simmvaluationweb").toExternalForm())
        override val servicePlugins: List<(PluginServiceHub) -> Any> = listOf(
                { services -> SimmFlow.Service(services) },
                { services -> IRSTradeFlow.Service(services) }
        )
    }
}
