package com.r3corda.node.services.api

import com.google.common.util.concurrent.ListenableFuture
import com.r3corda.core.messaging.Message
import com.r3corda.core.messaging.MessageHandlerRegistration
import com.r3corda.core.node.services.DEFAULT_SESSION_ID
import com.r3corda.core.protocols.ProtocolLogic
import com.r3corda.core.serialization.SingletonSerializeAsToken
import com.r3corda.core.serialization.deserialize
import com.r3corda.core.serialization.serialize
import com.r3corda.core.utilities.loggerFor
import com.r3corda.protocols.HandshakeMessage
import com.r3corda.protocols.ServiceRequestMessage
import javax.annotation.concurrent.ThreadSafe

/**
 * Abstract superclass for services that a node can host, which provides helper functions.
 */
@ThreadSafe
abstract class AbstractNodeService(val services: ServiceHubInternal) : SingletonSerializeAsToken() {

    companion object {
        val logger = loggerFor<AbstractNodeService>()
    }

    val net: MessagingServiceInternal get() = services.networkService

    /**
     * Register a handler for a message topic. In comparison to using net.addMessageHandler() this manages a lot of
     * common boilerplate code. Exceptions are caught and passed to the provided consumer.  If you just want a simple
     * acknowledgement response with no content, use [com.r3corda.core.messaging.Ack].
     *
     * @param topic the topic, without the default session ID postfix (".0).
     * @param handler a function to handle the deserialised request and return an optional response (if return type not Unit)
     * @param exceptionConsumer a function to which any thrown exception is passed.
     */
    protected inline fun <reified Q : ServiceRequestMessage, reified R : Any>
            addMessageHandler(topic: String,
                              crossinline handler: (Q) -> R,
                              crossinline exceptionConsumer: (Message, Exception) -> Unit): MessageHandlerRegistration {
        return net.addMessageHandler(topic, DEFAULT_SESSION_ID, null) { message, r ->
            try {
                val request = message.data.deserialize<Q>()
                val response = handler(request)
                // If the return type R is Unit, then do not send a response
                if (response.javaClass != Unit.javaClass) {
                    val msg = net.createMessage(topic, request.sessionID, response.serialize().bits)
                    net.send(msg, request.getReplyTo(services.networkMapCache))
                }
            } catch(e: Exception) {
                exceptionConsumer(message, e)
            }
        }
    }

    /**
     * Register a handler for a message topic. In comparison to using net.addMessageHandler() this manages a lot of
     * common boilerplate code. Exceptions are propagated to the messaging layer.  If you just want a simple
     * acknowledgement response with no content, use [com.r3corda.core.messaging.Ack].
     *
     * @param topic the topic, without the default session ID postfix (".0).
     * @param handler a function to handle the deserialised request and return an optional response (if return type not Unit).
     */
    protected inline fun <reified Q : ServiceRequestMessage, reified R : Any>
            addMessageHandler(topic: String,
                              crossinline handler: (Q) -> R): MessageHandlerRegistration {
        return addMessageHandler(topic, handler, { message: Message, exception: Exception -> throw exception })
    }

    /**
     * Register a handler to kick-off a protocol when a [HandshakeMessage] is received by the node. This performs the
     * necessary steps to enable communication between the two protocols, including calling ProtocolLogic.registerSession.
     * @param topic the topic on which the handshake is sent from the other party
     * @param loggerName the logger name to use when starting the protocol
     * @param protocolFactory a function to create the protocol with the given handshake message
     * @param onResultFuture provides access to the [ListenableFuture] when the protocol starts
     */
    protected inline fun <reified H : HandshakeMessage, R : Any> addProtocolHandler(
            topic: String,
            loggerName: String,
            crossinline protocolFactory: (H) -> ProtocolLogic<R>,
            crossinline onResultFuture: ProtocolLogic<R>.(ListenableFuture<R>, H) -> Unit) {
        net.addMessageHandler(topic, DEFAULT_SESSION_ID, null) { message, reg ->
            try {
                val handshake = message.data.deserialize<H>()
                val protocol = protocolFactory(handshake)
                protocol.registerSession(handshake)
                val resultFuture = services.startProtocol(loggerName, protocol)
                protocol.onResultFuture(resultFuture, handshake)
            } catch (e: Exception) {
                logger.error("Unable to process ${H::class.java.name} message", e)
            }
        }
    }

    protected inline fun <reified H : HandshakeMessage, R : Any> addProtocolHandler(
            topic: String,
            loggerName: String,
            crossinline protocolFactory: (H) -> ProtocolLogic<R>) {
        addProtocolHandler(topic, loggerName, protocolFactory, { future, handshake -> })
    }
}
