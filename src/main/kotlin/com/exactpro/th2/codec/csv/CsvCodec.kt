package com.exactpro.th2.codec.csv

import com.csvreader.CsvReader
import com.exactpro.th2.codec.DecodeException
import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration
import com.exactpro.th2.codec.util.toJson

import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup
import com.exactpro.th2.common.grpc.Message as ProtoMessage
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage
import com.exactpro.th2.common.grpc.MessageMetadata as ProtoMessageMetadata
import com.exactpro.th2.common.grpc.RawMessageMetadata as ProtoRawMessageMetadata
import com.exactpro.th2.common.grpc.MessageID as ProtoMessageID

import com.exactpro.th2.common.message.logId
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.*
import com.exactpro.th2.common.value.toValue
import mu.KotlinLogging
import java.io.ByteArrayInputStream
import java.io.IOException
import java.nio.charset.Charset
import kotlin.math.min

class CsvCodec(private val config: CsvCodecConfiguration) : IPipelineCodec {

    private var charset: Charset = Charset.forName(config.encoding)

    private val defaultHeader = config.defaultHeader?.asSequence()?.map(String::trim)?.toList()?.toTypedArray()?.apply {
        require(isNotEmpty()) { "Default header must not be empty" }
        LOGGER.info { "Default header: ${config.defaultHeader}" }
    }

    override fun decode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup = decode(messageGroup)
    override fun decode(messageGroup: ProtoMessageGroup): ProtoMessageGroup {
        val groupBuilder = ProtoMessageGroup.newBuilder()
        val errors: MutableCollection<ErrorHolder<ProtoRawMessage>> = mutableListOf()
        for (anyMessage in messageGroup.messagesList) {
            if (anyMessage.hasMessage()) {
                groupBuilder.addMessages(anyMessage)
                continue
            }
            if (!anyMessage.hasRawMessage()) {
                LOGGER.error { "Message should either have a raw or parsed message but has nothing: $anyMessage" }
                continue
            }
            val rawMessage = anyMessage.rawMessage
            val protocol = rawMessage.metadata.protocol
            if ("" != protocol && !"csv".equals(protocol, ignoreCase = true)) {
                LOGGER.error { "Wrong protocol: message should have empty or 'csv' protocol but has $protocol" }
                continue
            }
            val data = decodeValues(rawMessage.body.toByteArray())
            if (data.isEmpty()) {
                LOGGER.error { "The raw message does not contains any data: ${rawMessage.toJson()}" }
                errors.add(ErrorHolder("The raw message does not contains any data", rawMessage))
                continue
            }
            decodeCsvData(errors, groupBuilder, rawMessage, data)
        }
        if (errors.isNotEmpty()) {
            throw DecodeException(
                "Cannot decode some messages:\n" + errors.joinToString("\n") {
                    "Message ${it.originalMessage.logId} cannot be decoded because ${it.text}"
                }
            )
        }
        return groupBuilder.build()
    }

    override fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup = decode(messageGroup)
    override fun decode(messageGroup: MessageGroup): MessageGroup {
        val decodedMessages = mutableListOf<Message<*>>()
        val errors: MutableCollection<ErrorHolder<RawMessage>> = mutableListOf()

        for (message in messageGroup.messages) {
            if (message is ParsedMessage) {
                decodedMessages.add(message)
                continue
            }

            if (message !is RawMessage) {
                LOGGER.error { "Message should either have a raw or parsed message but has nothing: $message" }
                continue
            }

            val protocol = message.protocol
            if ("" != protocol && !"csv".equals(protocol, ignoreCase = true)) {
                LOGGER.error { "Wrong protocol: message should have empty or 'csv' protocol but has $protocol" }
                continue
            }

            val data = decodeValues(message.body.toByteArray())

            if (data.isEmpty()) {
                LOGGER.error { "The raw message does not contains any data: ${message.toJson()}" }
                errors.add(ErrorHolder<RawMessage>("The raw message does not contains any data", message))
                continue
            }

            decodeCsvData(errors, decodedMessages, message, data)
        }

        if (!errors.isEmpty()) {
            throw DecodeException(
                "Cannot decode some messages:\n$" + errors.joinToString("\n") {
                    "Message ${it.originalMessage.toJson()} cannot be decoded because ${it.text}"
                }
            )
        }

        return MessageGroup(decodedMessages)
    }

    private fun decodeCsvData(
        errors: MutableCollection<ErrorHolder<ProtoRawMessage>>,
        groupBuilder: ProtoMessageGroup.Builder,
        rawMessage: ProtoRawMessage,
        data: Iterable<Array<String>>
    ) {
        val originalMetadata = rawMessage.metadata
        val outputMessageType =
            originalMetadata.getPropertiesOrDefault(OVERRIDE_MESSAGE_TYPE_PROP_NAME_LOWERCASE, CSV_MESSAGE_TYPE)
        var currentIndex = 0
        var header: Array<String>? = defaultHeader
        for (strings in data) {
            currentIndex++
            if (strings.isEmpty()) {
                LOGGER.error { "Empty raw at $currentIndex index (starts with 1). Data: $data" }

                errors.add(ErrorHolder("Empty raw at $currentIndex index (starts with 1)", rawMessage))
                continue
            }
            trimEachElement(strings)
            if (header == null) {
                LOGGER.debug { "Set header to: ${strings.contentToString()}" }
                header = strings
                if (config.isPublishHeader) {
                    val messageBuilder = groupBuilder.addMessagesBuilder()
                    val headerMsg = ProtoMessage.newBuilder()
                    // Not set message type
                    setMetadata(originalMetadata, headerMsg, HEADER_MSG_TYPE, currentIndex)
                    headerMsg.putFields(HEADER_FIELD_NAME, strings.toValue())
                    messageBuilder.setMessage(headerMsg)
                }
                continue
            }
            if (strings.size != header.size && config.validateLength) {
                val msg = String.format(
                    "Wrong fields count in message. Expected count: %d; actual: %d; session alias: %s",
                    header.size, strings.size, originalMetadata.id.connectionId.sessionAlias
                )
                LOGGER.error(msg)
                LOGGER.debug { rawMessage.toString() }
                errors.add(ErrorHolder(msg, rawMessage, strings))
            }
            val messageBuilder = groupBuilder.addMessagesBuilder()
            val builder = ProtoMessage.newBuilder()
            // Not set message type
            setMetadata(originalMetadata, builder, outputMessageType, currentIndex)
            val headerLength = header.size
            val rowLength = strings.size
            var i = 0
            while (i < headerLength && i < rowLength) {
                val extraLength = getHeaderArrayLength(header, i)
                if (extraLength == 1) {
                    builder.putFields(header[i], strings[i].toValue())
                    i++
                } else {
                    val values = copyArray(strings, i, i + extraLength)
                    builder.putFields(header[i], values.toValue())
                    i += extraLength
                }
            }
            messageBuilder.setMessage(builder)
        }
    }

    private fun decodeCsvData(
        errors: MutableCollection<ErrorHolder<RawMessage>>,
        groupBuilder: MutableList<Message<*>>,
        rawMessage: RawMessage,
        data: Iterable<Array<String>>
    ) {
        val originalMetadata = rawMessage.metadata
        val outputMessageType = originalMetadata[OVERRIDE_MESSAGE_TYPE_PROP_NAME_LOWERCASE] ?: CSV_MESSAGE_TYPE
        var currentIndex = 0
        var header = defaultHeader
        for (strings in data) {

            currentIndex++
            if (strings.isEmpty()) {
                LOGGER.error { "Empty raw at $currentIndex index (starts with 1). Data: $data" }
                errors += ErrorHolder("Empty raw at $currentIndex index (starts with 1)", rawMessage)
                continue
            }

            trimEachElement(strings)

            if (header == null) {
                LOGGER.debug { "Set header to: ${strings.contentToString()}" }
                header = strings
                if (config.isPublishHeader) {
                    groupBuilder += ParsedMessage(
                        id = rawMessage.id.toBuilder().addSubsequence(currentIndex).build(),
                        eventId = rawMessage.eventId, // not set in proto version?
                        type = HEADER_MSG_TYPE,
                        metadata = rawMessage.metadata,
                        body = mapOf(HEADER_FIELD_NAME to strings)
                    )
                }
                continue
            }

            if (strings.size != header.size && config.validateLength) {
                val msg = "Wrong fields count in message. Expected count: ${header.size}; actual: ${strings.size}; session alias: ${rawMessage.id.sessionAlias}"
                LOGGER.error(msg)
                LOGGER.debug { rawMessage.toString() }
                errors.add(ErrorHolder(msg, rawMessage, strings))
            }

            var i = 0
            val body = mutableMapOf<String, Any>()
            while (i < header.size && i < strings.size) {
                val extraLength = getHeaderArrayLength(header, i)
                if (extraLength == 1) {
                    body[header[i]] = strings[i]
                    i++
                } else {
                    val values = copyArray(strings, i, i + extraLength)
                    body[header[i]] = values
                    i += extraLength
                }
            }

            groupBuilder += ParsedMessage(
                id = rawMessage.id.toBuilder().addSubsequence(currentIndex).build(),
                eventId = rawMessage.eventId, // not set in proto version?
                type = outputMessageType,
                metadata = rawMessage.metadata,
                body = body
            )
        }
    }

    private fun copyArray(original: Array<String>, from: Int, to: Int) = original.copyOfRange(from, min(to, original.size))

    private fun getHeaderArrayLength(header: Array<String>, index: Int): Int {
        var length = 1
        var i = index + 1
        while (i < header.size && header[i].isEmpty()) {
            length++
            i++
        }
        return length
    }

    private fun setMetadata(
        originalMetadata: ProtoRawMessageMetadata,
        messageBuilder: ProtoMessage.Builder,
        messageType: String,
        currentIndex: Int
    ) {
        messageBuilder.setMetadata(
            ProtoMessageMetadata
                .newBuilder()
                .setId(
                    ProtoMessageID.newBuilder(originalMetadata.id)
                        .setTimestamp(originalMetadata.id.timestamp)
                        .addSubsequence(currentIndex)
                        .build()
                )
                .putAllProperties(originalMetadata.propertiesMap)
                .setMessageType(messageType)
        )
    }

    private fun decodeValues(body: ByteArray): List<Array<String>> {
        try {
            ByteArrayInputStream(body).use {
                val reader = CsvReader(it, config.delimiter, charset)
                return try {
                    val result: MutableList<Array<String>> = ArrayList()
                    while (reader.readRecord()) {
                        result.add(reader.values)
                    }
                    result
                } finally {
                    reader.close()
                }
            }
        } catch (e: IOException) {
            throw RuntimeException("cannot read data from raw bytes", e)
        }
    }

    private class ErrorHolder<T>(
        val text: String,
        val originalMessage: T,
        val currentRow: Array<String> = emptyArray()
    )

    private fun trimEachElement(elements: Array<String>) {
        for (i in elements.indices) {
            elements[i] = elements[i].trim()
        }
    }

     companion object {
        private val LOGGER = KotlinLogging.logger {}
        private const val HEADER_MSG_TYPE = "Csv_Header"
        private const val CSV_MESSAGE_TYPE = "Csv_Message"
        private const val HEADER_FIELD_NAME = "Header"
        private const val OVERRIDE_MESSAGE_TYPE_PROP_NAME_LOWERCASE = "th2.csv.override_message_type"
    }
}