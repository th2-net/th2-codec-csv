/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec.csv

import com.csvreader.CsvReader
import com.exactpro.th2.codec.DecodeException
import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration
import com.exactpro.th2.common.grpc.AnyMessage as ProtoAnyMessage
import com.exactpro.th2.common.grpc.Value as ProtoValue
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup
import com.exactpro.th2.common.grpc.Message as ProtoMessage
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage
import com.exactpro.th2.common.grpc.RawMessage as ProtoParsedMessage
import com.exactpro.th2.common.grpc.MessageMetadata as ProtoMessageMetadata
import com.exactpro.th2.common.grpc.RawMessageMetadata as ProtoRawMessageMetadata
import com.exactpro.th2.common.grpc.MessageID as ProtoMessageID
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.*
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.utils.message.logId
import com.exactpro.th2.common.utils.message.sessionAlias
import com.exactpro.th2.common.utils.message.transport.logId
import com.exactpro.th2.common.value.toValue
import mu.KotlinLogging
import java.io.ByteArrayInputStream
import java.io.IOException
import java.nio.charset.Charset
import kotlin.math.min

class CsvCodec(private val config: CsvCodecConfiguration) : IPipelineCodec {

    private val charset: Charset = Charset.forName(config.encoding)
    private val defaultHeader = config.defaultHeader?.asSequence()?.map(String::trim)?.toList()?.toTypedArray()?.apply {
        require(isNotEmpty()) { "Default header must not be empty" }
        LOGGER.info { "Default header: ${config.defaultHeader}" }
    }

    private val protoDecoder = ProtoDecoder(charset, config.delimiter, defaultHeader, config.isPublishHeader, config.validateLength)
    private val transportDecoder = TransportDecoder(charset, config.delimiter, defaultHeader, config.isPublishHeader, config.validateLength)

    override fun decode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup = decode(messageGroup)
    override fun decode(messageGroup: ProtoMessageGroup): ProtoMessageGroup {
        val decodedMessages = protoDecoder.decode(messageGroup.messagesList)
        return ProtoMessageGroup.newBuilder().addAllMessages(decodedMessages).build()
    }

    override fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup = decode(messageGroup)
    override fun decode(messageGroup: MessageGroup): MessageGroup {
        val decodedMessages = transportDecoder.decode(messageGroup.messages)
        return MessageGroup(decodedMessages)
    }

    private abstract class Decoder<ANY_MESSAGE, RAW_MESSAGE, PARSED_MESSAGE, BODY_FIELD_VALUE>(
        private val charset: Charset,
        private val csvDelimiter: Char,
        private val defaultHeader: Array<String>?,
        private val publishHeader: Boolean,
        private val validateLength: Boolean
    ) {
        protected abstract val RAW_MESSAGE.messageMetadata: Map<String, String>
        protected abstract val RAW_MESSAGE.messageSessionAlias: String
        protected abstract val RAW_MESSAGE.rawBody: ByteArray
        protected abstract val RAW_MESSAGE.messageProtocol: String
        protected abstract val RAW_MESSAGE.logId: String
        protected abstract val RAW_MESSAGE.logData: String
        protected abstract val ANY_MESSAGE.isParsed: Boolean
        protected abstract val ANY_MESSAGE.isRaw: Boolean
        protected abstract val ANY_MESSAGE.asRaw: RAW_MESSAGE

        protected abstract fun createParsedMessage(sourceMessage: RAW_MESSAGE, outputMessageType: String, body: Map<String, BODY_FIELD_VALUE>, currentIndex: Int): ANY_MESSAGE
        protected abstract fun String.toFieldValue(): BODY_FIELD_VALUE
        protected abstract fun Array<String>.toFieldValue(): BODY_FIELD_VALUE

        fun decode(messageGroup: List<ANY_MESSAGE>): List<ANY_MESSAGE> {
            val groupBuilder = mutableListOf<ANY_MESSAGE>() // ProtoMessageGroup.newBuilder()
            val errors: MutableCollection<ErrorHolder<RAW_MESSAGE>> = mutableListOf()
            for (anyMessage in messageGroup) {
                if (anyMessage.isParsed) {
                    groupBuilder += anyMessage
                    continue
                }
                if (!anyMessage.isRaw) {
                    LOGGER.error { "Message should either have a raw or parsed message but has nothing: $anyMessage" }
                    continue
                }
                val rawMessage = anyMessage.asRaw
                val protocol = rawMessage.messageProtocol
                if ("" != protocol && !"csv".equals(protocol, ignoreCase = true)) {
                    LOGGER.error { "Wrong protocol: message should have empty or 'csv' protocol but has $protocol" }
                    continue
                }
                val data = decodeValues(rawMessage.rawBody)
                if (data.isEmpty()) {
                    LOGGER.error { "The raw message does not contains any data: ${rawMessage.logData}" }
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
            return groupBuilder
        }

        private fun decodeCsvData(
            errors: MutableCollection<ErrorHolder<RAW_MESSAGE>>,
            groupBuilder: MutableList<ANY_MESSAGE>,
            rawMessage: RAW_MESSAGE,
            data: Iterable<Array<String>>
        ) {
            val originalMetadata = rawMessage.messageMetadata
            val outputMessageType = originalMetadata.getOrDefault(OVERRIDE_MESSAGE_TYPE_PROP_NAME_LOWERCASE, CSV_MESSAGE_TYPE)
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
                    if (publishHeader) {
                        //groupBuilder += createHeadersMessage(rawMessage, strings, currentIndex)
                        groupBuilder += createParsedMessage(rawMessage, HEADER_MSG_TYPE, mapOf(HEADER_FIELD_NAME to strings.toFieldValue()), currentIndex)
                    }
                    continue
                }
                if (strings.size != header.size && validateLength) {
                    val msg = String.format(
                        "Wrong fields count in message. Expected count: %d; actual: %d; session alias: %s",
                        header.size, strings.size, rawMessage.messageSessionAlias
                    )
                    LOGGER.error(msg)
                    LOGGER.debug { rawMessage.toString() }
                    errors.add(ErrorHolder(msg, rawMessage, strings))
                }

                val headerLength = header.size
                val rowLength = strings.size
                var i = 0
                val body = mutableMapOf<String, BODY_FIELD_VALUE>()
                while (i < headerLength && i < rowLength) {
                    val extraLength = getHeaderArrayLength(header, i)
                    if (extraLength == 1) {
                        body[header[i]] = strings[i].toFieldValue()
                        i++
                    } else {
                        val values = copyArray(strings, i, i + extraLength)
                        body[header[i]] = values.toFieldValue()
                        i += extraLength
                    }
                }

                groupBuilder += createParsedMessage(rawMessage, outputMessageType, body, currentIndex)
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

        private fun decodeValues(body: ByteArray): List<Array<String>> {
            try {
                ByteArrayInputStream(body).use {
                    val reader = CsvReader(it, csvDelimiter, charset)
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

    }

    private class ProtoDecoder(
        charset: Charset,
        csvDelimiter: Char,
        defaultHeader: Array<String>?,
        publishHeader: Boolean,
        validateLength: Boolean
    ) : Decoder<ProtoAnyMessage, ProtoRawMessage, ProtoParsedMessage, ProtoValue>(charset, csvDelimiter, defaultHeader, publishHeader, validateLength) {

        override val ProtoRawMessage.messageMetadata: Map<String, String> get() = metadata.propertiesMap
        override val ProtoRawMessage.messageSessionAlias: String get() = sessionAlias ?: error("No session alias in message")

        override val ProtoRawMessage.rawBody: ByteArray get() = body.toByteArray()
        override val ProtoRawMessage.messageProtocol: String get() = metadata.protocol
        override val ProtoRawMessage.logId: String get() = this.id.logId
        override val com.exactpro.th2.common.grpc.RawMessage.logData: String get() = toJson()
        override val ProtoAnyMessage.isParsed: Boolean get() = hasMessage()
        override val ProtoAnyMessage.isRaw: Boolean get() = hasRawMessage()
        override val ProtoAnyMessage.asRaw: ProtoRawMessage get() = rawMessage

        override fun createParsedMessage(sourceMessage: ProtoRawMessage, outputMessageType: String, body: Map<String, ProtoValue>, currentIndex: Int): ProtoAnyMessage {
            val builder = ProtoMessage.newBuilder()
                .putAllFields(body)
                .setParentEventId(sourceMessage.parentEventId)

            // Not set message type
            setMetadata(sourceMessage.metadata, builder, outputMessageType, currentIndex)
            return ProtoAnyMessage.newBuilder().setMessage(builder).build()
        }

        override fun String.toFieldValue(): ProtoValue = toValue()
        override fun Array<String>.toFieldValue(): ProtoValue = toValue()

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
    }

    private class TransportDecoder(
        charset: Charset,
        csvDelimiter: Char,
        defaultHeader: Array<String>?,
        publishHeader: Boolean,
        validateLength: Boolean
    ) : Decoder<Message<*>, RawMessage, ParsedMessage, Any>(charset, csvDelimiter, defaultHeader, publishHeader, validateLength) {

        override val RawMessage.messageMetadata: Map<String, String> get() = this.metadata
        override val RawMessage.messageSessionAlias: String get() = id.sessionAlias

        override val RawMessage.rawBody: ByteArray get() = body.toByteArray()
        override val RawMessage.messageProtocol: String get() = protocol
        override val RawMessage.logId: String get() = id.logId
        override val RawMessage.logData: String get() = this.toString()
        override val Message<*>.isParsed: Boolean get() = this is ParsedMessage
        override val Message<*>.isRaw: Boolean get() = this is RawMessage
        override val Message<*>.asRaw: RawMessage get() = this as RawMessage

        override fun createParsedMessage(
            sourceMessage: RawMessage,
            outputMessageType: String,
            body: Map<String, Any>,
            currentIndex: Int
        ): Message<*> {
            return ParsedMessage(
                id = sourceMessage.id.toBuilder().addSubsequence(currentIndex).build(),
                eventId = sourceMessage.eventId,
                type = outputMessageType,
                metadata = sourceMessage.metadata,
                body = body
            )
        }

        override fun String.toFieldValue(): String = this
        override fun Array<String>.toFieldValue(): Any = this
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
        private const val HEADER_MSG_TYPE = "Csv_Header"
        private const val CSV_MESSAGE_TYPE = "Csv_Message"
        private const val HEADER_FIELD_NAME = "Header"
        private const val OVERRIDE_MESSAGE_TYPE_PROP_NAME_LOWERCASE = "th2.csv.override_message_type"
    }
}