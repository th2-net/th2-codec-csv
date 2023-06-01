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

import com.exactpro.th2.codec.DecodeException
import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration
import com.exactpro.th2.codec.util.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.*
import mu.KotlinLogging
import java.util.*

class TransportCsvCodec(
    config: CsvCodecConfiguration
) : CsvCodec(config) {
    override fun decode(messageGroup: MessageGroup): MessageGroup {
        val decodedMessages = mutableListOf<Message<*>>()
        val errors: MutableCollection<ErrorHolder<RawMessage>> = ArrayList()

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

            val data = decodeValues(message.body.array())

            if (data.isEmpty()) {
                LOGGER.error { "The raw message does not contains any data: ${message.toJson()}" }
                errors.add(ErrorHolder<RawMessage>("The raw message does not contains any data", message))
                continue
            }

            decodeCsvData(errors, decodedMessages, message, data)
        }

        if (!errors.isEmpty()) {
            throw createException(errors)
        }

        return MessageGroup(decodedMessages)
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
                if (configuration.isPublishHeader) {
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

            if (strings.size != header.size && configuration.validateLength) {
                val msg = "Wrong fields count in message. Expected count: ${header.size}; actual: ${strings.size}; session alias: ${rawMessage.id.sessionAlias}"
                LOGGER.error { msg }
                LOGGER.debug { rawMessage.toString() }
                errors.add(ErrorHolder(msg, strings, rawMessage))
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

    private fun createException(errors: Collection<ErrorHolder<RawMessage>>): RuntimeException = DecodeException(
        "Cannot decode some messages:\n$" + errors.joinToString(separator = "\n") {
            "Message ${it.originalMessage.toJson()} cannot be decoded because ${it.text}"
        }
    )

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}