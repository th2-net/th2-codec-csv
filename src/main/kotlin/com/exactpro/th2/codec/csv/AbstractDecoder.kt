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
import mu.KotlinLogging
import java.io.ByteArrayInputStream
import java.io.IOException
import java.nio.charset.Charset
import kotlin.math.min

abstract class AbstractDecoder<ANY_MESSAGE, RAW_MESSAGE, PARSED_MESSAGE, BODY_FIELD_VALUE>(
    private val charset: Charset,
    private val csvDelimiter: Char,
    private val defaultHeader: Array<String>?,
    private val publishHeader: Boolean,
    private val validateLength: Boolean,
    private val trimWhitespace: Boolean
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
        val outputMessageType = originalMetadata.getOrDefault(
            OVERRIDE_MESSAGE_TYPE_PROP_NAME_LOWERCASE,
            CSV_MESSAGE_TYPE
        )
        var currentIndex = 0
        var header: Array<String>? = defaultHeader
        for (strings in data) {
            currentIndex++
            if (strings.isEmpty()) {
                LOGGER.error { "Empty raw at $currentIndex index (starts with 1). Data: $data" }

                errors.add(ErrorHolder("Empty raw at $currentIndex index (starts with 1)", rawMessage))
                continue
            }
            if (trimWhitespace) {
                trimEachElement(strings)
            }
            if (header == null) {
                LOGGER.debug { "Set header to: ${strings.contentToString()}" }
                header = strings
                if (publishHeader) {
                    //groupBuilder += createHeadersMessage(rawMessage, strings, currentIndex)
                    groupBuilder += createParsedMessage(rawMessage,
                        HEADER_MSG_TYPE, mapOf(HEADER_FIELD_NAME to strings.toFieldValue()), currentIndex)
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
                errors.add(ErrorHolder(msg, rawMessage))
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

    private fun copyArray(original: Array<String>, from: Int, to: Int) = original.copyOfRange(from,
        min(to, original.size)
    )

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
                reader.trimWhitespace = trimWhitespace
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
        val originalMessage: T
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