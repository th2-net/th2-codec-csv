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

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.utils.message.logId
import com.exactpro.th2.common.utils.message.sessionAlias
import com.exactpro.th2.common.value.toValue
import java.nio.charset.Charset

class ProtoDecoder(
    charset: Charset,
    csvDelimiter: Char,
    defaultHeader: Array<String>?,
    publishHeader: Boolean,
    validateLength: Boolean,
    trimWhitespace: Boolean
) : AbstractDecoder<AnyMessage, RawMessage, RawMessage, Value>(
    charset,
    csvDelimiter,
    defaultHeader,
    publishHeader,
    validateLength,
    trimWhitespace,
) {

    override val RawMessage.messageMetadata: Map<String, String> get() = metadata.propertiesMap
    override val RawMessage.messageSessionAlias: String get() = sessionAlias ?: error("No session alias in message")

    override val RawMessage.rawBody: ByteArray get() = body.toByteArray()
    override val RawMessage.messageProtocol: String get() = metadata.protocol
    override val RawMessage.logId: String get() = this.id.logId
    override val RawMessage.logData: String get() = toJson()
    override val AnyMessage.isParsed: Boolean get() = hasMessage()
    override val AnyMessage.isRaw: Boolean get() = hasRawMessage()
    override val AnyMessage.asRaw: RawMessage get() = rawMessage

    override fun createParsedMessage(sourceMessage: RawMessage, outputMessageType: String, body: Map<String, Value>, currentIndex: Int): AnyMessage {
        val builder = Message.newBuilder()
            .putAllFields(body)
            .setParentEventId(sourceMessage.parentEventId)

        // Not set message type
        setMetadata(sourceMessage.metadata, builder, outputMessageType, currentIndex)
        return AnyMessage.newBuilder().setMessage(builder).build()
    }

    override fun String.toFieldValue(): Value = toValue()
    override fun Array<String>.toFieldValue(): Value = toValue()

    private fun setMetadata(
        originalMetadata: RawMessageMetadata,
        messageBuilder: Message.Builder,
        messageType: String,
        currentIndex: Int
    ) {
        messageBuilder.setMetadata(
            MessageMetadata.newBuilder()
                .setId(
                    MessageID.newBuilder(originalMetadata.id)
                        .setTimestamp(originalMetadata.id.timestamp)
                        .addSubsequence(currentIndex)
                        .build()
                )
                .putAllProperties(originalMetadata.propertiesMap)
                .setMessageType(messageType)
        )
    }
}