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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.exactpro.th2.common.utils.message.transport.logId
import java.nio.charset.Charset

class TransportDecoder(
    charset: Charset,
    csvDelimiter: Char,
    defaultHeader: Array<String>?,
    publishHeader: Boolean,
    validateLength: Boolean
) : AbstractDecoder<Message<*>, RawMessage, ParsedMessage, Any>(charset, csvDelimiter, defaultHeader, publishHeader, validateLength) {

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