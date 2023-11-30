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

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import mu.KotlinLogging
import java.nio.charset.Charset

class CsvCodec(private val config: CsvCodecConfiguration) : IPipelineCodec {

    private val charset: Charset = Charset.forName(config.encoding)
    private val defaultHeader = config.defaultHeader?.asSequence()?.map(String::trim)?.toList()?.toTypedArray()?.apply {
        require(isNotEmpty()) { "Default header must not be empty" }
        LOGGER.info { "Default header: ${config.defaultHeader}" }
    }

    private val protoDecoder = ProtoDecoder(
        charset,
        config.delimiter,
        defaultHeader,
        config.isPublishHeader,
        config.validateLength,
        config.isTrimWhitespace,
    )
    private val transportDecoder = TransportDecoder(
        charset,
        config.delimiter,
        defaultHeader,
        config.isPublishHeader,
        config.validateLength,
        config.isTrimWhitespace,
    )

    override fun decode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup {
        val decodedMessages = protoDecoder.decode(messageGroup.messagesList)
        return ProtoMessageGroup.newBuilder().addAllMessages(decodedMessages).build()
    }

    override fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup {
        val decodedMessages = transportDecoder.decode(messageGroup.messages)
        return MessageGroup(decodedMessages)
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}