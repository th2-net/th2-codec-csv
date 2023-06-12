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

import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import io.netty.buffer.Unpooled
import org.apache.commons.lang3.StringUtils
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets
import java.time.Instant

class TestCsvCodecTransport {
    @Test
    fun decodesDataUsingDefaultHeader() {
        val configuration = CsvCodecConfiguration()
        configuration.defaultHeader = listOf("A", "B", "C")
        val codec = CsvCodec(configuration)
        val group = MessageGroup(listOf(createCsvMessage("1,2,3")))
        val decodedGroup = codec.decode(group)

        Assertions.assertEquals(1, decodedGroup.messages.size)
        val message = decodedGroup.messages[0] as ParsedMessage

        message.body.size

        Assertions.assertEquals(3, message.body.size)
        Assertions.assertEquals(1, message.id.subsequence.size)
        Assertions.assertEquals(1, message.id.subsequence[0])
        Assertions.assertEquals("Csv_Message", message.type)
        Assertions.assertEquals("1", message.body["A"])
        Assertions.assertEquals("2", message.body["B"])
        Assertions.assertEquals("3", message.body["C"])
    }

    private fun createCsvMessage(vararg data: String): RawMessage {
        return createCsvMessage(java.util.Map.of(), *data)
    }

    private fun createCsvMessage(metadataProps: Map<String, String> = mapOf(), vararg data: String): RawMessage {
        val body = java.lang.String.join(StringUtils.LF, *data).toByteArray(StandardCharsets.UTF_8)
        return RawMessage(
            id = MessageId(
                "alias_01",
                Direction.INCOMING,
                System.nanoTime(),
                Instant.now()
            ),
            metadata = metadataProps,
            body = Unpooled.wrappedBuffer(body)
        )
    }
}