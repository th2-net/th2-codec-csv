/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.codec.csv;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessage.Builder;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.grpc.Value;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.google.protobuf.ByteString;

class TestCsvCodec {
    @SuppressWarnings("unchecked")
    private final MessageRouter<MessageBatch> routerMock = Mockito.mock(MessageRouter.class);

    @Test
    void decodesDataAndSkipsHeader() throws IOException {
        CsvCodec codec = createCodec(new CsvCodecConfiguration());

        RawMessageBatch batch = RawMessageBatch.newBuilder()
                .addMessages(
                        createCsvMessage("A,B,C", true)
                )
                .addMessages(
                        createCsvMessage("1,2,3", false)
                ).build();
        codec.handler("", batch);

        var captor = ArgumentCaptor.forClass(MessageBatch.class);
        verify(routerMock).sendAll(captor.capture(), any());

        MessageBatch value = captor.getValue();
        assertNotNull(value, "Did not capture any publication");
        assertEquals(1, value.getMessagesCount());

        Message message = value.getMessages(0);
        assertEquals(3, message.getFieldsCount());
        assertAll(
                () -> assertEquals("1", getFieldValue(message, "A", () -> "No field A. " + message)),
                () -> assertEquals("2", getFieldValue(message, "B", () -> "No field B. " + message)),
                () -> assertEquals("3", getFieldValue(message, "C", () -> "No field C. " + message))
        );
    }

    @Test
    void decodesDataUsingDefaultHeader() throws IOException {
        CsvCodecConfiguration configuration = new CsvCodecConfiguration();
        configuration.setDefaultHeader(List.of("A", "B", "C"));
        CsvCodec codec = createCodec(configuration);

        RawMessageBatch batch = RawMessageBatch.newBuilder()
                .addMessages(
                        createCsvMessage("1,2,3", false)
                ).build();
        codec.handler("", batch);

        var captor = ArgumentCaptor.forClass(MessageBatch.class);
        verify(routerMock).sendAll(captor.capture(), any());

        MessageBatch value = captor.getValue();
        assertNotNull(value, "Did not capture any publication");
        assertEquals(1, value.getMessagesCount());

        Message message = value.getMessages(0);
        assertEquals(3, message.getFieldsCount());
        assertAll(
                () -> assertEquals("1", getFieldValue(message, "A", () -> "No field A. " + message)),
                () -> assertEquals("2", getFieldValue(message, "B", () -> "No field B. " + message)),
                () -> assertEquals("3", getFieldValue(message, "C", () -> "No field C. " + message))
        );
    }

    private CsvCodec createCodec(CsvCodecConfiguration configuration) {
        return new CsvCodec(routerMock, configuration);
    }

    private String getFieldValue(Message message, String fieldName, Supplier<String> assertMessage) {
        Value value = message.getFieldsMap().get(fieldName);
        assertNotNull(value, assertMessage);
        return value.getSimpleValue();
    }

    private RawMessage createCsvMessage(String data, boolean isHeader) {
        Builder builder = RawMessage.newBuilder()
                .setBody(ByteString.copyFrom(data.getBytes(StandardCharsets.UTF_8)));
        if (isHeader) {
            builder.setMetadata(
                    RawMessageMetadata.newBuilder()
                            .putProperties("message.type", "header")
                            .build()
            );
        }
        return builder.build();
    }
}