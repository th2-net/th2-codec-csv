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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageID;
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
    @SuppressWarnings("unchecked")
    private final MessageRouter<EventBatch> eventRouterMock = Mockito.mock(MessageRouter.class);

    @Nested
    class TestPositive {
        @Test
        void decodesDataAndSkipsHeader() throws IOException {
            CsvCodec codec = createCodec();

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
            assertFieldCount(3, message);
            assertAll("Current message: " + message,
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
            assertFieldCount(3, message);
            assertAll("Current message: " + message,
                    () -> assertEquals("1", getFieldValue(message, "A", () -> "No field A. " + message)),
                    () -> assertEquals("2", getFieldValue(message, "B", () -> "No field B. " + message)),
                    () -> assertEquals("3", getFieldValue(message, "C", () -> "No field C. " + message))
            );
        }

        @Test
        void decodesDataWithEscapedCharacters() throws IOException {
            CsvCodec codec = createCodec();

            RawMessageBatch batch = RawMessageBatch.newBuilder()
                    .addMessages(
                            createCsvMessage("A,B", true)
                    )
                    .addMessages(
                            createCsvMessage("\"1,2\",\"\"\"value\"\"\"", false)
                    ).build();
            codec.handler("", batch);

            var captor = ArgumentCaptor.forClass(MessageBatch.class);
            verify(routerMock).sendAll(captor.capture(), any());

            MessageBatch value = captor.getValue();
            assertNotNull(value, "Did not capture any publication");
            assertEquals(1, value.getMessagesCount());

            Message message = value.getMessages(0);
            assertFieldCount(2, message);
            assertAll("Current message: " + message,
                    () -> assertEquals("1,2", getFieldValue(message, "A", () -> "No field A. " + message)),
                    () -> assertEquals("\"value\"", getFieldValue(message, "B", () -> "No field B. " + message))
            );
        }

        @Test
        void decodesDataCustomDelimiter() throws IOException {
            CsvCodecConfiguration configuration = new CsvCodecConfiguration();
            configuration.setDelimiter(';');
            CsvCodec codec = createCodec(configuration);

            RawMessageBatch batch = RawMessageBatch.newBuilder()
                    .addMessages(
                            createCsvMessage("A;B", true)
                    )
                    .addMessages(
                            createCsvMessage("1,2;3", false)
                    ).build();
            codec.handler("", batch);

            var captor = ArgumentCaptor.forClass(MessageBatch.class);
            verify(routerMock).sendAll(captor.capture(), any());

            MessageBatch value = captor.getValue();
            assertNotNull(value, "Did not capture any publication");
            assertEquals(1, value.getMessagesCount());

            Message message = value.getMessages(0);
            assertFieldCount(2, message);
            assertAll("Current message: " + message,
                    () -> assertEquals("1,2", getFieldValue(message, "A", () -> "No field A. " + message)),
                    () -> assertEquals("3", getFieldValue(message, "B", () -> "No field B. " + message))
            );
        }

        @Test
        void trimsWhitespacesDuringDecoding() throws IOException {
            CsvCodec codec = createCodec();

            RawMessageBatch batch = RawMessageBatch.newBuilder()
                    .addMessages(
                            createCsvMessage("A, B, C", true)
                    )
                    .addMessages(
                            createCsvMessage("1, , 3 3", false)
                    ).build();
            codec.handler("", batch);

            var captor = ArgumentCaptor.forClass(MessageBatch.class);
            verify(routerMock).sendAll(captor.capture(), any());

            MessageBatch value = captor.getValue();
            assertNotNull(value, "Did not capture any publication");
            assertEquals(1, value.getMessagesCount());

            Message message = value.getMessages(0);
            assertFieldCount(3, message);
            assertAll("Current message: " + message,
                    () -> assertEquals("1", getFieldValue(message, "A", () -> "No field A. " + message)),
                    () -> assertEquals("", getFieldValue(message, "B", () -> "No field B. " + message)),
                    () -> assertEquals("3 3", getFieldValue(message, "C", () -> "No field C. " + message))
            );
        }
    }

    @Nested
    class TestNegative {
        @Test
        void reportsErrorIfNotHeaderFound() throws IOException {
            CsvCodec codec = createCodec();
            codec.handler("", RawMessageBatch.newBuilder()
                    .addMessages(createCsvMessage("1,2,3",false))
                    .build());

            verify(routerMock, never()).sendAll(any(), any());
            verify(eventRouterMock).send(any());
        }

        @Test
        void reportsErrorIfDefaultHeaderAndDataHaveDifferentSize() throws IOException {
            CsvCodecConfiguration configuration = new CsvCodecConfiguration();
            configuration.setDefaultHeader(List.of("A", "B"));
            CsvCodec codec = createCodec(configuration);
            codec.handler("", RawMessageBatch.newBuilder()
                    .addMessages(createCsvMessage("1,2,3",false))
                    .build());

            var captor = ArgumentCaptor.forClass(MessageBatch.class);
            verify(routerMock).sendAll(captor.capture(), any());
            verify(eventRouterMock).send(any());

            var messageBatch = captor.getValue();
            assertNotNull(messageBatch);
            assertEquals(1, messageBatch.getMessagesCount(), () -> "Batch: " + messageBatch);
            Message message = messageBatch.getMessages(0);
            assertFieldCount(2, message);
            assertAll(
                    () -> assertEquals("1", getFieldValue(message, "A", () -> "No field A: " + message)),
                    () -> assertEquals("2", getFieldValue(message, "B", () -> "No field B: " + message))
            );
        }

        @Test
        void reportsAllErrors() throws IOException {
            CsvCodec codec = createCodec();
            List<RawMessage> messages = List.of(createCsvMessage("1,2,3", false),
                    createCsvMessage("4,5,6", false),
                    createCsvMessage("7,8,9", false));
            codec.handler("", RawMessageBatch.newBuilder()
                    .addAllMessages(messages)
                    .build());

            verify(routerMock, never()).sendAll(any(), any());
            var captor = ArgumentCaptor.forClass(EventBatch.class);
            verify(eventRouterMock).send(captor.capture());

            EventBatch eventBatch = captor.getValue();
            assertNotNull(eventBatch);
            assertEquals(3, eventBatch.getEventsCount());

            List<Executable> assertions = new ArrayList<>(3);
            for (int i = 0; i < messages.size(); i++) {
                RawMessage originalMessage = messages.get(i);
                Event event = eventBatch.getEvents(i);
                MessageID id = originalMessage.getMetadata().getId();
                assertions.add(() -> assertEquals(List.of(id), event.getAttachedMessageIdsList(), () -> "Unexpected event order: " + eventBatch));
            }
            assertAll(assertions);
        }
    }

    private CsvCodec createCodec() {
        return createCodec(new CsvCodecConfiguration());
    }

    private CsvCodec createCodec(CsvCodecConfiguration configuration) {
        return new CsvCodec(routerMock, eventRouterMock, EventID.newBuilder().setId("test").build(), configuration);
    }

    private RawMessage createCsvMessage(String data, boolean isHeader) {
        Builder builder = RawMessage.newBuilder()
                .setBody(ByteString.copyFrom(data.getBytes(StandardCharsets.UTF_8)));
        if (isHeader) {
            builder.setMetadata(
                    RawMessageMetadata.newBuilder()
                            .setId(MessageID.newBuilder().setSequence(System.nanoTime()).build())
                            .putProperties("message.type", "header")
                            .build()
            );
        }
        return builder.build();
    }

    private void assertFieldCount(int exceptedCount, Message message) {
        assertEquals(exceptedCount, message.getFieldsCount(), () -> "Message: " + message);
    }

    private String getFieldValue(Message message, String fieldName, Supplier<String> assertMessage) {
        Value value = message.getFieldsMap().get(fieldName);
        assertNotNull(value, assertMessage);
        return value.getSimpleValue();
    }
}