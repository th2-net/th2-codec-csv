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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.exactpro.th2.codec.DecodeException;
import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.ListValue;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessage.Builder;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.grpc.Value;
import com.google.protobuf.ByteString;

class TestCsvCodec {
    @Nested
    class TestPositive {

        @Test
void decodeArrayWithDifferentLength() throws IOException {
            CsvCodecConfiguration configuration = new CsvCodecConfiguration();
            configuration.setValidateLength(false);
            CsvCodec codec = createCodec(configuration);
            MessageGroup group = MessageGroup.newBuilder()
                    .addMessages(createCsvMessage("A,B, , ,", "1,2,3,4"))
                    .build();
            MessageGroup value = codec.decode(group);
            assertEquals(2, value.getMessagesCount());

            Message header = getMessage(value, 0);
            assertFieldCount(1, header);
            Message message = getMessage(value, 1);
            assertFieldCount(2, message);

            assertAll(
                    () -> assertAll("Current message: " + header,
                            () -> assertEquals("Csv_Header", header.getMetadata().getMessageType()),
                            () -> {
                                assertEquals(1, header.getMetadata().getId().getSubsequenceCount());
                                assertEquals(1, header.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertFieldValueEquals(header, "Header", listValue("A", "B", "", "", ""))
                    ),
                    () -> assertAll("Current message: " + message,
                            () -> {
                                assertEquals(1, message.getMetadata().getId().getSubsequenceCount());
                                assertEquals(2, message.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertEquals("1", getFieldValue(message, "A", () -> "No field A. " + message)),
                            () -> {
                                var listValues = getListValue(message, "B", () -> "No field B. " + message);
                                assertEquals(3, listValues.length);
                                assertEquals("2", listValues[0]);
                                assertEquals("3", listValues[1]);
                                assertEquals("4", listValues[2]);
                            }
                    )
            );
        }

        @Test
        void decodeArrayInEnd() throws IOException {
            CsvCodecConfiguration configuration = new CsvCodecConfiguration();
            configuration.setValidateLength(false);
            CsvCodec codec = createCodec(configuration);
            MessageGroup group = MessageGroup.newBuilder()
                    .addMessages(createCsvMessage("A,B,C ,", "1,2,3"))
                    .build();
            MessageGroup value = codec.decode(group);
            assertEquals(2, value.getMessagesCount());

            Message header = getMessage(value, 0);
            assertFieldCount(1, header);
            Message message = getMessage(value, 1);
            assertFieldCount(3, message);

            assertAll(
                    () -> assertAll("Current message: " + header,
                            () -> assertEquals("Csv_Header", header.getMetadata().getMessageType()),
                            () -> {
                                assertEquals(1, header.getMetadata().getId().getSubsequenceCount());
                                assertEquals(1, header.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertFieldValueEquals(header, "Header", listValue("A", "B", "C", ""))
                    ),
                    () -> assertAll("Current message: " + message,
                            () -> {
                                assertEquals(1, message.getMetadata().getId().getSubsequenceCount());
                                assertEquals(2, message.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertEquals("1", getFieldValue(message, "A", () -> "No field A. " + message)),
                            () -> assertEquals("2", getFieldValue(message, "B", () -> "No field B. " + message)),
                            () -> {
                                var listValues = getListValue(message, "C", () -> "No field C. " + message);
                                assertEquals(1, listValues.length);
                                assertEquals("3", listValues[0]);
                            }
                    )
            );
        }

        @Test
        void decodeArrayInMiddle() throws IOException {
            CsvCodec codec = createCodec();
            MessageGroup group = MessageGroup.newBuilder()
                    .addMessages(createCsvMessage("A,B, ,C", "1,2,3,4"))
                    .build();
            MessageGroup value = codec.decode(group);
            assertEquals(2, value.getMessagesCount());

            Message header = getMessage(value, 0);
            assertFieldCount(1, header);
            Message message = getMessage(value, 1);
            assertFieldCount(3, message);

            assertAll(
                    () -> assertAll("Current message: " + header,
                            () -> assertEquals("Csv_Header", header.getMetadata().getMessageType()),
                            () -> {
                                assertEquals(1, header.getMetadata().getId().getSubsequenceCount());
                                assertEquals(1, header.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertFieldValueEquals(header, "Header", listValue("A", "B", "", "C"))
                    ),
                    () -> assertAll("Current message: " + message,
                            () -> {
                                assertEquals(1, message.getMetadata().getId().getSubsequenceCount());
                                assertEquals(2, message.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertEquals("1", getFieldValue(message, "A", () -> "No field A. " + message)),
                            () -> {
                                var listValues = getListValue(message, "B", () -> "No field B. " + message);
                                assertEquals(2, listValues.length);
                                assertEquals("2", listValues[0]);
                                assertEquals("3", listValues[1]);
                            },
                            () -> assertEquals("4", getFieldValue(message, "C", () -> "No field C. " + message))
                    )
            );
        }

        @Test
        void decodesDataAndSkipsHeader() {
            CsvCodec codec = createCodec();
            MessageGroup group = MessageGroup.newBuilder()
                            .addMessages(createCsvMessage("A,B,C", "1,2,3"))
                    .build();

            MessageGroup value = codec.decode(group);
            assertEquals(2, value.getMessagesCount());

            Message header = getMessage(value, 0);
            assertFieldCount(1, header);
            Message message = getMessage(value, 1);
            assertFieldCount(3, message);

            assertAll(
                    () -> assertAll("Current message: " + header,
                            () -> assertEquals("Csv_Header", header.getMetadata().getMessageType()),
                            () -> {
                                assertEquals(1, header.getMetadata().getId().getSubsequenceCount());
                                assertEquals(1, header.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertFieldValueEquals(header, "Header", listValue("A", "B", "C"))
                    ),
                    () -> assertAll("Current message: " + message,
                            () -> {
                                assertEquals(1, message.getMetadata().getId().getSubsequenceCount());
                                assertEquals(2, message.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertEquals("1", getFieldValue(message, "A", () -> "No field A. " + message)),
                            () -> assertEquals("2", getFieldValue(message, "B", () -> "No field B. " + message)),
                            () -> assertEquals("3", getFieldValue(message, "C", () -> "No field C. " + message))
                    )
            );
        }

        @Test
        void trimsEndOfTheLine() {
            CsvCodec codec = createCodec();

            MessageGroup group = MessageGroup.newBuilder()
                    .addMessages(createCsvMessage("A,B,C\n\r1,2,3\n"))
                    .build();
            MessageGroup value = codec.decode(group);
            assertEquals(2, value.getMessagesCount());

            Message header = getMessage(value, 0);
            assertFieldCount(1, header);
            Message message = getMessage(value, 1);
            assertFieldCount(3, message);
            assertAll(
                    () -> assertAll("Current message: " + header,
                            () -> {
                                assertEquals(1, header.getMetadata().getId().getSubsequenceCount());
                                assertEquals(1, header.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertEquals("Csv_Header", header.getMetadata().getMessageType()),
                            () -> assertFieldValueEquals(header, "Header", listValue("A", "B", "C"))
                    ),
                    () -> assertAll("Current message: " + message,
                            () -> {
                                assertEquals(1, message.getMetadata().getId().getSubsequenceCount());
                                assertEquals(2, message.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertEquals("1", getFieldValue(message, "A", () -> "No field A. " + message)),
                            () -> assertEquals("2", getFieldValue(message, "B", () -> "No field B. " + message)),
                            () -> assertEquals("3", getFieldValue(message, "C", () -> "No field C. " + message))
                    )
            );
        }

        @Test
        void decodesDataUsingDefaultHeader() {
            CsvCodecConfiguration configuration = new CsvCodecConfiguration();
            configuration.setDefaultHeader(List.of("A", "B", "C"));
            CsvCodec codec = createCodec(configuration);

            MessageGroup group = MessageGroup.newBuilder()
                    .addMessages(
                            createCsvMessage("1,2,3")
                    )
                    .build();
            MessageGroup value = codec.decode(group);
            assertEquals(1, value.getMessagesCount());

            Message message = getMessage(value, 0);
            assertFieldCount(3, message);
            assertAll("Current message: " + message,
                    () -> {
                        assertEquals(1, message.getMetadata().getId().getSubsequenceCount());
                        assertEquals(1, message.getMetadata().getId().getSubsequence(0));
                    },
                    () -> assertEquals("1", getFieldValue(message, "A", () -> "No field A. " + message)),
                    () -> assertEquals("2", getFieldValue(message, "B", () -> "No field B. " + message)),
                    () -> assertEquals("3", getFieldValue(message, "C", () -> "No field C. " + message))
            );
        }

        @Test
        void decodesDataWithEscapedCharacters() {
            CsvCodec codec = createCodec();

            MessageGroup group = MessageGroup.newBuilder()
                    .addMessages(
                            createCsvMessage("A,B", "\"1,2\",\"\"\"value\"\"\"")
                    )
                    .build();
            MessageGroup value = codec.decode(group);
            assertEquals(2, value.getMessagesCount());

            Message header = getMessage(value, 0);
            assertFieldCount(1, header);
            Message message = getMessage(value, 1);
            assertFieldCount(2, message);

            assertAll(
                    () -> assertAll("Current message: " + header,
                            () -> {
                                assertEquals(1, header.getMetadata().getId().getSubsequenceCount());
                                assertEquals(1, header.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertEquals("Csv_Header", header.getMetadata().getMessageType()),
                            () -> assertFieldValueEquals(header, "Header", listValue("A", "B"))
                    ),
                    () -> assertAll("Current message: " + message,
                            () -> {
                                assertEquals(1, message.getMetadata().getId().getSubsequenceCount());
                                assertEquals(2, message.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertEquals("1,2", getFieldValue(message, "A", () -> "No field A. " + message)),
                            () -> assertEquals("\"value\"", getFieldValue(message, "B", () -> "No field B. " + message))
                    )
            );
        }

        @Test
        void decodesDataCustomDelimiter() {
            CsvCodecConfiguration configuration = new CsvCodecConfiguration();
            configuration.setDelimiter(';');
            CsvCodec codec = createCodec(configuration);

            MessageGroup group = MessageGroup.newBuilder()
                    .addMessages(
                            createCsvMessage("A;B", "1,2;3")
                    )
                    .build();
            MessageGroup value = codec.decode(group);
            assertEquals(2, value.getMessagesCount());

            Message header = getMessage(value, 0);
            assertFieldCount(1, header);
            Message message = getMessage(value, 1);
            assertFieldCount(2, message);

            assertAll(
                    () -> assertAll("Current message: " + header,
                            () -> {
                                assertEquals(1, header.getMetadata().getId().getSubsequenceCount());
                                assertEquals(1, header.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertEquals("Csv_Header", header.getMetadata().getMessageType()),
                            () -> assertFieldValueEquals(header, "Header", listValue("A", "B"))
                    ),
                    () -> assertAll("Current message: " + message,
                            () -> {
                                assertEquals(1, message.getMetadata().getId().getSubsequenceCount());
                                assertEquals(2, message.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertEquals("1,2", getFieldValue(message, "A", () -> "No field A. " + message)),
                            () -> assertEquals("3", getFieldValue(message, "B", () -> "No field B. " + message))
                    )
            );
        }

        @Test
        void trimsWhitespacesDuringDecoding() {
            CsvCodec codec = createCodec();

            MessageGroup group = MessageGroup.newBuilder()
                    .addMessages(
                            createCsvMessage("A, B, C", "1, , 3 3")
                    )
                    .build();
            MessageGroup value = codec.decode(group);
            assertEquals(2, value.getMessagesCount());

            Message header = getMessage(value, 0);
            assertFieldCount(1, header);
            Message message = getMessage(value, 1);
            assertFieldCount(3, message);

            assertAll(
                    () -> assertAll("Current message: " + header,
                            () -> {
                                assertEquals(1, header.getMetadata().getId().getSubsequenceCount());
                                assertEquals(1, header.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertEquals("Csv_Header", header.getMetadata().getMessageType()),
                            () -> assertFieldValueEquals(header, "Header", listValue("A", "B", "C"))
                    ),
                    () -> assertAll("Current message: " + message,
                            () -> {
                                assertEquals(1, message.getMetadata().getId().getSubsequenceCount());
                                assertEquals(2, message.getMetadata().getId().getSubsequence(0));
                            },
                            () -> assertEquals("1", getFieldValue(message, "A", () -> "No field A. " + message)),
                            () -> assertEquals("", getFieldValue(message, "B", () -> "No field B. " + message)),
                            () -> assertEquals("3 3", getFieldValue(message, "C", () -> "No field C. " + message))
                    )
            );
        }
    }

    @Nested
    class TestNegative {
        @Test
        void reportsErrorIfNotDataFound() {
            CsvCodec codec = createCodec();
            Assertions.assertThrows(DecodeException.class, () ->
                    codec.decode(MessageGroup.newBuilder().addMessages(createCsvMessage("")).build()));
        }

        @Test
        void reportsErrorIfRawDataIsEmpty() {
            CsvCodec codec = createCodec();

            Assertions.assertThrows(DecodeException.class, () ->
                    codec.decode(MessageGroup.newBuilder()
                            .addMessages(createCsvMessage("A,B,C"))
                            .addMessages(createCsvMessage(""))
                            .build())
            );
        }

        @Test
        void reportsErrorIfDefaultHeaderAndDataHaveDifferentSize() {
            CsvCodecConfiguration configuration = new CsvCodecConfiguration();
            configuration.setDefaultHeader(List.of("A", "B"));
            CsvCodec codec = createCodec(configuration);

            assertThrows(DecodeException.class, () ->
                    codec.decode(MessageGroup.newBuilder()
                            .addMessages(createCsvMessage("1,2,3")).build())
            );
        }
    }

    private CsvCodec createCodec() {
        return createCodec(new CsvCodecConfiguration());
    }

    private CsvCodec createCodec(CsvCodecConfiguration configuration) {
        return new CsvCodec(configuration);
    }

    private AnyMessage createCsvMessage(String... data) {
        Builder builder = RawMessage.newBuilder()
                .setBody(ByteString.copyFrom(String.join(StringUtils.LF, data).getBytes(StandardCharsets.UTF_8)));
        RawMessageMetadata.Builder metadataBuilder = RawMessageMetadata.newBuilder()
                .setId(MessageID.newBuilder().setSequence(System.nanoTime()).build());
        builder.setMetadata(metadataBuilder.build());
        return AnyMessage.newBuilder().setRawMessage(builder).build();
    }

    private void assertFieldCount(int exceptedCount, Message message) {
        assertEquals(exceptedCount, message.getFieldsCount(), () -> "Message: " + message);
    }

    private String getFieldValue(Message message, String fieldName, Supplier<String> assertMessage) {
        Value value = message.getFieldsMap().get(fieldName);
        assertNotNull(value, assertMessage);
        return value.getSimpleValue();
    }

    private String[] getListValue(Message message, String fieldName, Supplier<String> assertMessage) {
        Value value = message.getFieldsMap().get(fieldName);
        assertNotNull(value, assertMessage);
        return value.getListValue().getValuesList().stream().map(Value::getSimpleValue).toArray(String[]::new);
    }

    private void assertFieldValueEquals(Message message, String fieldName, Value expectedValue) {
        Value actualValue = message.getFieldsMap().get(fieldName);
        assertEquals(expectedValue, actualValue, () -> "Unexpected value in " + fieldName + " field. Message: " + message);
    }

    private static Value listValue(String... values) {
        ListValue.Builder listValue = ListValue.newBuilder();
        for (String value : values) {
            listValue.addValues(Value.newBuilder().setSimpleValue(value).build());
        }

        return Value.newBuilder()
                .setListValue(listValue)
                .build();
    }

    private static Message getMessage(MessageGroup value, int i) {
        AnyMessage anyMessage = value.getMessages(i);
        return getMessage(anyMessage);
    }

    private static Message getMessage(AnyMessage anyMessage) {
        assertTrue(anyMessage.hasMessage(), () -> "Does not have parsed message: " + anyMessage);
        return anyMessage.getMessage();
    }
}