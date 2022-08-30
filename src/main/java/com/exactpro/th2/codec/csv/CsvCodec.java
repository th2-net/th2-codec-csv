/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;
import com.exactpro.th2.codec.DecodeException;
import com.exactpro.th2.codec.api.IPipelineCodec;
import com.exactpro.th2.codec.api.IReportingContext;
import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.Message.Builder;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.value.ValueUtils;
import com.google.protobuf.ByteString;

public class CsvCodec implements IPipelineCodec {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvCodec.class);
    private static final String HEADER_MSG_TYPE = "Csv_Header";
    private static final String CSV_MESSAGE_TYPE = "Csv_Message";
    private static final String HEADER_FIELD_NAME = "Header";

    private static final String DEFAULT_MESSAGE_TYPE_PROP_NAME_LOWERCASE = "message_type";
    private static final String DEFAULT_MESSAGE_TYPE_PROP_NAME_UPPERCASE = "MESSAGE_TYPE";

    private final CsvCodecConfiguration configuration;
    private final String[] defaultHeader;
    private final Charset charset;

    public CsvCodec(CsvCodecConfiguration configuration) {
        this.configuration = requireNonNull(configuration, "'Configuration' parameter");
        List<String> defaultHeader = configuration.getDefaultHeader();
        this.defaultHeader = defaultHeader == null
                ? null
                : defaultHeader.stream().map(String::trim).toArray(String[]::new);
        if (this.defaultHeader != null && this.defaultHeader.length == 0) {
            throw new IllegalArgumentException("Default header must not be empty");
        }
        LOGGER.info("Default header: {}", configuration.getDefaultHeader());
        charset = Charset.forName(configuration.getEncoding());
    }

    @NotNull
    @Override
    public MessageGroup decode(@NotNull MessageGroup messageGroup) {
        MessageGroup.Builder groupBuilder = MessageGroup.newBuilder();
        Collection<ErrorHolder> errors = new ArrayList<>();
        for (AnyMessage anyMessage : messageGroup.getMessagesList()) {
            if (anyMessage.hasMessage()) {
                groupBuilder.addMessages(anyMessage);
                continue;
            }
            if (!anyMessage.hasRawMessage()) {
                LOGGER.error("Message should either have a raw or parsed message but has nothing: {}", anyMessage);
                continue;
            }
            RawMessage rawMessage = anyMessage.getRawMessage();
            ByteString body = rawMessage.getBody();
            List<String[]> data = decodeValues(body);
            if (data.isEmpty()) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("The raw message does not contains any data: {}", MessageUtils.toJson(rawMessage));
                }
                errors.add(new ErrorHolder("The raw message does not contains any data", rawMessage));
                continue;
            }
            decodeCsvData(errors, groupBuilder, rawMessage, data);
        }
        if (!errors.isEmpty()) {
            throw createException(errors);
        }
        return groupBuilder.build();
    }

    @NotNull
    @Override
    public MessageGroup decode(@NotNull MessageGroup messageGroup, @NotNull IReportingContext iReportingContext) {
        return decode(messageGroup);
    }

    private RuntimeException createException(Collection<ErrorHolder> errors) {
        return new DecodeException(
                "Cannot decode some messages: " + System.lineSeparator() + errors.stream()
                        .map(it -> "Message " + MessageUtils.toJson(it.originalMessage.getMetadata().getId()) + " cannot be decoded because " + it.text)
                        .collect(Collectors.joining(System.lineSeparator()))
        );
    }

    @NotNull
    @Override
    public MessageGroup encode(@NotNull MessageGroup messageGroup) {
        throw new UnsupportedOperationException("encode method is not implemented");
    }

    @NotNull
    @Override
    public MessageGroup encode(@NotNull MessageGroup messageGroup, @NotNull IReportingContext iReportingContext) {
        throw new UnsupportedOperationException("encode method is not implemented");
    }

    @Override
    public void close() {
    }

    private void decodeCsvData(Collection<ErrorHolder> errors, MessageGroup.Builder groupBuilder, RawMessage rawMessage, Iterable<String[]> data) {
        RawMessageMetadata originalMetadata = rawMessage.getMetadata();

        final String outputMessageType;
        if (configuration.getMessageTypePropertyName() != null) {
            outputMessageType = originalMetadata.getPropertiesOrDefault(configuration.getMessageTypePropertyName(), CSV_MESSAGE_TYPE);
        } else {
            if (originalMetadata.containsProperties(DEFAULT_MESSAGE_TYPE_PROP_NAME_LOWERCASE)) {
                outputMessageType = originalMetadata.getPropertiesOrThrow(DEFAULT_MESSAGE_TYPE_PROP_NAME_LOWERCASE);
            } else if (originalMetadata.containsProperties(DEFAULT_MESSAGE_TYPE_PROP_NAME_UPPERCASE)) {
                outputMessageType = originalMetadata.getPropertiesOrThrow(DEFAULT_MESSAGE_TYPE_PROP_NAME_UPPERCASE);
            } else {
                outputMessageType = CSV_MESSAGE_TYPE;
            }
        }

        int currentIndex = 0;
        String[] header = defaultHeader;
        for (String[] strings : data) {
            currentIndex++;

            if (strings.length == 0) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Empty raw at {} index (starts with 1). Data: {}", currentIndex, data);
                }
                errors.add(new ErrorHolder("Empty raw at " + currentIndex + " index (starts with 1)", rawMessage));
                continue;
            }

            trimEachElement(strings);

            if (header == null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Set header to: " + Arrays.toString(strings));
                }
                header = strings;

                if (configuration.isPublishHeader()) {
                    AnyMessage.Builder messageBuilder = groupBuilder.addMessagesBuilder();
                    Builder headerMsg = Message.newBuilder();
                    // Not set message type
                    setMetadata(originalMetadata, headerMsg, HEADER_MSG_TYPE, currentIndex);
                    headerMsg.putFields(HEADER_FIELD_NAME, ValueUtils.toValue(strings));
                    messageBuilder.setMessage(headerMsg);
                }

                continue;
            }

            if (strings.length != header.length && configuration.getValidateLength()) {
                String msg = String.format("Wrong fields count in message. Expected count: %d; actual: %d; session alias: %s",
                        header.length, strings.length, originalMetadata.getId().getConnectionId().getSessionAlias());
                LOGGER.error(msg);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(rawMessage.toString());
                }
                errors.add(new ErrorHolder(msg, strings, rawMessage));
            }

            AnyMessage.Builder messageBuilder = groupBuilder.addMessagesBuilder();

            Builder builder = Message.newBuilder();
            // Not set message type
            setMetadata(originalMetadata, builder, outputMessageType, currentIndex);

            int headerLength = header.length;
            int rowLength = strings.length;
            for (int i = 0; i < headerLength && i < rowLength; ) {
                int extraLength = getHeaderArrayLength(header, i);
                if (extraLength == 1) {
                    builder.putFields(header[i], ValueUtils.toValue(strings[i]));
                    i++;
                } else {
                    String[] values = copyArray(strings, i, i+extraLength);
                    builder.putFields(header[i], ValueUtils.toValue(values));
                    i+=extraLength;
                }
            }

            messageBuilder.setMessage(builder);
        }
    }

    public static String [] copyArray(String [] original, int from, int to){
        String [] copyArr = new String[Integer.min(to, original.length) - from];
        for (int i = from; i < to && i < original.length; i++){
            copyArr[i-from] = original[i];
        }
        return copyArr;
    }

    private int getHeaderArrayLength(String[] header, int index) {
        int length = 1;
        for (int i = index + 1; i < header.length && header[i].isEmpty(); i++) {
            length++;
        }
        return length;
    }

    private void setMetadata(RawMessageMetadata originalMetadata, Message.Builder messageBuilder, String messageType, int currentIndex) {
        messageBuilder.setMetadata(MessageMetadata
                .newBuilder()
                .setId(MessageID.newBuilder(originalMetadata.getId())
                        .addSubsequence(currentIndex)
                        .build())
                .setTimestamp(originalMetadata.getTimestamp())
                .putAllProperties(originalMetadata.getPropertiesMap())
                .setMessageType(messageType)
        );
    }

    private List<String[]> decodeValues(ByteString body) {
        try (InputStream in = new ByteArrayInputStream(body.toByteArray())) {
            CsvReader reader = new CsvReader(in, configuration.getDelimiter(), charset);
            try {
                List<String[]> result = new ArrayList<>();
                while (reader.readRecord()) {
                    result.add(reader.getValues());
                }
                return result;
            } finally {
                reader.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("cannot read data from raw bytes", e);
        }
    }

    private static class ErrorHolder {
        public final String text;
        public final String[] currentRow;
        public final RawMessage originalMessage;

        private ErrorHolder(String text, String[] currentRow, RawMessage originalMessage) {
            this.text = text;
            this.currentRow = currentRow;
            this.originalMessage = originalMessage;
        }

        private ErrorHolder(String text, RawMessage originalMessage) {
            this(text, null, originalMessage);
        }
    }

    private void trimEachElement(String[] elements) {
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i];
            elements[i] = element.trim();
        }
    }
}