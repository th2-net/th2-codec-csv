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

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;
import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.Message.Builder;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.value.ValueUtils;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

public class CsvCodec implements MessageListener<MessageGroupBatch> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvCodec.class);
    static final String DECODE_IN_ATTRIBUTE = "decode_in";
    static final String DECODE_OUT_ATTRIBUTE = "decode_out";
    private static final String HEADER_MSG_TYPE = "Csv_Header";
    private static final String CSV_MESSAGE_TYPE = "Csv_Message";
    private static final String HEADER_FIELD_NAME = "Header";

    private final MessageRouter<MessageGroupBatch> router;
    private final CsvCodecConfiguration configuration;
    private final String[] defaultHeader;
    private final Charset charset;
    private final MessageRouter<EventBatch> eventRouter;
    private final EventID rootId;

    public CsvCodec(MessageRouter<MessageGroupBatch> router, MessageRouter<EventBatch> eventRouter, EventID rootId, CsvCodecConfiguration configuration) {
        this.router = requireNonNull(router, "'Router' parameter");
        this.eventRouter = requireNonNull(eventRouter, "'Event router' parameter");
        this.configuration = requireNonNull(configuration, "'Configuration' parameter");
        this.rootId = requireNonNull(rootId, "'Root id' parameter");
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

    @Override
    public void handler(String consumerTag, MessageGroupBatch message) {
        try {
            List<ErrorHolder> errors = new ArrayList<>();
            MessageGroupBatch.Builder batchBuilder = MessageGroupBatch.newBuilder();

            for (MessageGroup originalGroup : message.getGroupsList()) {
                MessageGroup.Builder groupBuilder = batchBuilder.addGroupsBuilder();
                for (AnyMessage anyMessage : originalGroup.getMessagesList()) {
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
                            LOGGER.error("The raw message does not contains any data: {}", TextFormat.shortDebugString(rawMessage));
                        }
                        errors.add(new ErrorHolder("The raw message does not contains any data", rawMessage));
                        continue;
                    }
                    decodeCsvData(errors, groupBuilder, rawMessage, data);
                }
            }

            send(batchBuilder.build());
            if (!errors.isEmpty()) {
                reportErrors(errors);
            }

        } catch (Exception ex) {
            LOGGER.error("Cannot process batch for {}: {}", consumerTag, message, ex);
        }
    }

    private void decodeCsvData(Collection<ErrorHolder> errors, MessageGroup.Builder groupBuilder, RawMessage rawMessage, Iterable<String[]> data) {
        RawMessageMetadata originalMetadata = rawMessage.getMetadata();

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
                AnyMessage.Builder messageBuilder = groupBuilder.addMessagesBuilder();
                Builder headerMsg = Message.newBuilder();
                // Not set message type
                setMetadata(originalMetadata, headerMsg, HEADER_MSG_TYPE, currentIndex);
                headerMsg.putFields(HEADER_FIELD_NAME, ValueUtils.toValue(strings));
                messageBuilder.setMessage(headerMsg);
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
            setMetadata(originalMetadata, builder, CSV_MESSAGE_TYPE, currentIndex);

            int headerLength = header.length;
            int rowLength = strings.length;
            for (int i = 0; i < headerLength && i < rowLength; i++) {
                int extraLength = getArrayColumns(i, header);
                if (extraLength == 0) {
                    builder.putFields(header[i], ValueUtils.toValue(strings[i]));
                } else {
                    String[] values = new String[extraLength + 1];
                    System.arraycopy(strings, i, values, 0, extraLength + 1);

                    builder.putFields(header[i], ValueUtils.toValue(values));
                    i+=extraLength;
                }
            }
            messageBuilder.setMessage(builder);
        }
    }

    private int getArrayColumns(int from, String[] headers) {
        int count = 0;
        for (int i = from + 1; i < headers.length; i++) {
            if (headers[i].isEmpty()) {
                count++;
            } else {
                break;
            }
        }
        return count;
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

    private List<String[]> decodeValues(ByteString body) throws IOException {
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

    private void reportErrors(List<ErrorHolder> errors) {
        try {
            EventBatch.Builder errorBatch = EventBatch.newBuilder()
                    .setParentEventId(rootId);
            for (ErrorHolder error : errors) {
                Event event = Event.start()
                        .endTimestamp()
                        .type("DecodingError")
                        .name("Error during decoding data")
                        .status(Status.FAILED)
                        .messageID(error.originalMessage.getMetadata().getId())
                        .bodyData(EventUtils.createMessageBean(error.text));
                if (error.currentRow != null) {
                        event.bodyData(EventUtils.createMessageBean("Current data: " + Arrays.toString(error.currentRow)));
                }
                errorBatch.addEvents(event.toProtoEvent(rootId.getId()));
            }

            eventRouter.send(errorBatch.build());
        } catch (Exception ex) {
            LOGGER.error("Cannot send error to the event store", ex);
        }
    }

    private void send(MessageGroupBatch batch) throws IOException {
        if (batch.getGroupsList().isEmpty()) {
            return;
        }
        router.sendAll(batch, DECODE_OUT_ATTRIBUTE);
    }

    private void trimEachElement(String[] elements) {
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i];
            elements[i] = element.trim();
        }
    }
}
