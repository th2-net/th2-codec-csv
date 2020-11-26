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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;
import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageBatch.Builder;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.grpc.Value;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.google.protobuf.ByteString;

public class CsvCodec implements MessageListener<RawMessageBatch> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvCodec.class);
    static final String MESSAGE_TYPE_PROPERTY = "message.type";
    static final String HEADER_TYPE = "header";

    private final MessageRouter<MessageBatch> router;
    private final CsvCodecConfiguration configuration;
    private final String[] defaultHeader;
    private final Charset charset;
    private final MessageRouter<EventBatch> eventRouter;
    private final EventID rootId;

    public CsvCodec(MessageRouter<MessageBatch> router, MessageRouter<EventBatch> eventRouter, EventID rootId, CsvCodecConfiguration configuration) {
        this.router = requireNonNull(router, "'Router' parameter");
        this.eventRouter = requireNonNull(eventRouter, "'Event router' parameter");
        this.configuration = requireNonNull(configuration, "'Configuration' parameter");
        this.rootId = requireNonNull(rootId, "'Root id' parameter");
        List<String> defaultHeader = configuration.getDefaultHeader();
        this.defaultHeader = defaultHeader == null
                ? null
                : defaultHeader.stream().map(String::trim).toArray(String[]::new);
        LOGGER.info("Default header: {}", configuration.getDefaultHeader());
        charset = Charset.forName(configuration.getEncoding());
    }

    @Override
    public void handler(String consumerTag, RawMessageBatch message) {
        try {
            List<ErrorHolder> errors = new ArrayList<>();
            Builder batchBuilder = MessageBatch.newBuilder();

            String[] header = null;
            for (RawMessage rawMessage : message.getMessagesList()) {
                ByteString body = rawMessage.getBody();
                RawMessageMetadata originalMetadata = rawMessage.getMetadata();
                String[] strings = decodeValues(body);
                if (strings.length == 0) {
                    LOGGER.warn("No values decoded from {}", rawMessage);
                    continue;
                }

                trimEachElement(strings);

                if (HEADER_TYPE.equals(originalMetadata.getPropertiesMap().get(MESSAGE_TYPE_PROPERTY))) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Set header to: " + Arrays.toString(strings));
                    }
                    header = strings;
                    continue;
                }
                if (header == null && defaultHeader != null) {
                    LOGGER.info("Use default header for decoding");
                    header = defaultHeader;
                }

                if (header == null) {
                    String msg = "Neither default header or message with " + MESSAGE_TYPE_PROPERTY + "=" + HEADER_TYPE + " were found. Skip current string";
                    LOGGER.error(msg);
                    errors.add(new ErrorHolder(msg, strings, rawMessage));
                    continue;
                }

                if (strings.length != header.length) {
                    String msg = String.format("Wrong fields count in message. Expected count: %d; actual: %d; session alias: %s",
                            header.length, strings.length, originalMetadata.getId().getConnectionId().getSessionAlias());
                    LOGGER.error(msg);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(rawMessage.toString());
                    }
                    errors.add(new ErrorHolder(msg, strings, rawMessage));
                }

                Message.Builder messageBuilder = batchBuilder.addMessagesBuilder();

                // Not set message type
                messageBuilder.setMetadata(MessageMetadata
                        .newBuilder()
                        .setId(originalMetadata.getId())
                        .setTimestamp(originalMetadata.getTimestamp())
                        .putAllProperties(originalMetadata.getPropertiesMap())
                        .setMessageType("Csv_Message")
                );

                int headerLength = header.length;
                int rowLength = strings.length;
                for (int i = 0; i < headerLength && i < rowLength; i++) {
                    messageBuilder.putFields(header[i], Value.newBuilder().setSimpleValue(strings[i]).build());
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

    private String[] decodeValues(ByteString body) throws IOException {
        try (InputStream in = new ByteArrayInputStream(body.toByteArray())) {
            CsvReader reader = new CsvReader(in, configuration.getDelimiter(), charset);
            try {
                reader.readRecord();
                return reader.getValues();
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
    }

    private void reportErrors(List<ErrorHolder> errors) {
        try {
            EventBatch.Builder errorBatch = EventBatch.newBuilder();
            for (ErrorHolder error : errors) {
                errorBatch.addEvents(
                        Event.start()
                                .endTimestamp()
                                .type("DecodingError")
                                .name("Error during decoding data")
                                .status(Status.FAILED)
                                .bodyData(EventUtils.createMessageBean(error.text))
                                .bodyData(EventUtils.createMessageBean("Current data: " + Arrays.toString(error.currentRow)))
                                .messageID(error.originalMessage.getMetadata().getId())
                                .toProtoEvent(rootId.getId())
                );
            }

            eventRouter.send(errorBatch.build());
        } catch (Exception ex) {
            LOGGER.error("Cannot send error to the event store", ex);
        }
    }

    private void send(MessageBatch batch) throws IOException {
        if (batch.getMessagesList().isEmpty()) {
            return;
        }
        router.sendAll(batch);
    }

    private void trimEachElement(String[] elements) {
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i];
            elements[i] = element.trim();
        }
    }
}
