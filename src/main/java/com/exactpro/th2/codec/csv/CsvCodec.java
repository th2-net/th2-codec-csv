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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration;
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

    private final Logger logger = LoggerFactory.getLogger(getClass() + "@" + hashCode());
    static final String MESSAGE_TYPE_PROPERTY = "message.type";
    static final String HEADER_TYPE = "header";

    private final MessageRouter<MessageBatch> router;
    private final CsvCodecConfiguration configuration;
    private final String[] defaultHeader;
    private final Charset charset;
    private final CharsetDecoder decoder;

    public CsvCodec(MessageRouter<MessageBatch> router, CsvCodecConfiguration configuration) {
        this.router = requireNonNull(router, "'Router' parameter");
        this.configuration = requireNonNull(configuration, "'Configuration' parameter");
        List<String> defaultHeader = configuration.getDefaultHeader();
        this.defaultHeader = defaultHeader == null
                ? null
                : defaultHeader.stream().map(String::trim).toArray(String[]::new);
        logger.info("Default header: {}", configuration.getDefaultHeader());
        charset = Charset.forName(configuration.getEncoding());
        decoder = charset.newDecoder();
        requireNonNull(configuration.getDelimiter(), "Delimiter must be set");
    }

    @Override
    public void handler(String consumerTag, RawMessageBatch message) {
        try {
            Builder batchBuilder = MessageBatch.newBuilder();

            String[] header = null;
            for (RawMessage rawMessage : message.getMessagesList()) {
                ByteString body = rawMessage.getBody();
                RawMessageMetadata originalMetadata = rawMessage.getMetadata();
                String[] strings = decoder.decode(ByteBuffer.wrap(body.toByteArray())).toString().split(configuration.getDelimiter());

                trimEachElement(strings);

                if (HEADER_TYPE.equals(originalMetadata.getPropertiesMap().get(MESSAGE_TYPE_PROPERTY))) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Set header to: " + Arrays.toString(strings));
                    }
                    header = strings;
                    continue;
                }
                if (header == null && defaultHeader != null) {
                    logger.info("Use default header for decoding");
                    header = defaultHeader;
                }

                if (header == null) {
                    logger.warn("Neither default header or message with {}={} were found. Skip current string", MESSAGE_TYPE_PROPERTY, HEADER_TYPE);
                    continue;
                }

                if (strings.length != header.length) {
                    logger.warn("Wrong fields count in message. Expected count: {}; actual: {}; session alias: {}",
                            header.length, strings.length, originalMetadata.getId().getConnectionId().getSessionAlias());
                    if (logger.isDebugEnabled()) {
                        logger.debug(rawMessage.toString());
                    }
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

        } catch (Exception ex) {
            logger.error("Cannot process batch: {}", message, ex);
        }
    }

    private void send(MessageBatch batch) throws IOException {
        router.sendAll(batch);
    }

    private void trimEachElement(String[] elements) {
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i];
            elements[i] = element.trim();
        }
    }
}
