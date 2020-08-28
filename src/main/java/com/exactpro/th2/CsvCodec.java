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
package com.exactpro.th2;

import static java.util.Objects.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.MessageBatch.Builder;
import com.exactpro.th2.infra.grpc.MessageMetadata;
import com.exactpro.th2.infra.grpc.RawMessage;
import com.exactpro.th2.infra.grpc.RawMessageBatch;
import com.exactpro.th2.infra.grpc.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;

public class CsvCodec {

    private final Logger logger = LoggerFactory.getLogger(this.getClass() + "@" + this.hashCode());

    private final RabbitMQConfiguration configuration;

    private final String exchangeName;
    private final String inQueue;
    private final String outQueue;

    private RabbitMqSubscriber listener;
    private Connection senderConnection;
    private Channel sender;

    private List<String> header = null;

    /**
     * Create csv codec instance
     * @param configuration
     * @param exchangeName If null get value from env RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY
     * @param inQueue If null get value from env TH2_CSV_CODEC_IN_QUEUE
     * @param outQueue If null get value from env TH2_CSV_CODEC_OUT_QUEUE
     */
    public CsvCodec(RabbitMQConfiguration configuration, String exchangeName, String inQueue, String outQueue) {
        this.configuration = requireNonNull(configuration, "Rabbit configuration can not be null");
        this.exchangeName = requireNonNull(getDefaultFromEnv(exchangeName, "RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY"));
        this.inQueue = requireNonNull(getDefaultFromEnv(inQueue, "TH2_CSV_CODEC_IN_QUEUE"));
        this.outQueue = requireNonNull(getDefaultFromEnv(outQueue, "TH2_CSV_CODEC_OUT_QUEUE"));;
    }

    public void connect() {
        listener = new RabbitMqSubscriber(exchangeName, this::receive, null, inQueue);
        try {
            listener.startListening(configuration.getHost(),
                    configuration.getVirtualHost(),
                    configuration.getPort(),
                    configuration.getUsername(),
                    configuration.getPassword(),
                    "Csv-codec." + System.currentTimeMillis());
        } catch (IOException | TimeoutException e) {
            throw new IllegalStateException("Can not connect to RabbitMQ", e);
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(configuration.getHost());
        if (StringUtils.isNotEmpty(configuration.getVirtualHost())) {
            factory.setVirtualHost(configuration.getVirtualHost());
        }
        factory.setPort(configuration.getPort());
        if (StringUtils.isNotEmpty(configuration.getUsername())) {
            factory.setUsername(configuration.getUsername());
        }
        if (StringUtils.isNotEmpty(configuration.getPassword())) {
            factory.setPassword(configuration.getPassword());
        }
        try {
            senderConnection = factory.newConnection();
            sender = senderConnection.createChannel();
            sender.exchangeDeclare(exchangeName, "direct");
        } catch (TimeoutException | IOException e) {
            throw new IllegalStateException("Can not connect to RabbitMQ", e);
        }

        logger.info("CSV codec connected");
    }

    public void connectAndBlock() throws InterruptedException {
        connect();
        synchronized (this) {
            wait();
        }
    }

    private void receive(String tag, Delivery delivery) {
        try {
            RawMessageBatch batch = RawMessageBatch.parseFrom(delivery.getBody());

            Builder batchBuilder = MessageBatch.newBuilder();

            for (RawMessage rawMessage : batch.getMessagesList()) {
                ByteString body = rawMessage.getBody();
                String[] strings = body.toString().split(",");

                if (header == null) {
                    header = Arrays.asList(strings);
                    continue;
                }

                if (strings.length != header.size()) {
                    logger.warn("Wrong fields count in message. Session alias = " + rawMessage.getMetadata().getId().getConnectionId().getSessionAlias());
                    if (logger.isDebugEnabled()) {
                        logger.debug(rawMessage.toString());
                    }
                }

                Message.Builder messageBuilder = batchBuilder.addMessagesBuilder();


                // Not set message type
                messageBuilder.setMetadata(MessageMetadata
                        .newBuilder()
                        .setId(rawMessage
                                .getMetadata()
                                .getId())
                        .setTimestamp(rawMessage.getMetadata().getTimestamp())
                );

                for (int i = 0; i < header.size() && i < strings.length; i++) {
                    messageBuilder.putFields(header.get(i), Value.newBuilder().setSimpleValue(strings[i]).build());
                }
            }

            send(batchBuilder.build());

        } catch (InvalidProtocolBufferException e) {
            logger.error("Can not parse raw message batch", e);
        } catch (IOException e) {
            logger.error("Can not send parsed message batch", e);
        }
    }

    public void close() {
        IllegalStateException exception = new IllegalStateException("Can not close CSV codec");

        try {
            listener.close();
        } catch (IOException e) {
            exception.addSuppressed(e);
        }

        try {
            if (senderConnection != null) {
                senderConnection.close();
            }
        } catch (IOException e) {
            exception.addSuppressed(e);
        } finally {
            sender = null;
        }

        if (exception.getSuppressed().length > 0) {
            throw exception;
        }
    }

    private void send(MessageBatch batch) throws IOException {
        synchronized (sender) {
            sender.basicPublish(exchangeName, outQueue, null, batch.toByteArray());
        }
        if(logger.isInfoEnabled()) { logger.info("Sent '{}':'"+ TextFormat.shortDebugString(batch) + '\'', outQueue); }
    }

    private String getDefaultFromEnv(String obj, String envKey) {
        return ObjectUtils.defaultIfNull(obj, System.getenv(envKey));
    }
}
