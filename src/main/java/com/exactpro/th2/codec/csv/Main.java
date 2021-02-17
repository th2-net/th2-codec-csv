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

import static com.exactpro.th2.common.metrics.CommonMetrics.setLiveness;
import static com.exactpro.th2.common.metrics.CommonMetrics.setReadiness;

import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.codec.csv.cfg.CsvCodecConfiguration;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.fasterxml.jackson.core.JsonProcessingException;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Deque<AutoCloseable> resources = new ConcurrentLinkedDeque<>();
        Lock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        configureShutdownHook(resources, lock, condition);
        try {
            var factory = CommonFactory.createFromArguments(args);
            resources.add(factory);
            setLiveness(true);

            var configuration = factory.getCustomConfiguration(CsvCodecConfiguration.class);
            MessageRouter<MessageGroupBatch> messageGroupRouter = factory.getMessageRouterMessageGroupBatch();
            MessageRouter<EventBatch> eventBatchRouter = factory.getEventBatchRouter();
            var rootEvent = createRootEvent(configuration);
            eventBatchRouter.send(EventBatch.newBuilder().addEvents(rootEvent).build());

            var codec = new CsvCodec(messageGroupRouter, eventBatchRouter, rootEvent.getId(), configuration);
            SubscriberMonitor monitor = Objects.requireNonNull(messageGroupRouter.subscribeAll(codec), "Cannot subscribe to raw queues");
            resources.add(monitor::unsubscribe);

            setReadiness(true);
            awaitShutdown(lock, condition);
        } catch (Exception e) {
            LOGGER.error("Error occurred. Exit the program", e);
            System.exit(-1);
        }
    }

    private static void awaitShutdown(Lock lock, Condition condition) throws InterruptedException {
        try {
            lock.lock();
            LOGGER.info("Wait shutdown");
            condition.await();
            LOGGER.info("App shutdown");
        } finally {
            lock.unlock();
        }
    }

    private static void configureShutdownHook(Deque<AutoCloseable> resources, Lock lock, Condition condition) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutDown(resources, lock, condition), "Shutdown hook"));
    }

    private static void shutDown(Deque<AutoCloseable> resources, Lock lock, Condition condition) {
        LOGGER.info("Shutdown start");
        setReadiness(false);
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }

        resources.descendingIterator().forEachRemaining(resource -> {
            try {
                resource.close();
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        });
        setLiveness(false);
        LOGGER.info("Shutdown end");
    }

    private static com.exactpro.th2.common.grpc.Event createRootEvent(CsvCodecConfiguration cfg) throws JsonProcessingException {
        String displayName = Objects.requireNonNull(cfg.getDisplayName(), "Display name should not be null");
        return Event.start()
                .type("CodecCsv")
                .name(displayName + "_" + System.currentTimeMillis())
                .bodyData(EventUtils.createMessageBean("Root event for CSV coded. All errors during decoding will be attached here"))
                .toProtoEvent(null);
    }
}
