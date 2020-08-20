package com.exactpro.th2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.configuration.RabbitMQConfiguration;

public class Main {

    private final static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            CsvCodec codec = new CsvCodec(new RabbitMQConfiguration(),
                    args.length > 0 ? args[0] : null,
                    args.length > 1 ? args[1] : null,
                    args.length > 2 ? args[2] : null);

            codec.connectAndBlock();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    codec.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
        } catch (Exception e) {
            LOGGER.error("Error occurred. Exit the program", e);
            System.exit(-1);
        }
    }

}
