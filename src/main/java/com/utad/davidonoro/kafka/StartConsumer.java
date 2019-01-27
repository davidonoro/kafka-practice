package com.utad.davidonoro.kafka;

public class StartConsumer {

    static final String TOPIC = "test";

    public static void main(String[] args) {
        Consumer consumer = new Consumer(KafkaProperties.getConsumerProperties());
        consumer.consume(TOPIC);
    }
}
