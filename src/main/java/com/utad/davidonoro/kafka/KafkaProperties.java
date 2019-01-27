package com.utad.davidonoro.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class KafkaProperties {

    static String serverKafka = "localhost:9092";

    public static Properties getProducerProperties(){
        Properties properties = new Properties();

        properties.put("bootstrap.servers", serverKafka);
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put("key.serializer", IntegerSerializer.class.getName());

        return properties;
    }

    public static Properties getConsumerProperties(){
        Properties properties = new Properties();

        properties.put("bootstrap.servers", serverKafka);
        properties.put("group.id", "mygroup");
        properties.put("enable.auto.commit", true);
        properties.put("auto.commit.interval.ms", 1000);
        properties.put("session.timeout.ms", 30000);
        properties.put("key.deserializer", IntegerDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        return properties;
    }
}
