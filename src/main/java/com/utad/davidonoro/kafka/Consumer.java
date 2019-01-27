package com.utad.davidonoro.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 *
 */
public class Consumer {

    KafkaConsumer<Integer,String> consumer;

    Logger logger = LoggerFactory.getLogger(Consumer.class);

    /**
     * Constructor
     * @param properties
     */
    public Consumer(Properties properties){
        consumer = new KafkaConsumer<Integer, String>(properties);
    }

    public void consume(String topic){
        consumer.subscribe(Arrays.asList(topic));

        while (true){
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(400));
            for (ConsumerRecord<Integer, String> record : records)
                logDetails(record);
        }
    }

    /**
     * Logging send details
     */
    private void logDetails(ConsumerRecord record){
        logger.info(String.format("received record(key=%s value=%s) " + "meta(partition=%d, offset=%d)\n",
                record.key(), record.value(), record.partition(), record.offset()));
    }
}
