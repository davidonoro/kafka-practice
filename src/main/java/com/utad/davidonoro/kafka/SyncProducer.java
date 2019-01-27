package com.utad.davidonoro.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SyncProducer {

    Producer<Integer, String> producer;
    Logger logger = LoggerFactory.getLogger(SyncProducer.class);

    /**
     * Constructor
     * @param properties
     */
    public SyncProducer(Properties properties){
        this.producer = new KafkaProducer<Integer, String>(properties);
    }

    /**
     * Send msg
     * @param topic
     * @param key
     * @param msg
     */
    public void sendMessage(String topic, Integer key, String msg){
        ProducerRecord<Integer,String> record = new ProducerRecord<Integer, String>(topic,key,msg);
        try {
            RecordMetadata metadata = producer.send(record).get();
            logDetails(record,metadata);
        } catch (InterruptedException e) {
            logger.error("Error sending msg : "+e.getMessage());
        } catch (ExecutionException e) {
            logger.error("Error sending msg : "+e.getMessage());
        }
    }

    /**
     * Stop the producer
     */
    public void stopProducer(){
        producer.close(1, TimeUnit.SECONDS);
    }

    /**
     * Logging send details
     */
    private void logDetails(ProducerRecord record, RecordMetadata metadata){
        logger.info(String.format("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d)\n",
                record.key(), record.value(), metadata.partition(), metadata.offset()));
    }
}
