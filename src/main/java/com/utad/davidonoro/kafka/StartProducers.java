package com.utad.davidonoro.kafka;

public class StartProducers {

    static final String TOPIC = "test";

    public static void main(String[] args) {

        SyncProducer producer = new SyncProducer(KafkaProperties.getProducerProperties());
        AsyncProducer aSyncProducer = new AsyncProducer(KafkaProperties.getProducerProperties());

        for(int i = 0;i<=10;i++){
            producer.sendMessage(TOPIC,i,"msg"+i);
            aSyncProducer.sendMessage(TOPIC,i,"async-msg"+i);
        }


        producer.stopProducer();
        aSyncProducer.stopProducer();
    }
}
