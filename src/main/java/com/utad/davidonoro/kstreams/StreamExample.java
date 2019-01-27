package com.utad.davidonoro.kstreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamExample {

    /**
     * Application id
     */
    static final String appId = "metrics";

    /**
     * Kafka broker
     */
    static final String kafka_broker = "localhost:9092";


    /**
     * Kafka topic to read
     */
    static final Integer maxNumThreads = 1;

    /**
     * Logger
     */
    static Logger logger = LoggerFactory.getLogger(StreamExample.class);


    /**
     * Object which performs the continuous computation
     */
    private static KafkaStreams stream;



    public static void main(String args[]){
        //stream = new KafkaStreams(TopologyDefinitions.getAlerts("metrics","alertmetrics"),getConfig());

        stream = new KafkaStreams(TopologyDefinitions.routeMetrics("metrics"),getConfig());

        stream.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread thread, Throwable throwable) {
                logger.error("Uncaught exception: "+throwable.getMessage());
                closeStream();
                Thread.currentThread().interrupt();
                Runtime.getRuntime().exit(1);
            }
        });

        stream.start();

    }


    /**
     * It stops the streaming computation
     */
    public static void closeStream(){
        stream.close();
    }


    /**
     * Returns the configuration of the streaming: kafka server, zookeeer, aplication id
     * @return
     */
    private static Properties getConfig(){
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG,appId);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,maxNumThreads);

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,kafka_broker);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }
}
