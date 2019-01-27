package com.utad.davidonoro.kstreams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class TopologyDefinitions {

    /**
     * Logger
     */
    private static Logger logger = LoggerFactory.getLogger(TopologyDefinitions.class);

    /**
     * JSON helper
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Returns a simple topology that parse strings into Metric events and filter those events
     * with value above 5
     * @param originTopic
     * @param destinyTopic
     * @return
     */
    public static Topology getAlerts(String originTopic, String destinyTopic){
        StreamsBuilder builder = new StreamsBuilder();

        // Stream creation
        KStream<String,String> inputStream = builder.stream(originTopic);

        // Transform json string in metric
        KStream<String,MetricEvent> metricsStream = getMetricsStream(inputStream);

        // Getting alerts
        KStream<String,MetricEvent> alertsStream = metricsStream.filter((key,metric)->metric.getMetric()>5);

        // Transform to String and send to topic
        alertsStream.flatMapValues(metric-> {
            try {
                return Collections.singletonList(mapper.writeValueAsString(metric));
            } catch (JsonProcessingException e) {
                logger.error("Can't write metric: "+e.getMessage());
                return Collections.emptyList();
            }
        }).to(destinyTopic);


        return builder.build();
    }

    /**
     * Sum metrics grouping by sensor id
     * @param originTopic
     * @return
     */
    public static Topology sumMetricsBySensor(String originTopic){
        StreamsBuilder builder = new StreamsBuilder();

        // Stream creation
        KStream<String,String> inputStream = builder.stream(originTopic);

        // Transform json string in metric
        KStream<String,MetricEvent> metricsStream = getMetricsStream(inputStream);

        // Grouping metrics by sensor id
        KStream<String,Integer> rekeyedStream = metricsStream.selectKey((key,metric)->metric.getIdSensor()).mapValues(metric->metric.getMetric());
        KStream<String,Integer> aggMetrics = rekeyedStream.groupByKey(Grouped.with(Serdes.String(),Serdes.Integer())).reduce((aggValue, newValue) -> aggValue + newValue).toStream();

        // Printing results
        aggMetrics.foreach((k,v)->{
            logger.info("Key="+k+"\t Value="+v);
        });

        return builder.build();
    }


    /**
     * Route metrics into different topics based on sensor
     * @param originTopic
     * @return
     */
    public static Topology routeMetrics(String originTopic){
        StreamsBuilder builder = new StreamsBuilder();

        // Stream creation
        KStream<String,String> inputStream = builder.stream(originTopic);

        // Transform json string in metric
        KStream<String,MetricEvent> metricsStream = getMetricsStream(inputStream);

        // choose sensor as key and change to string
        KStream<String,String> stringStream = metricsStream.selectKey((key,metric)->metric.getIdSensor()).flatMapValues(metric-> {
            try {
                return Collections.singletonList(mapper.writeValueAsString(metric));
            } catch (JsonProcessingException e) {
                logger.error("Error writing metric "+e.getMessage());
                return Collections.emptyList();
            }
        });

        // routing based on msg content
        stringStream.to(new TopicNameExtractor<String, String>() {
            @Override
            public String extract(String key, String value, RecordContext recordContext) {
                return "metrics-"+key;
            }
        });

        return builder.build();
    }



    /**
     * Transforms the string stream into a metric stream
     * @param inputStream
     * @return
     */
    private static KStream<String, MetricEvent> getMetricsStream(KStream<String, String> inputStream) {
        return inputStream.flatMapValues(value -> {
            try {
                MetricEvent metric = mapper.readValue(value, MetricEvent.class);
                return Collections.singletonList(metric);
            } catch (Exception e) {
                logger.error("Can't parse message: " + e.toString());
                return Collections.emptyList();
            }
        });
    }
}
