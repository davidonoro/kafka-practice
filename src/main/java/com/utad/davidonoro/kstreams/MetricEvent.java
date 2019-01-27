package com.utad.davidonoro.kstreams;

/**
 * {"idSensor":"id1", "metric":4}
 */
public class MetricEvent {

    private String idSensor;
    private Integer metric;

    public String getIdSensor() {
        return idSensor;
    }

    public void setIdSensor(String idSensor) {
        this.idSensor = idSensor;
    }

    public Integer getMetric() {
        return metric;
    }

    public void setMetric(Integer metric) {
        this.metric = metric;
    }
}
