package com.environment.flink.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RecentAggregateState implements Serializable {
    public List<Long> sensorAlertTimes = new ArrayList<>();
    public List<Long> fireTimes = new ArrayList<>();

    public Double maxSmoke10m;
    public Double maxCo210m;
    public Double maxSensorTemp10m;

    public RecentAggregateState() {
    }
}