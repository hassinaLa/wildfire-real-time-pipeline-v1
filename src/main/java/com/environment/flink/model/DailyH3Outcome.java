package com.environment.flink.model;

import java.io.Serializable;

public class DailyH3Outcome implements Serializable {
    public String h3Cell;
    public String eventDate;

    public Boolean sensorConfirmed;
    public Boolean fireObserved;
    public Boolean fireConfirmedWithSensor;

    public Long confirmationTime;
    public String confirmationSource;

    public Integer lastPredictionBeforeAlert;
    public Double lastProbabilityBeforeAlert;
    public String lastRiskBeforeAlert;

    public Integer suspectedVoluntary;

    public Long latestMeteoEventTime;
    public String latestMeteoSlot;

    public DailyH3Outcome() {
    }
}