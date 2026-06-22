package com.environment.flink.model;

import java.io.Serializable;

public class PredictionResult implements Serializable {
    public String h3Cell;
    public Long eventTime;
    public String eventDate;
    public Integer prediction;
    public Double probability;
    public String riskLevel;
    public Integer suspectedVoluntary;

    public String triggerType;
    public String triggerSource;
    public Boolean isNeighborPrediction;
    public String triggerH3Cell;
    public Boolean confirmedAlert;
    public Boolean fireObserved;
    public Boolean sensorConfirmed;
    public Long confirmationTime;
    public String predictionSlot;

    public PredictionResult() {
    }

    @Override
    public String toString() {
        return "PredictionResult{" +
                "h3Cell='" + h3Cell + '\'' +
                ", eventTime=" + eventTime +
                ", eventDate='" + eventDate + '\'' +
                ", prediction=" + prediction +
                ", probability=" + probability +
                ", riskLevel='" + riskLevel + '\'' +
                ", suspectedVoluntary=" + suspectedVoluntary +
                ", triggerType='" + triggerType + '\'' +
                ", triggerSource='" + triggerSource + '\'' +
                ", isNeighborPrediction=" + isNeighborPrediction +
                ", triggerH3Cell='" + triggerH3Cell + '\'' +
                ", confirmedAlert=" + confirmedAlert +
                ", fireObserved=" + fireObserved +
                ", sensorConfirmed=" + sensorConfirmed +
                ", confirmationTime=" + confirmationTime +
                ", predictionSlot='" + predictionSlot + '\'' +
                '}';
    }
}