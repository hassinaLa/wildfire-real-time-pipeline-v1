package com.environment.flink.model;

import java.io.Serializable;

public class DailyH3TrainingRow implements Serializable {
    public String h3Cell;
    public String eventDate;

    public String h3Commune;

    public Double h3CenterLat;
    public Double h3CenterLon;

    public Double altitudeMoy;
    public Double penteMoy;
    public Double distRouteM;
    public Double distUrbainM;

    public Double humidityMin;
    public Double temperatureMoy;
    public Double windSpeedMax;
    public Double precipitation1dMm;
    public Double precipitation7dSumMm;
    public Double soilMoisture;
    public Double vpdKpa;

    public Double ndvi30dMean;
    public Double ndvi30dMin;
    public Double ndvi30dStd;

    public Integer month;
    public Double monthSin;
    public Double monthCos;
    public Double doySin;
    public Double doyCos;
    public Integer isWeekend;

    public Long inputEventTime;
    public String inputSlot;

    public Integer label;
    public String confirmationSource;
    public Integer suspectedVoluntary;

    public Boolean fireObserved;
    public Boolean sensorConfirmed;

    public DailyH3TrainingRow() {
    }
}