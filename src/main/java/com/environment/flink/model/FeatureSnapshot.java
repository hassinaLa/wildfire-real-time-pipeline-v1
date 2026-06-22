package com.environment.flink.model;

import java.io.Serializable;

public class FeatureSnapshot implements Serializable {
    public String h3Cell;
    public String h3Commune;
    public Long eventTime;
    public String eventDate;

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

    public FeatureSnapshot() {
    }

    @Override
    public String toString() {
        return "FeatureSnapshot{" +
                "h3Cell='" + h3Cell + '\'' +
                ", h3Commune='" + h3Commune + '\'' +
                ", eventTime=" + eventTime +
                ", eventDate='" + eventDate + '\'' +
                ", h3CenterLat=" + h3CenterLat +
                ", h3CenterLon=" + h3CenterLon +
                ", altitudeMoy=" + altitudeMoy +
                ", penteMoy=" + penteMoy +
                ", distRouteM=" + distRouteM +
                ", distUrbainM=" + distUrbainM +
                ", humidityMin=" + humidityMin +
                ", temperatureMoy=" + temperatureMoy +
                ", windSpeedMax=" + windSpeedMax +
                ", precipitation1dMm=" + precipitation1dMm +
                ", precipitation7dSumMm=" + precipitation7dSumMm +
                ", soilMoisture=" + soilMoisture +
                ", vpdKpa=" + vpdKpa +
                ", ndvi30dMean=" + ndvi30dMean +
                ", ndvi30dMin=" + ndvi30dMin +
                ", ndvi30dStd=" + ndvi30dStd +
                ", month=" + month +
                ", monthSin=" + monthSin +
                ", monthCos=" + monthCos +
                ", doySin=" + doySin +
                ", doyCos=" + doyCos +
                ", isWeekend=" + isWeekend +
                '}';
    }
}