package com.environment.simulator.model;

public class SensorEvent {
    private String h3Cell;
    private Double lat;
    private Double lon;
    private Long eventTime;   // epoch millis
    private String eventDate; // yyyy-MM-dd

    private Double co2;
    private Double smoke;
    private Double sensorTemperature;
    private Double battery;
    private String alertLevel;
    private Boolean fireRelated;

    public SensorEvent() {
    }

    public SensorEvent(String h3Cell, Double lat, Double lon, Long eventTime, String eventDate,
                       Double co2, Double smoke, Double sensorTemperature,
                       Double battery, String alertLevel, Boolean fireRelated) {
        this.h3Cell = h3Cell;
        this.lat = lat;
        this.lon = lon;
        this.eventTime = eventTime;
        this.eventDate = eventDate;
        this.co2 = co2;
        this.smoke = smoke;
        this.sensorTemperature = sensorTemperature;
        this.battery = battery;
        this.alertLevel = alertLevel;
        this.fireRelated = fireRelated;
    }

    public String getH3Cell() { return h3Cell; }
    public void setH3Cell(String h3Cell) { this.h3Cell = h3Cell; }

    public Double getLat() { return lat; }
    public void setLat(Double lat) { this.lat = lat; }

    public Double getLon() { return lon; }
    public void setLon(Double lon) { this.lon = lon; }

    public Long getEventTime() { return eventTime; }
    public void setEventTime(Long eventTime) { this.eventTime = eventTime; }

    public String getEventDate() { return eventDate; }
    public void setEventDate(String eventDate) { this.eventDate = eventDate; }

    public Double getCo2() { return co2; }
    public void setCo2(Double co2) { this.co2 = co2; }

    public Double getSmoke() { return smoke; }
    public void setSmoke(Double smoke) { this.smoke = smoke; }

    public Double getSensorTemperature() { return sensorTemperature; }
    public void setSensorTemperature(Double sensorTemperature) { this.sensorTemperature = sensorTemperature; }

    public Double getBattery() { return battery; }
    public void setBattery(Double battery) { this.battery = battery; }

    public String getAlertLevel() { return alertLevel; }
    public void setAlertLevel(String alertLevel) { this.alertLevel = alertLevel; }

    public Boolean getFireRelated() { return fireRelated; }
    public void setFireRelated(Boolean fireRelated) { this.fireRelated = fireRelated; }

    @Override
    public String toString() {
        return "SensorEvent{" +
                "h3Cell='" + h3Cell + '\'' +
                ", lat=" + lat +
                ", lon=" + lon +
                ", eventTime=" + eventTime +
                ", eventDate='" + eventDate + '\'' +
                ", co2=" + co2 +
                ", smoke=" + smoke +
                ", sensorTemperature=" + sensorTemperature +
                ", battery=" + battery +
                ", alertLevel='" + alertLevel + '\'' +
                ", fireRelated=" + fireRelated +
                '}';
    }
}