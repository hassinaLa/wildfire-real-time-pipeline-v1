package com.environment.simulator.model;

public class MeteoEvent {
    private String h3Cell;
    private Double lat;
    private Double lon;
    private Long eventTime;   // epoch millis
    private String eventDate; // yyyy-MM-dd

    private Double temperature;
    private Double humidity;
    private Double windSpeed;
    private Double precipitation;
    private Double soilMoisture;
    private Double vpd;

    public MeteoEvent() {
    }

    public MeteoEvent(String h3Cell, Double lat, Double lon, Long eventTime, String eventDate,
                      Double temperature, Double humidity, Double windSpeed,
                      Double precipitation, Double soilMoisture, Double vpd) {
        this.h3Cell = h3Cell;
        this.lat = lat;
        this.lon = lon;
        this.eventTime = eventTime;
        this.eventDate = eventDate;
        this.temperature = temperature;
        this.humidity = humidity;
        this.windSpeed = windSpeed;
        this.precipitation = precipitation;
        this.soilMoisture = soilMoisture;
        this.vpd = vpd;
    }

    public String getH3Cell() {
        return h3Cell;
    }

    public void setH3Cell(String h3Cell) {
        this.h3Cell = h3Cell;
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public Double getLon() {
        return lon;
    }

    public void setLon(Double lon) {
        this.lon = lon;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getEventDate() {
        return eventDate;
    }

    public void setEventDate(String eventDate) {
        this.eventDate = eventDate;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public Double getHumidity() {
        return humidity;
    }

    public void setHumidity(Double humidity) {
        this.humidity = humidity;
    }

    public Double getWindSpeed() {
        return windSpeed;
    }

    public void setWindSpeed(Double windSpeed) {
        this.windSpeed = windSpeed;
    }

    public Double getPrecipitation() {
        return precipitation;
    }

    public void setPrecipitation(Double precipitation) {
        this.precipitation = precipitation;
    }

    public Double getSoilMoisture() {
        return soilMoisture;
    }

    public void setSoilMoisture(Double soilMoisture) {
        this.soilMoisture = soilMoisture;
    }

    public Double getVpd() {
        return vpd;
    }

    public void setVpd(Double vpd) {
        this.vpd = vpd;
    }

    @Override
    public String toString() {
        return "MeteoEvent{" +
                "h3Cell='" + h3Cell + '\'' +
                ", lat=" + lat +
                ", lon=" + lon +
                ", eventTime=" + eventTime +
                ", eventDate='" + eventDate + '\'' +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                ", windSpeed=" + windSpeed +
                ", precipitation=" + precipitation +
                ", soilMoisture=" + soilMoisture +
                ", vpd=" + vpd +
                '}';
    }
}