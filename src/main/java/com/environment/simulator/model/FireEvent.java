package com.environment.simulator.model;

public class FireEvent {
    private String h3Cell;
    private Double lat;
    private Double lon;
    private Long eventTime;    // ignition / observed time in epoch millis
    private String eventDate;  // yyyy-MM-dd

    private Long declaredAt;   // epoch millis, nullable
    private String agentId;
    private Double intensity;
    private Double burnedArea;
    private String cause;
    private Boolean declared;

    public FireEvent() {
    }

    public FireEvent(String h3Cell, Double lat, Double lon, Long eventTime, String eventDate,
                     Long declaredAt, String agentId, Double intensity,
                     Double burnedArea, String cause, Boolean declared) {
        this.h3Cell = h3Cell;
        this.lat = lat;
        this.lon = lon;
        this.eventTime = eventTime;
        this.eventDate = eventDate;
        this.declaredAt = declaredAt;
        this.agentId = agentId;
        this.intensity = intensity;
        this.burnedArea = burnedArea;
        this.cause = cause;
        this.declared = declared;
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

    public Long getDeclaredAt() { return declaredAt; }
    public void setDeclaredAt(Long declaredAt) { this.declaredAt = declaredAt; }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public Double getIntensity() { return intensity; }
    public void setIntensity(Double intensity) { this.intensity = intensity; }

    public Double getBurnedArea() { return burnedArea; }
    public void setBurnedArea(Double burnedArea) { this.burnedArea = burnedArea; }

    public String getCause() { return cause; }
    public void setCause(String cause) { this.cause = cause; }

    public Boolean getDeclared() { return declared; }
    public void setDeclared(Boolean declared) { this.declared = declared; }

    @Override
    public String toString() {
        return "FireEvent{" +
                "h3Cell='" + h3Cell + '\'' +
                ", lat=" + lat +
                ", lon=" + lon +
                ", eventTime=" + eventTime +
                ", eventDate='" + eventDate + '\'' +
                ", declaredAt=" + declaredAt +
                ", agentId='" + agentId + '\'' +
                ", intensity=" + intensity +
                ", burnedArea=" + burnedArea +
                ", cause='" + cause + '\'' +
                ", declared=" + declared +
                '}';
    }
}