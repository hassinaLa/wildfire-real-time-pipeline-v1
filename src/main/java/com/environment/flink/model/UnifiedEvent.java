package com.environment.flink.model;

import java.io.Serializable;

public class UnifiedEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String sourceType;
    private String h3Cell;
    private Long eventTime;
    private String eventDate;
    private Double lat;
    private Double lon;

    // Meteo
    private Double temperature;
    private Double humidity;
    private Double windSpeed;
    private Double precipitation;
    private Double soilMoisture;
    private Double vpd;

    // Sensor
    private Double co2;
    private Double smoke;
    private Double sensorTemperature;
    private Double battery;

    // Fire
    private Integer fireDeclared;
    private Double intensity;
    private Double burnedArea;

    // Static
    private Double altitudeMoy;
    private Double penteMoy;
    private Double distRouteM;
    private Double distUrbainM;
    private Double ndvi30dMean;
    private Double ndvi30dMin;
    private Double ndvi30dStd;
    private Double h3CenterLat;
    private Double h3CenterLon;

    public UnifiedEvent() {
    }

    public String getSourceType() { return sourceType; }
    public void setSourceType(String sourceType) { this.sourceType = sourceType; }

    public String getH3Cell() { return h3Cell; }
    public void setH3Cell(String h3Cell) { this.h3Cell = h3Cell; }

    public Long getEventTime() { return eventTime; }
    public void setEventTime(Long eventTime) { this.eventTime = eventTime; }

    public String getEventDate() { return eventDate; }
    public void setEventDate(String eventDate) { this.eventDate = eventDate; }

    public Double getLat() { return lat; }
    public void setLat(Double lat) { this.lat = lat; }

    public Double getLon() { return lon; }
    public void setLon(Double lon) { this.lon = lon; }

    public Double getTemperature() { return temperature; }
    public void setTemperature(Double temperature) { this.temperature = temperature; }

    public Double getHumidity() { return humidity; }
    public void setHumidity(Double humidity) { this.humidity = humidity; }

    public Double getWindSpeed() { return windSpeed; }
    public void setWindSpeed(Double windSpeed) { this.windSpeed = windSpeed; }

    public Double getPrecipitation() { return precipitation; }
    public void setPrecipitation(Double precipitation) { this.precipitation = precipitation; }

    public Double getSoilMoisture() { return soilMoisture; }
    public void setSoilMoisture(Double soilMoisture) { this.soilMoisture = soilMoisture; }

    public Double getVpd() { return vpd; }
    public void setVpd(Double vpd) { this.vpd = vpd; }

    public Double getCo2() { return co2; }
    public void setCo2(Double co2) { this.co2 = co2; }

    public Double getSmoke() { return smoke; }
    public void setSmoke(Double smoke) { this.smoke = smoke; }

    public Double getSensorTemperature() { return sensorTemperature; }
    public void setSensorTemperature(Double sensorTemperature) { this.sensorTemperature = sensorTemperature; }

    public Double getBattery() { return battery; }
    public void setBattery(Double battery) { this.battery = battery; }

    public Integer getFireDeclared() { return fireDeclared; }
    public void setFireDeclared(Integer fireDeclared) { this.fireDeclared = fireDeclared; }

    public Double getIntensity() { return intensity; }
    public void setIntensity(Double intensity) { this.intensity = intensity; }

    public Double getBurnedArea() { return burnedArea; }
    public void setBurnedArea(Double burnedArea) { this.burnedArea = burnedArea; }

    public Double getAltitudeMoy() { return altitudeMoy; }
    public void setAltitudeMoy(Double altitudeMoy) { this.altitudeMoy = altitudeMoy; }

    public Double getPenteMoy() { return penteMoy; }
    public void setPenteMoy(Double penteMoy) { this.penteMoy = penteMoy; }

    public Double getDistRouteM() { return distRouteM; }
    public void setDistRouteM(Double distRouteM) { this.distRouteM = distRouteM; }

    public Double getDistUrbainM() { return distUrbainM; }
    public void setDistUrbainM(Double distUrbainM) { this.distUrbainM = distUrbainM; }

    public Double getNdvi30dMean() { return ndvi30dMean; }
    public void setNdvi30dMean(Double ndvi30dMean) { this.ndvi30dMean = ndvi30dMean; }

    public Double getNdvi30dMin() { return ndvi30dMin; }
    public void setNdvi30dMin(Double ndvi30dMin) { this.ndvi30dMin = ndvi30dMin; }

    public Double getNdvi30dStd() { return ndvi30dStd; }
    public void setNdvi30dStd(Double ndvi30dStd) { this.ndvi30dStd = ndvi30dStd; }

    public Double getH3CenterLat() { return h3CenterLat; }
    public void setH3CenterLat(Double h3CenterLat) { this.h3CenterLat = h3CenterLat; }

    public Double getH3CenterLon() { return h3CenterLon; }
    public void setH3CenterLon(Double h3CenterLon) { this.h3CenterLon = h3CenterLon; }
}