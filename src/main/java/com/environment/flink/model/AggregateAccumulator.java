package com.environment.flink.model;

import java.io.Serializable;

public class AggregateAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    private String h3Cell;
    private String eventDate;

    private Long windowStart;
    private Long windowEnd;

    // Fire
    private int fireCount;
    private int fireDeclaredCount;
    private double sumFireIntensity;
    private int countFireIntensity;
    private Double maxFireIntensity;
    private double totalBurnedArea;

    // Meteo
    private int meteoCount;
    private double sumTemp;
    private int countTemp;
    private Double minHumidity;
    private Double maxWindSpeed;
    private double sumVpd;
    private int countVpd;
    private double sumSoilMoisture;
    private int countSoilMoisture;
    private double totalPrecipitation1d;
    private double totalPrecipitation7d;

    // Sensor
    private int sensorCount;
    private double sumCo2;
    private int countCo2;
    private Double maxCo2;
    private double sumSmoke;
    private int countSmoke;
    private Double maxSmoke;
    private double sumSensorTemp;
    private int countSensorTemp;
    private Double maxSensorTemp;
    private Double minBattery;

    // Location
    private double sumLat;
    private int countLat;
    private double sumLon;
    private int countLon;

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

    public AggregateAccumulator() {
    }

    public void add(UnifiedEvent e) {
        if (e == null) return;

        this.h3Cell = e.getH3Cell();
        this.eventDate = e.getEventDate();

        if (e.getEventTime() != null) {
            if (windowStart == null || e.getEventTime() < windowStart) {
                windowStart = e.getEventTime();
            }
            if (windowEnd == null || e.getEventTime() > windowEnd) {
                windowEnd = e.getEventTime();
            }
        }

        if (e.getLat() != null) {
            sumLat += e.getLat();
            countLat++;
        }
        if (e.getLon() != null) {
            sumLon += e.getLon();
            countLon++;
        }

        if (e.getAltitudeMoy() != null) altitudeMoy = e.getAltitudeMoy();
        if (e.getPenteMoy() != null) penteMoy = e.getPenteMoy();
        if (e.getDistRouteM() != null) distRouteM = e.getDistRouteM();
        if (e.getDistUrbainM() != null) distUrbainM = e.getDistUrbainM();
        if (e.getNdvi30dMean() != null) ndvi30dMean = e.getNdvi30dMean();
        if (e.getNdvi30dMin() != null) ndvi30dMin = e.getNdvi30dMin();
        if (e.getNdvi30dStd() != null) ndvi30dStd = e.getNdvi30dStd();
        if (e.getH3CenterLat() != null) h3CenterLat = e.getH3CenterLat();
        if (e.getH3CenterLon() != null) h3CenterLon = e.getH3CenterLon();

        if ("fire".equalsIgnoreCase(e.getSourceType())) {
            fireCount++;

            if (e.getFireDeclared() != null && e.getFireDeclared() > 0) {
                fireDeclaredCount += e.getFireDeclared();
            }

            if (e.getIntensity() != null) {
                sumFireIntensity += e.getIntensity();
                countFireIntensity++;
                if (maxFireIntensity == null || e.getIntensity() > maxFireIntensity) {
                    maxFireIntensity = e.getIntensity();
                }
            }

            if (e.getBurnedArea() != null) {
                totalBurnedArea += e.getBurnedArea();
            }
        }

        if ("meteo".equalsIgnoreCase(e.getSourceType())) {
            meteoCount++;

            if (e.getTemperature() != null) {
                sumTemp += e.getTemperature();
                countTemp++;
            }

            if (e.getHumidity() != null) {
                if (minHumidity == null || e.getHumidity() < minHumidity) {
                    minHumidity = e.getHumidity();
                }
            }

            if (e.getWindSpeed() != null) {
                if (maxWindSpeed == null || e.getWindSpeed() > maxWindSpeed) {
                    maxWindSpeed = e.getWindSpeed();
                }
            }

            if (e.getVpd() != null) {
                sumVpd += e.getVpd();
                countVpd++;
            }

            if (e.getSoilMoisture() != null) {
                sumSoilMoisture += e.getSoilMoisture();
                countSoilMoisture++;
            }

            if (e.getPrecipitation() != null) {
                totalPrecipitation1d += e.getPrecipitation();
                totalPrecipitation7d += e.getPrecipitation();
            }
        }

        if ("sensor".equalsIgnoreCase(e.getSourceType())) {
            sensorCount++;

            if (e.getCo2() != null) {
                sumCo2 += e.getCo2();
                countCo2++;
                if (maxCo2 == null || e.getCo2() > maxCo2) {
                    maxCo2 = e.getCo2();
                }
            }

            if (e.getSmoke() != null) {
                sumSmoke += e.getSmoke();
                countSmoke++;
                if (maxSmoke == null || e.getSmoke() > maxSmoke) {
                    maxSmoke = e.getSmoke();
                }
            }

            if (e.getSensorTemperature() != null) {
                sumSensorTemp += e.getSensorTemperature();
                countSensorTemp++;
                if (maxSensorTemp == null || e.getSensorTemperature() > maxSensorTemp) {
                    maxSensorTemp = e.getSensorTemperature();
                }
            }

            if (e.getBattery() != null) {
                if (minBattery == null || e.getBattery() < minBattery) {
                    minBattery = e.getBattery();
                }
            }
        }
    }

    public void merge(AggregateAccumulator other) {
        if (other == null) return;

        if (this.h3Cell == null) this.h3Cell = other.h3Cell;
        if (this.eventDate == null) this.eventDate = other.eventDate;

        if (other.windowStart != null && (this.windowStart == null || other.windowStart < this.windowStart)) {
            this.windowStart = other.windowStart;
        }
        if (other.windowEnd != null && (this.windowEnd == null || other.windowEnd > this.windowEnd)) {
            this.windowEnd = other.windowEnd;
        }

        this.fireCount += other.fireCount;
        this.fireDeclaredCount += other.fireDeclaredCount;
        this.sumFireIntensity += other.sumFireIntensity;
        this.countFireIntensity += other.countFireIntensity;
        if (this.maxFireIntensity == null || (other.maxFireIntensity != null && other.maxFireIntensity > this.maxFireIntensity)) {
            this.maxFireIntensity = other.maxFireIntensity;
        }
        this.totalBurnedArea += other.totalBurnedArea;

        this.meteoCount += other.meteoCount;
        this.sumTemp += other.sumTemp;
        this.countTemp += other.countTemp;
        if (this.minHumidity == null || (other.minHumidity != null && other.minHumidity < this.minHumidity)) {
            this.minHumidity = other.minHumidity;
        }
        if (this.maxWindSpeed == null || (other.maxWindSpeed != null && other.maxWindSpeed > this.maxWindSpeed)) {
            this.maxWindSpeed = other.maxWindSpeed;
        }
        this.sumVpd += other.sumVpd;
        this.countVpd += other.countVpd;
        this.sumSoilMoisture += other.sumSoilMoisture;
        this.countSoilMoisture += other.countSoilMoisture;
        this.totalPrecipitation1d += other.totalPrecipitation1d;
        this.totalPrecipitation7d += other.totalPrecipitation7d;

        this.sensorCount += other.sensorCount;
        this.sumCo2 += other.sumCo2;
        this.countCo2 += other.countCo2;
        if (this.maxCo2 == null || (other.maxCo2 != null && other.maxCo2 > this.maxCo2)) {
            this.maxCo2 = other.maxCo2;
        }
        this.sumSmoke += other.sumSmoke;
        this.countSmoke += other.countSmoke;
        if (this.maxSmoke == null || (other.maxSmoke != null && other.maxSmoke > this.maxSmoke)) {
            this.maxSmoke = other.maxSmoke;
        }
        this.sumSensorTemp += other.sumSensorTemp;
        this.countSensorTemp += other.countSensorTemp;
        if (this.maxSensorTemp == null || (other.maxSensorTemp != null && other.maxSensorTemp > this.maxSensorTemp)) {
            this.maxSensorTemp = other.maxSensorTemp;
        }
        if (this.minBattery == null || (other.minBattery != null && other.minBattery < this.minBattery)) {
            this.minBattery = other.minBattery;
        }

        this.sumLat += other.sumLat;
        this.countLat += other.countLat;
        this.sumLon += other.sumLon;
        this.countLon += other.countLon;

        if (this.altitudeMoy == null) this.altitudeMoy = other.altitudeMoy;
        if (this.penteMoy == null) this.penteMoy = other.penteMoy;
        if (this.distRouteM == null) this.distRouteM = other.distRouteM;
        if (this.distUrbainM == null) this.distUrbainM = other.distUrbainM;
        if (this.ndvi30dMean == null) this.ndvi30dMean = other.ndvi30dMean;
        if (this.ndvi30dMin == null) this.ndvi30dMin = other.ndvi30dMin;
        if (this.ndvi30dStd == null) this.ndvi30dStd = other.ndvi30dStd;
        if (this.h3CenterLat == null) this.h3CenterLat = other.h3CenterLat;
        if (this.h3CenterLon == null) this.h3CenterLon = other.h3CenterLon;
    }

    public String getH3Cell() { return h3Cell; }
    public String getEventDate() { return eventDate; }
    public Long getWindowStart() { return windowStart; }
    public Long getWindowEnd() { return windowEnd; }

    public void setEventDate(String eventDate) { this.eventDate = eventDate; }
    public void setWindowStart(Long windowStart) { this.windowStart = windowStart; }
    public void setWindowEnd(Long windowEnd) { this.windowEnd = windowEnd; }

    public Integer getFireCount() { return fireCount; }
    public Integer getFireDeclaredCount() { return fireDeclaredCount; }
    public Double getAvgFireIntensity() { return countFireIntensity > 0 ? sumFireIntensity / countFireIntensity : null; }
    public Double getMaxFireIntensity() { return maxFireIntensity; }
    public Double getTotalBurnedArea() { return totalBurnedArea; }

    public Integer getMeteoCount() { return meteoCount; }
    public Double getAvgTemp() { return countTemp > 0 ? sumTemp / countTemp : null; }
    public Double getMinHumidity() { return minHumidity; }
    public Double getMaxWindSpeed() { return maxWindSpeed; }
    public Double getAvgVpd() { return countVpd > 0 ? sumVpd / countVpd : null; }
    public Double getAvgSoilMoisture() { return countSoilMoisture > 0 ? sumSoilMoisture / countSoilMoisture : null; }
    public Double getTotalPrecipitation1d() { return totalPrecipitation1d; }
    public Double getTotalPrecipitation7d() { return totalPrecipitation7d; }

    public Integer getSensorCount() { return sensorCount; }
    public Double getAvgCo2() { return countCo2 > 0 ? sumCo2 / countCo2 : null; }
    public Double getMaxCo2() { return maxCo2; }
    public Double getAvgSmoke() { return countSmoke > 0 ? sumSmoke / countSmoke : null; }
    public Double getMaxSmoke() { return maxSmoke; }
    public Double getAvgSensorTemp() { return countSensorTemp > 0 ? sumSensorTemp / countSensorTemp : null; }
    public Double getMaxSensorTemp() { return maxSensorTemp; }
    public Double getMinBattery() { return minBattery; }

    public Double getAvgLat() { return countLat > 0 ? sumLat / countLat : null; }
    public Double getAvgLon() { return countLon > 0 ? sumLon / countLon : null; }

    public Double getAltitudeMoy() { return altitudeMoy; }
    public Double getPenteMoy() { return penteMoy; }
    public Double getDistRouteM() { return distRouteM; }
    public Double getDistUrbainM() { return distUrbainM; }
    public Double getNdvi30dMean() { return ndvi30dMean; }
    public Double getNdvi30dMin() { return ndvi30dMin; }
    public Double getNdvi30dStd() { return ndvi30dStd; }
    public Double getH3CenterLat() { return h3CenterLat; }
    public Double getH3CenterLon() { return h3CenterLon; }
}