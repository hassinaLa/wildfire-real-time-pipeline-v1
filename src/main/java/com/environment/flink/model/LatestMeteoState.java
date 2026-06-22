package com.environment.flink.model;

import java.io.Serializable;

public class LatestMeteoState implements Serializable {
    public Long eventTime;
    public Double temperature;
    public Double humidity;
    public Double windSpeed;
    public Double precipitation;
    public Double soilMoisture;
    public Double vpd;

    public LatestMeteoState() {
    }

    public static LatestMeteoState fromValues(
            Long eventTime,
            Double temperature,
            Double humidity,
            Double windSpeed,
            Double precipitation,
            Double soilMoisture,
            Double vpd
    ) {
        LatestMeteoState s = new LatestMeteoState();
        s.eventTime = eventTime;
        s.temperature = temperature;
        s.humidity = humidity;
        s.windSpeed = windSpeed;
        s.precipitation = precipitation;
        s.soilMoisture = soilMoisture;
        s.vpd = vpd;
        return s;
    }
}
