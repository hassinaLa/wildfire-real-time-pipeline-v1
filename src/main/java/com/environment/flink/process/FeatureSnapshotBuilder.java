package com.environment.flink.process;

import com.environment.flink.model.AggregateAccumulator;
import com.environment.flink.model.FeatureSnapshot;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

public final class FeatureSnapshotBuilder {

    private static final ZoneId ZONE = ZoneId.of("UTC");

    private FeatureSnapshotBuilder() {
    }

    public static FeatureSnapshot build(
            String h3Cell,
            AggregateAccumulator acc,
            long windowStart,
            long windowEnd
    ) {
        FeatureSnapshot s = new FeatureSnapshot();

        LocalDate date = Instant.ofEpochMilli(windowEnd).atZone(ZONE).toLocalDate();

        s.h3Cell = h3Cell;
        s.h3Commune = h3Cell;
        s.eventTime = windowEnd;
        s.eventDate = date.toString();

        s.h3CenterLat = acc.getH3CenterLat();
        s.h3CenterLon = acc.getH3CenterLon();
        s.altitudeMoy = acc.getAltitudeMoy();
        s.penteMoy = acc.getPenteMoy();
        s.distRouteM = acc.getDistRouteM();
        s.distUrbainM = acc.getDistUrbainM();

        s.humidityMin = acc.getMinHumidity();
        s.temperatureMoy = acc.getAvgTemp();
        s.windSpeedMax = acc.getMaxWindSpeed();
        s.precipitation1dMm = acc.getTotalPrecipitation1d();
        s.precipitation7dSumMm = acc.getTotalPrecipitation7d();
        s.soilMoisture = acc.getAvgSoilMoisture();
        s.vpdKpa = acc.getAvgVpd();

        s.ndvi30dMean = acc.getNdvi30dMean();
        s.ndvi30dMin = acc.getNdvi30dMin();
        s.ndvi30dStd = acc.getNdvi30dStd();

        int month = date.getMonthValue();
        int dayOfYear = date.getDayOfYear();

        s.month = month;
        s.monthSin = Math.sin(2.0 * Math.PI * month / 12.0);
        s.monthCos = Math.cos(2.0 * Math.PI * month / 12.0);
        s.doySin = Math.sin(2.0 * Math.PI * dayOfYear / 365.0);
        s.doyCos = Math.cos(2.0 * Math.PI * dayOfYear / 365.0);
        s.isWeekend = isWeekend(date) ? 1 : 0;

        return s;
    }

    private static boolean isWeekend(LocalDate date) {
        switch (date.getDayOfWeek()) {
            case SATURDAY:
            case SUNDAY:
                return true;
            default:
                return false;
        }
    }
}