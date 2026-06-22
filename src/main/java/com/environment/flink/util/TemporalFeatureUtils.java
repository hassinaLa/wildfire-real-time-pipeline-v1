package com.environment.flink.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

public final class TemporalFeatureUtils {

    private TemporalFeatureUtils() {
    }

    public static LocalDate toLocalDate(Long eventTimeMillis) {
        return Instant.ofEpochMilli(eventTimeMillis).atZone(ZoneOffset.UTC).toLocalDate();
    }

    public static int getMonth(LocalDate date) {
        return date.getMonthValue();
    }

    public static int getDayOfYear(LocalDate date) {
        return date.getDayOfYear();
    }

    public static double monthSin(int month) {
        return Math.sin(2.0 * Math.PI * month / 12.0);
    }

    public static double monthCos(int month) {
        return Math.cos(2.0 * Math.PI * month / 12.0);
    }

    public static double doySin(int doy) {
        return Math.sin(2.0 * Math.PI * doy / 365.0);
    }

    public static double doyCos(int doy) {
        return Math.cos(2.0 * Math.PI * doy / 365.0);
    }

    public static int isWeekend(LocalDate date) {
        switch (date.getDayOfWeek()) {
            case SATURDAY:
            case SUNDAY:
                return 1;
            default:
                return 0;
        }
    }
}
