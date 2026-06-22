package com.environment.simulator.util;

public enum TimeMode {
    LIVE,
    HISTORICAL_2025;

    public static TimeMode fromString(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return HISTORICAL_2025;
        }

        String normalized = raw.trim().toUpperCase();
        for (TimeMode mode : values()) {
            if (mode.name().equals(normalized)) {
                return mode;
            }
        }

        throw new IllegalArgumentException(
                "Invalid TIME_MODE='" + raw + "'. Allowed values: LIVE, HISTORICAL_2025"
        );
    }
}