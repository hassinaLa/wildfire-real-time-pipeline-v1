package com.environment.simulator.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Random;

public class EventTimePolicy {

    public static final ZoneId DEFAULT_ZONE = ZoneId.of("Africa/Algiers");
    public static final LocalDate HISTORICAL_START = LocalDate.of(2025, 5, 1);
    public static final LocalDate HISTORICAL_END = LocalDate.of(2025, 10, 31);

    private final TimeMode mode;
    private final ZoneId zone;

    public EventTimePolicy(TimeMode mode) {
        this(mode, DEFAULT_ZONE);
    }

    public EventTimePolicy(TimeMode mode, ZoneId zone) {
        if (mode == null) {
            throw new IllegalArgumentException("mode must not be null");
        }
        if (zone == null) {
            throw new IllegalArgumentException("zone must not be null");
        }

        this.mode = mode;
        this.zone = zone;
    }

    public static TimeMode readModeFromEnv() {
        String raw = System.getenv().getOrDefault("TIME_MODE", "HISTORICAL_2025");
        return TimeMode.fromString(raw);
    }

    public TimeMode getMode() {
        return mode;
    }

    public ZoneId getZone() {
        return zone;
    }

    public boolean isLive() {
        return mode == TimeMode.LIVE;
    }

    public boolean isHistorical2025() {
        return mode == TimeMode.HISTORICAL_2025;
    }

    public ReplayClock newReplayClock() {
        return new ReplayClock(HISTORICAL_START, HISTORICAL_END);
    }

    public EventTimestamp nextLiveTimestamp() {
        long now = System.currentTimeMillis();
        String date = Instant.ofEpochMilli(now).atZone(zone).toLocalDate().toString();
        return new EventTimestamp(now, date);
    }

    public EventTimestamp timestampForReplayDate(LocalDate replayDate, Random random) {
        if (replayDate == null) {
            throw new IllegalArgumentException("replayDate must not be null");
        }
        if (random == null) {
            throw new IllegalArgumentException("random must not be null");
        }

        long dayStart = replayDate.atStartOfDay(zone).toInstant().toEpochMilli();
        int maxOffsetExclusive = 24 * 60 * 60 * 1000; // milliseconds in one day
        long offset = random.nextInt(maxOffsetExclusive);

        return new EventTimestamp(dayStart + offset, replayDate.toString());
    }

    public EventTimestamp timestampForReplayDateStart(LocalDate replayDate) {
        if (replayDate == null) {
            throw new IllegalArgumentException("replayDate must not be null");
        }

        long dayStart = replayDate.atStartOfDay(zone).toInstant().toEpochMilli();
        return new EventTimestamp(dayStart, replayDate.toString());
    }

    public EventTimestamp resolveTimestamp(LocalDate replayDate, Random random) {
        if (isLive()) {
            return nextLiveTimestamp();
        }
        return timestampForReplayDate(replayDate, random);
    }

    public EventTimestamp resolveTimestampAtDayStart(LocalDate replayDate) {
        if (isLive()) {
            return nextLiveTimestamp();
        }
        return timestampForReplayDateStart(replayDate);
    }
}