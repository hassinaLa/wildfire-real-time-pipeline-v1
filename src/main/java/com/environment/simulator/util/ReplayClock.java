package com.environment.simulator.util;

import java.time.LocalDate;

public class ReplayClock {
    private final LocalDate startDate;
    private final LocalDate endDate;
    private LocalDate currentDate;

    public ReplayClock(LocalDate startDate, LocalDate endDate) {
        if (startDate == null || endDate == null) {
            throw new IllegalArgumentException("startDate and endDate must not be null");
        }
        if (endDate.isBefore(startDate)) {
            throw new IllegalArgumentException("endDate must be >= startDate");
        }

        this.startDate = startDate;
        this.endDate = endDate;
        this.currentDate = startDate;
    }

    public LocalDate currentDate() {
        return currentDate;
    }

    public void advanceDay() {
        currentDate = currentDate.plusDays(1);
        if (currentDate.isAfter(endDate)) {
            currentDate = startDate;
        }
    }

    public void reset() {
        currentDate = startDate;
    }

    @Override
    public String toString() {
        return "ReplayClock{" +
                "startDate=" + startDate +
                ", endDate=" + endDate +
                ", currentDate=" + currentDate +
                '}';
    }
}