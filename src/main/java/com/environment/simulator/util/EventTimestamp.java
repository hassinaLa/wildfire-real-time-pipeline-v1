package com.environment.simulator.util;

public class EventTimestamp {
    private final long eventTime;
    private final String eventDate;

    public EventTimestamp(long eventTime, String eventDate) {
        this.eventTime = eventTime;
        this.eventDate = eventDate;
    }

    public long getEventTime() {
        return eventTime;
    }

    public String getEventDate() {
        return eventDate;
    }

    @Override
    public String toString() {
        return "EventTimestamp{" +
                "eventTime=" + eventTime +
                ", eventDate='" + eventDate + '\'' +
                '}';
    }
}