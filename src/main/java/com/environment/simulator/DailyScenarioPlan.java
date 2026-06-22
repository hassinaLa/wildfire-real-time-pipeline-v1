package com.environment.simulator;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DailyScenarioPlan implements Serializable {

    private final LocalDate scenarioDate;
    private final Map<String, DailyScenarioCellType> planByH3;

    public DailyScenarioPlan(LocalDate scenarioDate, Map<String, DailyScenarioCellType> planByH3) {
        this.scenarioDate = scenarioDate;
        this.planByH3 = new HashMap<>(planByH3);
    }

    public LocalDate getScenarioDate() {
        return scenarioDate;
    }

    public DailyScenarioCellType getType(String h3Cell) {
        return planByH3.getOrDefault(h3Cell, DailyScenarioCellType.NORMAL);
    }

    public Map<String, DailyScenarioCellType> getPlanByH3() {
        return Collections.unmodifiableMap(planByH3);
    }

    public long countByType(DailyScenarioCellType type) {
        return planByH3.values().stream().filter(t -> t == type).count();
    }

    @Override
    public String toString() {
        return "DailyScenarioPlan{" +
                "scenarioDate=" + scenarioDate +
                ", totalCells=" + planByH3.size() +
                ", sensorOnly=" + countByType(DailyScenarioCellType.SENSOR_ONLY) +
                ", sensorAndFire=" + countByType(DailyScenarioCellType.SENSOR_AND_FIRE) +
                ", fireOnly=" + countByType(DailyScenarioCellType.FIRE_ONLY) +
                ", voluntary=" + countByType(DailyScenarioCellType.VOLUNTARY) +
                '}';
    }
}