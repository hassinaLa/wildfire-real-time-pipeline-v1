package com.environment.simulator;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DailyScenarioPlanner {

    private final List<AllowedH3Loader.PointLocation> points;
    private final Random random = new Random();

    private DailyScenarioPlan currentPlan;

    public DailyScenarioPlanner(List<AllowedH3Loader.PointLocation> points) {
        this.points = new ArrayList<>(points);
    }

    public synchronized DailyScenarioPlan getOrCreatePlan(LocalDate date) {
        if (currentPlan == null || !currentPlan.getScenarioDate().equals(date)) {
            currentPlan = generatePlan(date);
            System.out.println("Generated new daily scenario plan -> " + currentPlan);
        }
        return currentPlan;
    }

    private DailyScenarioPlan generatePlan(LocalDate date) {
        Map<String, DailyScenarioCellType> plan = new HashMap<>();

        for (AllowedH3Loader.PointLocation point : points) {
            plan.put(point.getH3Cell(), DailyScenarioCellType.NORMAL);
        }

        List<AllowedH3Loader.PointLocation> shuffled = new ArrayList<>(points);
        Collections.shuffle(shuffled, random);

        int total = shuffled.size();
        int positiveCount = Math.max(1, (int) Math.ceil(total * 0.05)); // 5%

        List<AllowedH3Loader.PointLocation> selected = shuffled.subList(0, Math.min(positiveCount, shuffled.size()));

        int sensorOnlyCount = Math.max(1, (int) Math.round(positiveCount * 0.50));
        int sensorAndFireCount = Math.max(1, (int) Math.round(positiveCount * 0.30));
        int fireOnlyCount = Math.max(1, (int) Math.round(positiveCount * 0.10));
        int voluntaryCount = Math.max(1, positiveCount - sensorOnlyCount - sensorAndFireCount - fireOnlyCount);

        int index = 0;

        for (int i = 0; i < sensorOnlyCount && index < selected.size(); i++, index++) {
            plan.put(selected.get(index).getH3Cell(), DailyScenarioCellType.SENSOR_ONLY);
        }

        for (int i = 0; i < sensorAndFireCount && index < selected.size(); i++, index++) {
            plan.put(selected.get(index).getH3Cell(), DailyScenarioCellType.SENSOR_AND_FIRE);
        }

        for (int i = 0; i < fireOnlyCount && index < selected.size(); i++, index++) {
            plan.put(selected.get(index).getH3Cell(), DailyScenarioCellType.FIRE_ONLY);
        }

        for (int i = 0; i < voluntaryCount && index < selected.size(); i++, index++) {
            plan.put(selected.get(index).getH3Cell(), DailyScenarioCellType.VOLUNTARY);
        }

        return new DailyScenarioPlan(date, plan);
    }
}