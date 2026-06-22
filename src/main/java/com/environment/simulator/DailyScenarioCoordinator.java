package com.environment.simulator;

import com.environment.simulator.util.EventTimePolicy;
import com.environment.simulator.util.ReplayClock;
import com.environment.simulator.util.TimeMode;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

public class DailyScenarioCoordinator {

    public static final String METEO = "METEO";
    public static final String SENSOR = "SENSOR";
    public static final String FIRE = "FIRE";

    private static final ZoneId ALGERIA_ZONE = ZoneId.of("Africa/Algiers");

    private final TimeMode timeMode;
    private final EventTimePolicy timePolicy;
    private final ReplayClock replayClock;
    private final DailyScenarioPlanner planner;

    private LocalDate currentDate;
    private DailyScenarioPlan currentPlan;

    private final Set<String> completedRoles = new HashSet<>();

    public DailyScenarioCoordinator(DailyScenarioPlanner planner) throws Exception {
        this.timeMode = EventTimePolicy.readModeFromEnv();
        this.timePolicy = new EventTimePolicy(timeMode);
        this.replayClock = timePolicy.isHistorical2025() ? timePolicy.newReplayClock() : null;
        this.planner = planner;

        initializeCurrentScenario();
    }

    private synchronized void initializeCurrentScenario() {
        if (timePolicy.isHistorical2025()) {
            this.currentDate = replayClock.currentDate();
        } else {
            this.currentDate = LocalDate.now(ALGERIA_ZONE);
        }

        this.currentPlan = planner.getOrCreatePlan(currentDate);
        this.completedRoles.clear();

        System.out.println("DailyScenarioCoordinator initialized -> mode=" + timeMode
                + ", currentDate=" + currentDate
                + ", plan=" + currentPlan);
    }

    public synchronized ScenarioContext getCurrentScenario(String role) {
        refreshLiveDayIfNeeded();
        return new ScenarioContext(currentDate, currentPlan);
    }

    public synchronized void markCompleted(String role, LocalDate processedDate) {
        refreshLiveDayIfNeeded();

        if (processedDate == null || currentDate == null) {
            return;
        }

        if (!processedDate.equals(currentDate)) {
            System.out.println("Coordinator ignoring markCompleted from role=" + role
                    + " because processedDate=" + processedDate
                    + " != currentDate=" + currentDate);
            return;
        }

        completedRoles.add(role);
        System.out.println("Coordinator markCompleted -> role=" + role
                + ", date=" + processedDate
                + ", completedRoles=" + completedRoles);

        if (allRolesCompleted()) {
            if (timePolicy.isHistorical2025()) {
                replayClock.advanceDay();
                currentDate = replayClock.currentDate();
                currentPlan = planner.getOrCreatePlan(currentDate);
                completedRoles.clear();

                System.out.println("Coordinator advanced historical day -> " + currentDate
                        + ", plan=" + currentPlan);

                notifyAll();
            } else {
                // في live mode ما نزيدوش اليوم يدويًا
                // نستناو حتى يتبدل اليوم الحقيقي
                System.out.println("Coordinator completed all live roles for date=" + currentDate
                        + ". Waiting for real next day...");
                notifyAll();
            }
        }
    }

    public synchronized void awaitNextCycle(LocalDate processedDate) throws InterruptedException {
        if (processedDate == null) {
            return;
        }

        if (timePolicy.isHistorical2025()) {
            while (processedDate.equals(currentDate)) {
                wait(1000L);
            }
            return;
        }

        // LIVE MODE:
        // نبقى نستنى حتى يتبدل اليوم الحقيقي في timezone الجزائر
        while (true) {
            refreshLiveDayIfNeeded();
            if (!processedDate.equals(currentDate)) {
                return;
            }
            wait(30_000L);
        }
    }

    private boolean allRolesCompleted() {
        return completedRoles.contains(METEO)
                && completedRoles.contains(SENSOR)
                && completedRoles.contains(FIRE);
    }

    private void refreshLiveDayIfNeeded() {
        if (!timePolicy.isLive()) {
            return;
        }

        LocalDate today = LocalDate.now(ALGERIA_ZONE);
        if (!today.equals(currentDate)) {
            currentDate = today;
            currentPlan = planner.getOrCreatePlan(today);
            completedRoles.clear();

            System.out.println("Coordinator detected new live day -> " + currentDate
                    + ", plan=" + currentPlan);

            notifyAll();
        }
    }

    public boolean isLive() {
        return timePolicy.isLive();
    }

    public boolean isHistorical2025() {
        return timePolicy.isHistorical2025();
    }

    public static class ScenarioContext {
        private final LocalDate scenarioDate;
        private final DailyScenarioPlan plan;

        public ScenarioContext(LocalDate scenarioDate, DailyScenarioPlan plan) {
            this.scenarioDate = scenarioDate;
            this.plan = plan;
        }

        public LocalDate getScenarioDate() {
            return scenarioDate;
        }

        public DailyScenarioPlan getPlan() {
            return plan;
        }
    }
}