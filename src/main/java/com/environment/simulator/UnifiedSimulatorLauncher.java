package com.environment.simulator;

import com.environment.simulator.util.EventTimePolicy;
import com.environment.simulator.util.TimeMode;

import java.util.List;

public class UnifiedSimulatorLauncher {

    public static void main(String[] args) {
        try {
            TimeMode mode = EventTimePolicy.readModeFromEnv();
            System.out.println("UnifiedSimulatorLauncher starting with TIME_MODE=" + mode);

            List<AllowedH3Loader.PointLocation> points = AllowedH3Loader.load();
            
            DailyScenarioPlanner planner = new DailyScenarioPlanner(points);
 
            DailyScenarioCoordinator coordinator = new DailyScenarioCoordinator(planner);

            new Thread(() -> {
                try {
                    MeteoApiProducer meteoApiProducer = new MeteoApiProducer();
                    meteoApiProducer.startProducing();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, "MeteoApiThread").start();

            new Thread(() -> {
                try {
                    SensorSimulator sensorSimulator = new SensorSimulator(coordinator);
                    sensorSimulator.startProducing();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, "SensorThread").start();

            new Thread(() -> {
                try {
                    FireSimulator fireSimulator = new FireSimulator(coordinator);
                    fireSimulator.startProducing();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, "FireThread").start();

            System.out.println("All simulators started: Meteo + Sensor + Fire with shared daily scenario plan");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}