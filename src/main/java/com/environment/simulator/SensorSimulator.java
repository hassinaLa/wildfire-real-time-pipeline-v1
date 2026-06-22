package com.environment.simulator;

import com.environment.simulator.model.SensorEvent;
import com.environment.simulator.util.EventTimePolicy;
import com.environment.simulator.util.EventTimestamp;
import com.environment.simulator.util.TimeMode;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDate;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class SensorSimulator {

    private static final String TOPIC = "sensor-events";
    private static final Gson GSON = new Gson();
    private static final Random RANDOM = new Random();

    private final KafkaProducer<String, String> producer;
    private final String kafkaBootstrap;
    private final List<AllowedH3Loader.PointLocation> points;
    private final DailyScenarioCoordinator coordinator;

    private final TimeMode timeMode;
    private final EventTimePolicy timePolicy;

    public SensorSimulator(DailyScenarioCoordinator coordinator) throws Exception {
        this.timeMode = EventTimePolicy.readModeFromEnv();
        this.timePolicy = new EventTimePolicy(timeMode);

        this.kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        this.points = AllowedH3Loader.load();
        this.coordinator = coordinator;

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBootstrap);
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
    }

    public void startProducing() throws Exception {
        System.out.println("Starting SensorSimulator on " + kafkaBootstrap);
        System.out.println("Loaded allowed H3 points: " + points.size());
        System.out.println("SensorSimulator TIME_MODE=" + timeMode);

        while (true) {
            DailyScenarioCoordinator.ScenarioContext context =
                    coordinator.getCurrentScenario(DailyScenarioCoordinator.SENSOR);

            LocalDate scenarioDate = context.getScenarioDate();
            DailyScenarioPlan plan = context.getPlan();

            System.out.println("SensorSimulator scenarioDate=" + scenarioDate);

            for (AllowedH3Loader.PointLocation point : points) {
                DailyScenarioCellType type = plan.getType(point.getH3Cell());

                if (type == DailyScenarioCellType.SENSOR_ONLY
                        || type == DailyScenarioCellType.SENSOR_AND_FIRE
                        || type == DailyScenarioCellType.VOLUNTARY) {

                    SensorEvent event = buildConfirmedSensorEvent(point, scenarioDate, type);
                    String json = GSON.toJson(event);
                    producer.send(new ProducerRecord<>(TOPIC, point.getH3Cell(), json));
                    System.out.println("Sensor confirmed sent -> " + json);
                }
            }

            producer.flush();

            coordinator.markCompleted(DailyScenarioCoordinator.SENSOR, scenarioDate);
            coordinator.awaitNextCycle(scenarioDate);
        }
    }

    private SensorEvent buildConfirmedSensorEvent(
            AllowedH3Loader.PointLocation point,
            LocalDate scenarioDate,
            DailyScenarioCellType type
    ) {
        EventTimestamp ts = timePolicy.resolveTimestamp(scenarioDate, RANDOM);

        SensorEvent event = new SensorEvent();
        event.setH3Cell(point.getH3Cell());
        event.setLat(point.getLat());
        event.setLon(point.getLon());
        event.setEventTime(ts.getEventTime());
        event.setEventDate(ts.getEventDate());

        if (type == DailyScenarioCellType.VOLUNTARY) {
            event.setCo2(round(randomBetween(800.0, 1400.0)));
            event.setSmoke(round(randomBetween(75.0, 100.0)));
            event.setSensorTemperature(round(randomBetween(55.0, 90.0)));
            event.setBattery(round(randomBetween(35.0, 95.0)));
            event.setAlertLevel("CRITICAL");
            event.setFireRelated(true);
        } else {
            event.setCo2(round(randomBetween(500.0, 1200.0)));
            event.setSmoke(round(randomBetween(60.0, 100.0)));
            event.setSensorTemperature(round(randomBetween(45.0, 80.0)));
            event.setBattery(round(randomBetween(30.0, 95.0)));
            event.setAlertLevel(randomAlert());
            event.setFireRelated(true);
        }

        return event;
    }

    private String randomAlert() {
        double p = RANDOM.nextDouble();
        if (p < 0.20) return "MEDIUM";
        if (p < 0.75) return "HIGH";
        return "CRITICAL";
    }

    private double randomBetween(double min, double max) {
        return min + (max - min) * RANDOM.nextDouble();
    }

    private double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    public void close() {
        producer.close();
    }
}