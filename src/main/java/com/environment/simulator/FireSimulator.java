package com.environment.simulator;

import com.environment.simulator.model.FireEvent;
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

public class FireSimulator {

    private static final String TOPIC = "fire-events";
    private static final Gson GSON = new Gson();
    private static final Random RANDOM = new Random();

    private static final List<String> CAUSES = List.of(
            "UNKNOWN", "HUMAN", "LIGHTNING", "AGRICULTURE", "INFRASTRUCTURE"
    );

    private final KafkaProducer<String, String> producer;
    private final String kafkaBootstrap;
    private final List<AllowedH3Loader.PointLocation> points;
    private final DailyScenarioCoordinator coordinator;

    private final TimeMode timeMode;
    private final EventTimePolicy timePolicy;

    public FireSimulator(DailyScenarioCoordinator coordinator) throws Exception {
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
        System.out.println("Starting FireSimulator on " + kafkaBootstrap);
        System.out.println("Loaded allowed H3 points: " + points.size());
        System.out.println("FireSimulator TIME_MODE=" + timeMode);

        while (true) {
            DailyScenarioCoordinator.ScenarioContext context =
                    coordinator.getCurrentScenario(DailyScenarioCoordinator.FIRE);

            LocalDate scenarioDate = context.getScenarioDate();
            DailyScenarioPlan plan = context.getPlan();

            System.out.println("FireSimulator scenarioDate=" + scenarioDate);

            for (AllowedH3Loader.PointLocation point : points) {
                DailyScenarioCellType type = plan.getType(point.getH3Cell());

                if (type == DailyScenarioCellType.SENSOR_AND_FIRE
                        || type == DailyScenarioCellType.FIRE_ONLY
                        || type == DailyScenarioCellType.VOLUNTARY) {

                    FireEvent event = buildFireEvent(point, scenarioDate, type);
                    String json = GSON.toJson(event);
                    producer.send(new ProducerRecord<>(TOPIC, point.getH3Cell(), json));
                    System.out.println("Fire sent -> " + json);
                }
            }

            producer.flush();

            coordinator.markCompleted(DailyScenarioCoordinator.FIRE, scenarioDate);
            coordinator.awaitNextCycle(scenarioDate);
        }
    }

    private FireEvent buildFireEvent(
            AllowedH3Loader.PointLocation point,
            LocalDate scenarioDate,
            DailyScenarioCellType type
    ) {
        EventTimestamp ts = timePolicy.resolveTimestamp(scenarioDate, RANDOM);

        boolean declared = true;
        long declaredAt = ts.getEventTime() + (5 + RANDOM.nextInt(60)) * 60_000L;

        FireEvent event = new FireEvent();
        event.setH3Cell(point.getH3Cell());
        event.setLat(point.getLat());
        event.setLon(point.getLon());
        event.setEventTime(ts.getEventTime());
        event.setEventDate(ts.getEventDate());

        event.setDeclaredAt(declaredAt);
        event.setAgentId("AG_" + (100 + RANDOM.nextInt(900)));
        event.setIntensity(round(randomBetween(20.0, 100.0)));
        event.setBurnedArea(round(randomBetween(0.2, 15.0)));

        if (type == DailyScenarioCellType.VOLUNTARY) {
            event.setCause("HUMAN");
        } else {
            event.setCause(CAUSES.get(RANDOM.nextInt(CAUSES.size())));
        }

        event.setDeclared(declared);
        return event;
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