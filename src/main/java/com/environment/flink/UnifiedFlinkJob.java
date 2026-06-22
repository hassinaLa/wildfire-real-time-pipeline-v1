package com.environment.flink;

import com.environment.flink.model.DailyH3Outcome;
import com.environment.flink.model.DailyH3TrainingRow;
import com.environment.flink.model.FeatureSnapshot;
import com.environment.flink.model.PredictionResult;
import com.environment.flink.model.SnapshotPredictionRecord;
import com.environment.flink.model.UnifiedEvent;
import com.environment.flink.process.FireAndSensorConfirmationProcessFunction;
import com.environment.flink.process.SnapshotPredictionProcessFunction;
import com.environment.simulator.model.FireEvent;
import com.environment.simulator.model.MeteoEvent;
import com.environment.simulator.model.SensorEvent;
import com.google.gson.Gson;
import com.uber.h3core.H3Core;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class UnifiedFlinkJob {

    private static final int H3_RESOLUTION = 8;

    public static void main(String[] args) throws Exception {
        String timeMode = System.getenv().getOrDefault("TIME_MODE", "HISTORICAL_2025");
        System.out.println("UnifiedFlinkJob running with TIME_MODE=" + timeMode);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String postgresUrl = System.getenv().getOrDefault("POSTGRES_URL", "jdbc:postgresql://postgres:5432/meteo_db");
        String postgresUser = System.getenv().getOrDefault("POSTGRES_USER", "flinkuser");
        String postgresPassword = System.getenv().getOrDefault("POSTGRES_PASSWORD", "flinkpass");
        String checkpointPath = System.getenv().getOrDefault("FLINK_CHECKPOINT_PATH", "file:///opt/flink/checkpoints");

        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage(checkpointPath)
        );

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        env.getConfig().setLatencyTrackingInterval(2000);

        Map<String, StaticNumericData> staticNumericMap =
                loadStaticNumeric(postgresUrl, postgresUser, postgresPassword);

        KafkaSource<String> meteoSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("meteo-events")
                .setGroupId("unified-flink-meteo-direct")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> sensorSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("sensor-events")
                .setGroupId("unified-flink-sensor")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> fireSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("fire-events")
                .setGroupId("unified-flink-fire")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> meteoRaw = env
                .fromSource(meteoSource, WatermarkStrategy.noWatermarks(), "meteo-source-raw");

        SingleOutputStreamOperator<FeatureSnapshot> directSnapshots = meteoRaw
                .map(new DirectMeteoToSnapshotMapper(staticNumericMap))
                .filter(new ValidFeatureSnapshotFilter())
                .name("direct-meteo-to-feature-snapshot");

        SingleOutputStreamOperator<SnapshotPredictionRecord> snapshotPredictionRecords = directSnapshots
                .keyBy(snapshot -> snapshot.h3Cell)
                .process(new SnapshotPredictionProcessFunction())
                .name("snapshot-prediction-records");

        DataStream<UnifiedEvent> sensorUnified = env
                .fromSource(sensorSource, WatermarkStrategy.noWatermarks(), "sensor-source")
                .map(new SensorUnifiedMapper(staticNumericMap))
                .filter(new ValidUnifiedEventFilter())
                .name("sensor-unified");

        DataStream<UnifiedEvent> fireUnified = env
                .fromSource(fireSource, WatermarkStrategy.noWatermarks(), "fire-source")
                .map(new FireUnifiedMapper(staticNumericMap))
                .filter(new ValidUnifiedEventFilter())
                .name("fire-unified");

        DataStream<UnifiedEvent> controlEvents = sensorUnified.union(fireUnified);

        ConnectedStreams<SnapshotPredictionRecord, UnifiedEvent> connected =
                snapshotPredictionRecords.keyBy(r -> r.snapshot.h3Cell)
                        .connect(controlEvents.keyBy(UnifiedEvent::getH3Cell));

        SingleOutputStreamOperator<PredictionResult> finalPredictions = connected
                .process(new FireAndSensorConfirmationProcessFunction())
                .name("fire-sensor-confirmation");

        DataStream<DailyH3Outcome> dailyOutcomes =
                finalPredictions.getSideOutput(FireAndSensorConfirmationProcessFunction.OUTCOME_OUTPUT_TAG);

        DataStream<DailyH3TrainingRow> dailyTrainingRows =
                finalPredictions.getSideOutput(FireAndSensorConfirmationProcessFunction.TRAINING_OUTPUT_TAG);

        DataStream<FeatureSnapshot> snapshots = snapshotPredictionRecords.map(r -> r.snapshot);

        snapshots.print("SNAPSHOT");
        finalPredictions.print("PREDICTION");
        dailyOutcomes.print("DAILY-OUTCOME");
        dailyTrainingRows.print("DAILY-TRAINING");

        snapshots
                .addSink(buildFeatureSnapshotSink(postgresUrl, postgresUser, postgresPassword))
                .name("feature-snapshots-to-postgres");

        finalPredictions
                .addSink(buildPredictionSink(postgresUrl, postgresUser, postgresPassword))
                .name("predictions-to-postgres");

        dailyOutcomes
                .addSink(buildDailyOutcomeSink(postgresUrl, postgresUser, postgresPassword))
                .name("daily-outcomes-to-postgres");

        dailyTrainingRows
                .addSink(buildDailyTrainingSink(postgresUrl, postgresUser, postgresPassword))
                .name("daily-training-to-postgres");

        env.execute("Unified H3 Direct Meteo + Fire/Sensor Confirmation Pipeline");
    }

    private static class DirectMeteoToSnapshotMapper extends RichMapFunction<String, FeatureSnapshot> {
        private transient Gson gson;
        private transient H3Core h3;
        private final Map<String, StaticNumericData> staticNumericMap;

        public DirectMeteoToSnapshotMapper(Map<String, StaticNumericData> staticNumericMap) {
            this.staticNumericMap = staticNumericMap;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            gson = new Gson();
            h3 = H3Core.newInstance();
        }

        @Override
        public FeatureSnapshot map(String json) {
            try {
                MeteoEvent e = gson.fromJson(json, MeteoEvent.class);
                if (e == null) {
                    return null;
                }

                String h3Cell = toH3(e.getLat(), e.getLon(), e.getH3Cell());
                if (h3Cell == null || e.getEventDate() == null || e.getEventTime() == null) {
                    return null;
                }

                StaticNumericData sdata = staticNumericMap.get(h3Cell);
                if (sdata == null) {
                    System.err.println("No static numeric data found for h3=" + h3Cell);
                    return null;
                }

                FeatureSnapshot s = new FeatureSnapshot();

                s.h3Cell = h3Cell;
                s.h3Commune = h3Cell;
                s.eventTime = e.getEventTime();
                s.eventDate = e.getEventDate();

                s.h3CenterLat = sdata.h3CenterLat;
                s.h3CenterLon = sdata.h3CenterLon;
                s.altitudeMoy = sdata.altitudeMoy;
                s.penteMoy = sdata.penteMoy;
                s.distRouteM = sdata.distRouteM;
                s.distUrbainM = sdata.distUrbainM;

                s.temperatureMoy = e.getTemperature();
                s.humidityMin = e.getHumidity();
                s.windSpeedMax = e.getWindSpeed();
                s.precipitation1dMm = e.getPrecipitation();
                s.precipitation7dSumMm = e.getPrecipitation();
                s.soilMoisture = e.getSoilMoisture();
                s.vpdKpa = e.getVpd();

                s.ndvi30dMean = sdata.ndvi30dMean;
                s.ndvi30dMin = sdata.ndvi30dMin;
                s.ndvi30dStd = sdata.ndvi30dStd;

                LocalDate date = LocalDate.parse(e.getEventDate());
                int month = date.getMonthValue();
                int dayOfYear = date.getDayOfYear();

                s.month = month;
                s.monthSin = Math.sin(2.0 * Math.PI * month / 12.0);
                s.monthCos = Math.cos(2.0 * Math.PI * month / 12.0);
                s.doySin = Math.sin(2.0 * Math.PI * dayOfYear / 365.0);
                s.doyCos = Math.cos(2.0 * Math.PI * dayOfYear / 365.0);
                s.isWeekend = isWeekend(date) ? 1 : 0;

                return s;

            } catch (Exception ex) {
                System.err.println("Error mapping raw meteo JSON to FeatureSnapshot");
                ex.printStackTrace();
                return null;
            }
        }

        private String toH3(Double lat, Double lon, String fallback) {
            if (lat != null && lon != null) {
                try {
                    return h3.geoToH3Address(lat, lon, H3_RESOLUTION);
                } catch (Exception ignored) {
                }
            }
            return fallback;
        }

        private boolean isWeekend(LocalDate date) {
            switch (date.getDayOfWeek()) {
                case SATURDAY:
                case SUNDAY:
                    return true;
                default:
                    return false;
            }
        }
    }

    private static class ValidFeatureSnapshotFilter implements FilterFunction<FeatureSnapshot> {
        @Override
        public boolean filter(FeatureSnapshot s) {
            return s != null
                    && s.h3Cell != null
                    && s.eventDate != null
                    && s.eventTime != null;
        }
    }

    private static SinkFunction<FeatureSnapshot> buildFeatureSnapshotSink(
            String postgresUrl,
            String postgresUser,
            String postgresPassword
    ) {
        String sql =
                "INSERT INTO feature_snapshots (" +
                        "h3_cell, h3_commune, event_time, event_date, " +
                        "h3_center_lat, h3_center_lon, altitude_moy, pente_moy, dist_route_m, dist_urbain_m, " +
                        "humidity_min, temperature_moy, wind_speed_max, precipitation_1d_mm, precipitation_7d_sum_mm, " +
                        "soil_moisture, vpd_kpa, " +
                        "ndvi_30d_mean, ndvi_30d_min, ndvi_30d_std, " +
                        "month, month_sin, month_cos, doy_sin, doy_cos, is_weekend" +
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return JdbcSink.sink(
                sql,
                (ps, s) -> {
                    setString(ps, 1, s.h3Cell);
                    setString(ps, 2, s.h3Commune);
                    setLong(ps, 3, s.eventTime);
                    setSqlDate(ps, 4, s.eventDate);

                    setDouble(ps, 5, s.h3CenterLat);
                    setDouble(ps, 6, s.h3CenterLon);
                    setDouble(ps, 7, s.altitudeMoy);
                    setDouble(ps, 8, s.penteMoy);
                    setDouble(ps, 9, s.distRouteM);
                    setDouble(ps, 10, s.distUrbainM);

                    setDouble(ps, 11, s.humidityMin);
                    setDouble(ps, 12, s.temperatureMoy);
                    setDouble(ps, 13, s.windSpeedMax);
                    setDouble(ps, 14, s.precipitation1dMm);
                    setDouble(ps, 15, s.precipitation7dSumMm);
                    setDouble(ps, 16, s.soilMoisture);
                    setDouble(ps, 17, s.vpdKpa);

                    setDouble(ps, 18, s.ndvi30dMean);
                    setDouble(ps, 19, s.ndvi30dMin);
                    setDouble(ps, 20, s.ndvi30dStd);

                    setInteger(ps, 21, s.month);
                    setDouble(ps, 22, s.monthSin);
                    setDouble(ps, 23, s.monthCos);
                    setDouble(ps, 24, s.doySin);
                    setDouble(ps, 25, s.doyCos);
                    setInteger(ps, 26, s.isWeekend);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(20)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(postgresUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(postgresUser)
                        .withPassword(postgresPassword)
                        .build()
        );
    }

    private static SinkFunction<PredictionResult> buildPredictionSink(
            String postgresUrl,
            String postgresUser,
            String postgresPassword
    ) {
        String sql =
                "INSERT INTO predictions (" +
                        "h3_cell, event_time, event_date, prediction, probability, risk_level, suspected_voluntary, " +
                        "trigger_type, trigger_source, is_neighbor_prediction, trigger_h3_cell, confirmed_alert, " +
                        "fire_observed, sensor_confirmed, confirmation_time, prediction_slot" +
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return JdbcSink.sink(
                sql,
                (ps, p) -> {
                    setString(ps, 1, p.h3Cell);
                    setLong(ps, 2, p.eventTime);
                    setSqlDate(ps, 3, p.eventDate);
                    setInteger(ps, 4, p.prediction);
                    setDouble(ps, 5, p.probability);
                    setString(ps, 6, p.riskLevel);
                    setInteger(ps, 7, p.suspectedVoluntary);

                    setString(ps, 8, p.triggerType);
                    setString(ps, 9, p.triggerSource);
                    setBoolean(ps, 10, p.isNeighborPrediction);
                    setString(ps, 11, p.triggerH3Cell);
                    setBoolean(ps, 12, p.confirmedAlert);
                    setBoolean(ps, 13, p.fireObserved);
                    setBoolean(ps, 14, p.sensorConfirmed);
                    setLong(ps, 15, p.confirmationTime);
                    setString(ps, 16, p.predictionSlot);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(20)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(postgresUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(postgresUser)
                        .withPassword(postgresPassword)
                        .build()
        );
    }

    private static SinkFunction<DailyH3Outcome> buildDailyOutcomeSink(
            String postgresUrl,
            String postgresUser,
            String postgresPassword
    ) {
        String sql =
                "INSERT INTO daily_h3_outcomes (" +
                        "h3_cell, event_date, sensor_confirmed, fire_observed, fire_confirmed_with_sensor, " +
                        "confirmation_time, confirmation_source, last_prediction_before_alert, " +
                        "last_probability_before_alert, last_risk_before_alert, suspected_voluntary, " +
                        "latest_meteo_event_time, latest_meteo_slot" +
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (h3_cell, event_date) DO UPDATE SET " +
                        "sensor_confirmed = EXCLUDED.sensor_confirmed, " +
                        "fire_observed = EXCLUDED.fire_observed, " +
                        "fire_confirmed_with_sensor = EXCLUDED.fire_confirmed_with_sensor, " +
                        "confirmation_time = EXCLUDED.confirmation_time, " +
                        "confirmation_source = EXCLUDED.confirmation_source, " +
                        "last_prediction_before_alert = EXCLUDED.last_prediction_before_alert, " +
                        "last_probability_before_alert = EXCLUDED.last_probability_before_alert, " +
                        "last_risk_before_alert = EXCLUDED.last_risk_before_alert, " +
                        "suspected_voluntary = EXCLUDED.suspected_voluntary, " +
                        "latest_meteo_event_time = EXCLUDED.latest_meteo_event_time, " +
                        "latest_meteo_slot = EXCLUDED.latest_meteo_slot";

        return JdbcSink.sink(
                sql,
                (ps, o) -> {
                    setString(ps, 1, o.h3Cell);
                    setSqlDate(ps, 2, o.eventDate);
                    setBoolean(ps, 3, o.sensorConfirmed);
                    setBoolean(ps, 4, o.fireObserved);
                    setBoolean(ps, 5, o.fireConfirmedWithSensor);
                    setLong(ps, 6, o.confirmationTime);
                    setString(ps, 7, o.confirmationSource);
                    setInteger(ps, 8, o.lastPredictionBeforeAlert);
                    setDouble(ps, 9, o.lastProbabilityBeforeAlert);
                    setString(ps, 10, o.lastRiskBeforeAlert);
                    setInteger(ps, 11, o.suspectedVoluntary);
                    setLong(ps, 12, o.latestMeteoEventTime);
                    setString(ps, 13, o.latestMeteoSlot);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(20)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(postgresUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(postgresUser)
                        .withPassword(postgresPassword)
                        .build()
        );
    }

    private static SinkFunction<DailyH3TrainingRow> buildDailyTrainingSink(
            String postgresUrl,
            String postgresUser,
            String postgresPassword
    ) {
        String sql =
                "INSERT INTO daily_h3_training_dataset (" +
                        "h3_cell, event_date, h3_commune, h3_center_lat, h3_center_lon, " +
                        "altitude_moy, pente_moy, dist_route_m, dist_urbain_m, " +
                        "humidity_min, temperature_moy, wind_speed_max, precipitation_1d_mm, " +
                        "precipitation_7d_sum_mm, soil_moisture, vpd_kpa, " +
                        "ndvi_30d_mean, ndvi_30d_min, ndvi_30d_std, " +
                        "month, month_sin, month_cos, doy_sin, doy_cos, is_weekend, " +
                        "input_event_time, input_slot, label, confirmation_source, suspected_voluntary, " +
                        "fire_observed, sensor_confirmed" +
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (h3_cell, event_date) DO UPDATE SET " +
                        "h3_commune = EXCLUDED.h3_commune, " +
                        "h3_center_lat = EXCLUDED.h3_center_lat, " +
                        "h3_center_lon = EXCLUDED.h3_center_lon, " +
                        "altitude_moy = EXCLUDED.altitude_moy, " +
                        "pente_moy = EXCLUDED.pente_moy, " +
                        "dist_route_m = EXCLUDED.dist_route_m, " +
                        "dist_urbain_m = EXCLUDED.dist_urbain_m, " +
                        "humidity_min = EXCLUDED.humidity_min, " +
                        "temperature_moy = EXCLUDED.temperature_moy, " +
                        "wind_speed_max = EXCLUDED.wind_speed_max, " +
                        "precipitation_1d_mm = EXCLUDED.precipitation_1d_mm, " +
                        "precipitation_7d_sum_mm = EXCLUDED.precipitation_7d_sum_mm, " +
                        "soil_moisture = EXCLUDED.soil_moisture, " +
                        "vpd_kpa = EXCLUDED.vpd_kpa, " +
                        "ndvi_30d_mean = EXCLUDED.ndvi_30d_mean, " +
                        "ndvi_30d_min = EXCLUDED.ndvi_30d_min, " +
                        "ndvi_30d_std = EXCLUDED.ndvi_30d_std, " +
                        "month = EXCLUDED.month, " +
                        "month_sin = EXCLUDED.month_sin, " +
                        "month_cos = EXCLUDED.month_cos, " +
                        "doy_sin = EXCLUDED.doy_sin, " +
                        "doy_cos = EXCLUDED.doy_cos, " +
                        "is_weekend = EXCLUDED.is_weekend, " +
                        "input_event_time = EXCLUDED.input_event_time, " +
                        "input_slot = EXCLUDED.input_slot, " +
                        "label = EXCLUDED.label, " +
                        "confirmation_source = EXCLUDED.confirmation_source, " +
                        "suspected_voluntary = EXCLUDED.suspected_voluntary, " +
                        "fire_observed = EXCLUDED.fire_observed, " +
                        "sensor_confirmed = EXCLUDED.sensor_confirmed";

        return JdbcSink.sink(
                sql,
                (ps, r) -> {
                    setString(ps, 1, r.h3Cell);
                    setSqlDate(ps, 2, r.eventDate);
                    setString(ps, 3, r.h3Commune);
                    setDouble(ps, 4, r.h3CenterLat);
                    setDouble(ps, 5, r.h3CenterLon);

                    setDouble(ps, 6, r.altitudeMoy);
                    setDouble(ps, 7, r.penteMoy);
                    setDouble(ps, 8, r.distRouteM);
                    setDouble(ps, 9, r.distUrbainM);

                    setDouble(ps, 10, r.humidityMin);
                    setDouble(ps, 11, r.temperatureMoy);
                    setDouble(ps, 12, r.windSpeedMax);
                    setDouble(ps, 13, r.precipitation1dMm);
                    setDouble(ps, 14, r.precipitation7dSumMm);
                    setDouble(ps, 15, r.soilMoisture);
                    setDouble(ps, 16, r.vpdKpa);

                    setDouble(ps, 17, r.ndvi30dMean);
                    setDouble(ps, 18, r.ndvi30dMin);
                    setDouble(ps, 19, r.ndvi30dStd);

                    setInteger(ps, 20, r.month);
                    setDouble(ps, 21, r.monthSin);
                    setDouble(ps, 22, r.monthCos);
                    setDouble(ps, 23, r.doySin);
                    setDouble(ps, 24, r.doyCos);
                    setInteger(ps, 25, r.isWeekend);

                    setLong(ps, 26, r.inputEventTime);
                    setString(ps, 27, r.inputSlot);
                    setInteger(ps, 28, r.label);
                    setString(ps, 29, r.confirmationSource);
                    setInteger(ps, 30, r.suspectedVoluntary);
                    setBoolean(ps, 31, r.fireObserved);
                    setBoolean(ps, 32, r.sensorConfirmed);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(20)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(postgresUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(postgresUser)
                        .withPassword(postgresPassword)
                        .build()
        );
    }

    private static void setString(PreparedStatement ps, int index, String value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, value);
        }
    }

    private static void setLong(PreparedStatement ps, int index, Long value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.BIGINT);
        } else {
            ps.setLong(index, value);
        }
    }

    private static void setInteger(PreparedStatement ps, int index, Integer value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.INTEGER);
        } else {
            ps.setInt(index, value);
        }
    }

    private static void setDouble(PreparedStatement ps, int index, Double value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.DOUBLE);
        } else {
            ps.setDouble(index, value);
        }
    }

    private static void setBoolean(PreparedStatement ps, int index, Boolean value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.BOOLEAN);
        } else {
            ps.setBoolean(index, value);
        }
    }

    private static void setSqlDate(PreparedStatement ps, int index, String value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.DATE);
        } else {
            ps.setDate(index, java.sql.Date.valueOf(value));
        }
    }

    private static Map<String, StaticNumericData> loadStaticNumeric(
            String postgresUrl,
            String postgresUser,
            String postgresPassword
    ) throws Exception {
        Map<String, StaticNumericData> map = new HashMap<>();

        String sql =
                "SELECT " +
                        "h3_cell, altitude_moy, pente_moy, dist_route_m, dist_urbain_m, " +
                        "ndvi_30d_mean, ndvi_30d_min, ndvi_30d_std, h3_center_lat, h3_center_lon " +
                        "FROM h3_static_numeric";

        try (Connection conn = DriverManager.getConnection(postgresUrl, postgresUser, postgresPassword);
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                StaticNumericData data = new StaticNumericData();
                data.h3Cell = rs.getString("h3_cell");
                data.altitudeMoy = getNullableDouble(rs, "altitude_moy");
                data.penteMoy = getNullableDouble(rs, "pente_moy");
                data.distRouteM = getNullableDouble(rs, "dist_route_m");
                data.distUrbainM = getNullableDouble(rs, "dist_urbain_m");
                data.ndvi30dMean = getNullableDouble(rs, "ndvi_30d_mean");
                data.ndvi30dMin = getNullableDouble(rs, "ndvi_30d_min");
                data.ndvi30dStd = getNullableDouble(rs, "ndvi_30d_std");
                data.h3CenterLat = getNullableDouble(rs, "h3_center_lat");
                data.h3CenterLon = getNullableDouble(rs, "h3_center_lon");
                map.put(data.h3Cell, data);
            }
        }

        System.out.println("Loaded h3_static_numeric rows: " + map.size());
        return map;
    }

    private static Double getNullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }

    private static class StaticNumericData implements Serializable {
        String h3Cell;
        Double altitudeMoy;
        Double penteMoy;
        Double distRouteM;
        Double distUrbainM;
        Double ndvi30dMean;
        Double ndvi30dMin;
        Double ndvi30dStd;
        Double h3CenterLat;
        Double h3CenterLon;
    }

    private static abstract class BaseUnifiedMapper extends RichMapFunction<String, UnifiedEvent> {
        protected transient Gson gson;
        protected transient H3Core h3;
        protected final Map<String, StaticNumericData> staticNumericMap;

        protected BaseUnifiedMapper(Map<String, StaticNumericData> staticNumericMap) {
            this.staticNumericMap = staticNumericMap;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            gson = new Gson();
            h3 = H3Core.newInstance();
        }

        protected String toH3(Double lat, Double lon, String fallback) {
            if (lat != null && lon != null) {
                try {
                    return h3.geoToH3Address(lat, lon, H3_RESOLUTION);
                } catch (Exception ignored) {
                }
            }
            return fallback;
        }

        protected void enrichStatic(UnifiedEvent u, String h3Cell) {
            if (u == null || h3Cell == null) {
                return;
            }

            StaticNumericData s = staticNumericMap.get(h3Cell);
            if (s == null) {
                return;
            }

            u.setAltitudeMoy(s.altitudeMoy);
            u.setPenteMoy(s.penteMoy);
            u.setDistRouteM(s.distRouteM);
            u.setDistUrbainM(s.distUrbainM);
            u.setNdvi30dMean(s.ndvi30dMean);
            u.setNdvi30dMin(s.ndvi30dMin);
            u.setNdvi30dStd(s.ndvi30dStd);
            u.setH3CenterLat(s.h3CenterLat);
            u.setH3CenterLon(s.h3CenterLon);
        }
    }

    private static class SensorUnifiedMapper extends BaseUnifiedMapper {
        public SensorUnifiedMapper(Map<String, StaticNumericData> staticNumericMap) {
            super(staticNumericMap);
        }

        @Override
        public UnifiedEvent map(String json) {
            try {
                SensorEvent e = gson.fromJson(json, SensorEvent.class);
                if (e == null) {
                    return null;
                }

                UnifiedEvent u = new UnifiedEvent();
                u.setSourceType("sensor");

                String h3Cell = toH3(e.getLat(), e.getLon(), e.getH3Cell());
                u.setH3Cell(h3Cell);
                u.setEventTime(e.getEventTime());
                u.setEventDate(e.getEventDate());
                u.setLat(e.getLat());
                u.setLon(e.getLon());

                u.setCo2(e.getCo2());
                u.setSmoke(e.getSmoke());
                u.setSensorTemperature(e.getSensorTemperature());
                u.setBattery(e.getBattery());

                enrichStatic(u, h3Cell);
                return u;
            } catch (Exception ex) {
                return null;
            }
        }
    }

    private static class FireUnifiedMapper extends BaseUnifiedMapper {
        public FireUnifiedMapper(Map<String, StaticNumericData> staticNumericMap) {
            super(staticNumericMap);
        }

        @Override
        public UnifiedEvent map(String json) {
            try {
                FireEvent e = gson.fromJson(json, FireEvent.class);
                if (e == null) {
                    return null;
                }

                UnifiedEvent u = new UnifiedEvent();
                u.setSourceType("fire");

                String h3Cell = toH3(e.getLat(), e.getLon(), e.getH3Cell());
                u.setH3Cell(h3Cell);
                u.setEventTime(e.getEventTime());
                u.setEventDate(e.getEventDate());
                u.setLat(e.getLat());
                u.setLon(e.getLon());

                u.setFireDeclared(Boolean.TRUE.equals(e.getDeclared()) ? 1 : 0);
                u.setIntensity(e.getIntensity());
                u.setBurnedArea(e.getBurnedArea());

                enrichStatic(u, h3Cell);
                return u;
            } catch (Exception ex) {
                return null;
            }
        }
    }

    private static class ValidUnifiedEventFilter implements FilterFunction<UnifiedEvent> {
        @Override
        public boolean filter(UnifiedEvent event) {
            return event != null
                    && event.getH3Cell() != null
                    && event.getEventDate() != null
                    && event.getEventTime() != null;
        }
    }
}