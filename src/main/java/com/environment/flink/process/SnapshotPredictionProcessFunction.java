package com.environment.flink.process;

import com.environment.flink.model.FeatureSnapshot;
import com.environment.flink.model.PredictionResult;
import com.environment.flink.model.SnapshotPredictionRecord;
import com.environment.flink.util.OnnxOrdinalPredictorNoHistory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;

public class SnapshotPredictionProcessFunction
        extends KeyedProcessFunction<String, FeatureSnapshot, SnapshotPredictionRecord> {

    private transient OnnxOrdinalPredictorNoHistory predictor;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.predictor = new OnnxOrdinalPredictorNoHistory();
    }

    @Override
    public void processElement(
            FeatureSnapshot snapshot,
            Context ctx,
            Collector<SnapshotPredictionRecord> out
    ) throws Exception {

        if (snapshot == null) {
            return;
        }

        PredictionResult result = predictor.predict(snapshot, "meteo");
        if (result == null) {
            return;
        }

        result.triggerType = "regular_meteo";
        result.triggerSource = "meteo";
        result.isNeighborPrediction = false;
        result.triggerH3Cell = null;
        result.confirmedAlert = false;
        result.fireObserved = false;
        result.sensorConfirmed = false;
        result.confirmationTime = null;
        result.predictionSlot = extractSlot(snapshot.eventTime);

        out.collect(new SnapshotPredictionRecord(snapshot, result));
    }

    private String extractSlot(Long eventTime) {
        if (eventTime == null) return null;
        int hour = Instant.ofEpochMilli(eventTime)
                .atZone(ZoneId.of("Africa/Algiers"))
                .getHour();

        if (hour == 8) return "08:00";
        if (hour == 14) return "14:00";
        if (hour == 20) return "20:00";
        return String.format("%02d:00", hour);
    }

    @Override
    public void close() throws Exception {
        if (predictor != null) {
            predictor.close();
        }
    }
}