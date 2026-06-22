package com.environment.flink.process;

import com.environment.flink.model.DailyH3Outcome;
import com.environment.flink.model.DailyH3TrainingRow;
import com.environment.flink.model.FeatureSnapshot;
import com.environment.flink.model.PredictionResult;
import com.environment.flink.model.SnapshotPredictionRecord;
import com.environment.flink.model.UnifiedEvent;
import com.environment.flink.util.OnnxOrdinalPredictorNoHistory;
import com.uber.h3core.H3Core;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

public class FireAndSensorConfirmationProcessFunction
        extends KeyedCoProcessFunction<String, SnapshotPredictionRecord, UnifiedEvent, PredictionResult> {

    public static final OutputTag<DailyH3Outcome> OUTCOME_OUTPUT_TAG =
            new OutputTag<DailyH3Outcome>("daily-h3-outcomes") {};

    public static final OutputTag<DailyH3TrainingRow> TRAINING_OUTPUT_TAG =
            new OutputTag<DailyH3TrainingRow>("daily-h3-training-rows") {};

    private transient ValueState<FeatureSnapshot> lastSnapshotState;
    private transient ValueState<PredictionResult> lastPredictionState;
    private transient ValueState<String> activeDateState;
    private transient ValueState<Boolean> sensorConfirmedState;
    private transient ValueState<Boolean> fireObservedState;
    private transient ValueState<Boolean> alertEmittedState;
    private transient ValueState<Boolean> trainingWrittenState;

    private transient H3Core h3;
    private transient OnnxOrdinalPredictorNoHistory predictor;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastSnapshotState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastSnapshotState", FeatureSnapshot.class)
        );
        lastPredictionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastPredictionState", PredictionResult.class)
        );
        activeDateState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("activeDateState", String.class)
        );
        sensorConfirmedState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("sensorConfirmedState", Boolean.class)
        );
        fireObservedState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("fireObservedState", Boolean.class)
        );
        alertEmittedState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("alertEmittedState", Boolean.class)
        );
        trainingWrittenState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("trainingWrittenState", Boolean.class)
        );

        h3 = H3Core.newInstance();
        predictor = new OnnxOrdinalPredictorNoHistory();
    }

    @Override
    public void processElement1(
            SnapshotPredictionRecord value,
            Context ctx,
            Collector<PredictionResult> out
    ) throws Exception {
        if (value == null || value.snapshot == null || value.prediction == null) {
            return;
        }

        rolloverIfNeeded(value.snapshot.eventDate, ctx);

        if (activeDateState.value() == null) {
            activeDateState.update(value.snapshot.eventDate);
        }

        lastSnapshotState.update(value.snapshot);
        lastPredictionState.update(value.prediction);

        out.collect(value.prediction);
    }

    @Override
    public void processElement2(
            UnifiedEvent event,
            Context ctx,
            Collector<PredictionResult> out
    ) throws Exception {

        if (event == null || event.getEventDate() == null) {
            return;
        }

        rolloverIfNeeded(event.getEventDate(), ctx);

        if (activeDateState.value() == null) {
            activeDateState.update(event.getEventDate());
        }

        if ("fire".equalsIgnoreCase(event.getSourceType())) {
            fireObservedState.update(true);

            DailyH3Outcome pendingOutcome = buildOutcome(
                    event.getH3Cell(),
                    event.getEventDate(),
                    bool(sensorConfirmedState.value()),
                    true,
                    bool(sensorConfirmedState.value()),
                    event.getEventTime(),
                    bool(sensorConfirmedState.value()) ? "sensor_fire" : "fire_pending",
                    lastPredictionState.value(),
                    lastSnapshotState.value(),
                    computeSuspectedVoluntary(lastPredictionState.value(), true)
            );
            ctx.output(OUTCOME_OUTPUT_TAG, pendingOutcome);

            if (bool(sensorConfirmedState.value()) && !bool(alertEmittedState.value())) {
                emitConfirmedAlertAndTraining(event, ctx, out, true);
            }
            return;
        }

        if ("sensor".equalsIgnoreCase(event.getSourceType())) {
            sensorConfirmedState.update(true);

            if (!bool(alertEmittedState.value())) {
                emitConfirmedAlertAndTraining(event, ctx, out, bool(fireObservedState.value()));
            }
        }
    }

    private void emitConfirmedAlertAndTraining(
            UnifiedEvent event,
            Context ctx,
            Collector<PredictionResult> out,
            boolean fireObserved
    ) throws Exception {

        FeatureSnapshot lastSnapshot = lastSnapshotState.value();
        PredictionResult lastPrediction = lastPredictionState.value();

        if (lastSnapshot == null || lastPrediction == null) {
            return;
        }

        int suspectedVoluntary = computeSuspectedVoluntary(lastPrediction, fireObserved);
        String confirmationSource = fireObserved ? "sensor_fire" : "sensor";

        PredictionResult alertPrediction = copyPrediction(lastPrediction);
        alertPrediction.eventTime = event.getEventTime();
        alertPrediction.eventDate = event.getEventDate();
        alertPrediction.triggerType = "confirmed_alert";
        alertPrediction.triggerSource = confirmationSource;
        alertPrediction.isNeighborPrediction = false;
        alertPrediction.triggerH3Cell = event.getH3Cell();
        alertPrediction.confirmedAlert = true;
        alertPrediction.fireObserved = fireObserved;
        alertPrediction.sensorConfirmed = true;
        alertPrediction.confirmationTime = event.getEventTime();
        alertPrediction.suspectedVoluntary = suspectedVoluntary;
        out.collect(alertPrediction);

        List<String> neighbors = h3.kRing(event.getH3Cell(), 1);
        for (String neighbor : neighbors) {
            if (neighbor == null || neighbor.equals(event.getH3Cell())) {
                continue;
            }

            FeatureSnapshot neighborSnapshot = copySnapshot(lastSnapshot);
            neighborSnapshot.h3Cell = neighbor;
            neighborSnapshot.h3Commune = neighbor;

            PredictionResult neighborPrediction = predictor.predict(neighborSnapshot, "neighbor");
            if (neighborPrediction != null) {
                neighborPrediction.triggerType = "neighbor_propagation";
                neighborPrediction.triggerSource = confirmationSource;
                neighborPrediction.isNeighborPrediction = true;
                neighborPrediction.triggerH3Cell = event.getH3Cell();
                neighborPrediction.confirmedAlert = true;
                neighborPrediction.fireObserved = fireObserved;
                neighborPrediction.sensorConfirmed = true;
                neighborPrediction.confirmationTime = event.getEventTime();
                neighborPrediction.predictionSlot = slotFromTime(neighborSnapshot.eventTime);
                neighborPrediction.suspectedVoluntary = 0;
                out.collect(neighborPrediction);
            }
        }

        DailyH3Outcome outcome = buildOutcome(
                event.getH3Cell(),
                event.getEventDate(),
                true,
                fireObserved,
                fireObserved,
                event.getEventTime(),
                confirmationSource,
                lastPrediction,
                lastSnapshot,
                suspectedVoluntary
        );
        ctx.output(OUTCOME_OUTPUT_TAG, outcome);

        DailyH3TrainingRow row = buildTrainingRow(
                lastSnapshot,
                event.getEventDate(),
                event.getEventTime(),
                confirmationSource,
                1,
                suspectedVoluntary,
                fireObserved,
                true
        );
        ctx.output(TRAINING_OUTPUT_TAG, row);

        alertEmittedState.update(true);
        trainingWrittenState.update(true);
    }

    private void rolloverIfNeeded(String newDate, Context ctx) throws Exception {
        String activeDate = activeDateState.value();
        if (activeDate == null || activeDate.equals(newDate)) {
            return;
        }

        FeatureSnapshot lastSnapshot = lastSnapshotState.value();

        if (lastSnapshot != null && !bool(trainingWrittenState.value())) {
            DailyH3TrainingRow negativeRow = buildTrainingRow(
                    lastSnapshot,
                    activeDate,
                    lastSnapshot.eventTime,
                    "none",
                    0,
                    0,
                    bool(fireObservedState.value()),
                    bool(sensorConfirmedState.value())
            );
            ctx.output(TRAINING_OUTPUT_TAG, negativeRow);
        }

        lastSnapshotState.clear();
        lastPredictionState.clear();
        sensorConfirmedState.clear();
        fireObservedState.clear();
        alertEmittedState.clear();
        trainingWrittenState.clear();
        activeDateState.update(newDate);
    }

    private DailyH3Outcome buildOutcome(
            String h3Cell,
            String eventDate,
            boolean sensorConfirmed,
            boolean fireObserved,
            boolean fireConfirmedWithSensor,
            Long confirmationTime,
            String confirmationSource,
            PredictionResult lastPrediction,
            FeatureSnapshot lastSnapshot,
            int suspectedVoluntary
    ) {
        DailyH3Outcome o = new DailyH3Outcome();
        o.h3Cell = h3Cell;
        o.eventDate = eventDate;
        o.sensorConfirmed = sensorConfirmed;
        o.fireObserved = fireObserved;
        o.fireConfirmedWithSensor = fireConfirmedWithSensor;
        o.confirmationTime = confirmationTime;
        o.confirmationSource = confirmationSource;
        o.lastPredictionBeforeAlert = lastPrediction != null ? lastPrediction.prediction : null;
        o.lastProbabilityBeforeAlert = lastPrediction != null ? lastPrediction.probability : null;
        o.lastRiskBeforeAlert = lastPrediction != null ? lastPrediction.riskLevel : null;
        o.suspectedVoluntary = suspectedVoluntary;
        o.latestMeteoEventTime = lastSnapshot != null ? lastSnapshot.eventTime : null;
        o.latestMeteoSlot = lastSnapshot != null ? slotFromTime(lastSnapshot.eventTime) : null;
        return o;
    }

    private DailyH3TrainingRow buildTrainingRow(
            FeatureSnapshot s,
            String eventDate,
            Long inputEventTime,
            String confirmationSource,
            int label,
            int suspectedVoluntary,
            boolean fireObserved,
            boolean sensorConfirmed
    ) {
        DailyH3TrainingRow r = new DailyH3TrainingRow();

        r.h3Cell = s.h3Cell;
        r.eventDate = eventDate;
        r.h3Commune = s.h3Commune;

        r.h3CenterLat = s.h3CenterLat;
        r.h3CenterLon = s.h3CenterLon;
        r.altitudeMoy = s.altitudeMoy;
        r.penteMoy = s.penteMoy;
        r.distRouteM = s.distRouteM;
        r.distUrbainM = s.distUrbainM;

        r.humidityMin = s.humidityMin;
        r.temperatureMoy = s.temperatureMoy;
        r.windSpeedMax = s.windSpeedMax;
        r.precipitation1dMm = s.precipitation1dMm;
        r.precipitation7dSumMm = s.precipitation7dSumMm;
        r.soilMoisture = s.soilMoisture;
        r.vpdKpa = s.vpdKpa;

        r.ndvi30dMean = s.ndvi30dMean;
        r.ndvi30dMin = s.ndvi30dMin;
        r.ndvi30dStd = s.ndvi30dStd;

        r.month = s.month;
        r.monthSin = s.monthSin;
        r.monthCos = s.monthCos;
        r.doySin = s.doySin;
        r.doyCos = s.doyCos;
        r.isWeekend = s.isWeekend;

        r.inputEventTime = inputEventTime;
        r.inputSlot = slotFromTime(inputEventTime);

        r.label = label;
        r.confirmationSource = confirmationSource;
        r.suspectedVoluntary = suspectedVoluntary;
        r.fireObserved = fireObserved;
        r.sensorConfirmed = sensorConfirmed;

        return r;
    }

    private PredictionResult copyPrediction(PredictionResult p) {
        PredictionResult x = new PredictionResult();
        x.h3Cell = p.h3Cell;
        x.eventTime = p.eventTime;
        x.eventDate = p.eventDate;
        x.prediction = p.prediction;
        x.probability = p.probability;
        x.riskLevel = p.riskLevel;
        x.suspectedVoluntary = p.suspectedVoluntary;
        x.triggerType = p.triggerType;
        x.triggerSource = p.triggerSource;
        x.isNeighborPrediction = p.isNeighborPrediction;
        x.triggerH3Cell = p.triggerH3Cell;
        x.confirmedAlert = p.confirmedAlert;
        x.fireObserved = p.fireObserved;
        x.sensorConfirmed = p.sensorConfirmed;
        x.confirmationTime = p.confirmationTime;
        x.predictionSlot = p.predictionSlot;
        return x;
    }

    private FeatureSnapshot copySnapshot(FeatureSnapshot s) {
        FeatureSnapshot x = new FeatureSnapshot();
        x.h3Cell = s.h3Cell;
        x.h3Commune = s.h3Commune;
        x.eventTime = s.eventTime;
        x.eventDate = s.eventDate;

        x.h3CenterLat = s.h3CenterLat;
        x.h3CenterLon = s.h3CenterLon;
        x.altitudeMoy = s.altitudeMoy;
        x.penteMoy = s.penteMoy;
        x.distRouteM = s.distRouteM;
        x.distUrbainM = s.distUrbainM;

        x.humidityMin = s.humidityMin;
        x.temperatureMoy = s.temperatureMoy;
        x.windSpeedMax = s.windSpeedMax;
        x.precipitation1dMm = s.precipitation1dMm;
        x.precipitation7dSumMm = s.precipitation7dSumMm;
        x.soilMoisture = s.soilMoisture;
        x.vpdKpa = s.vpdKpa;

        x.ndvi30dMean = s.ndvi30dMean;
        x.ndvi30dMin = s.ndvi30dMin;
        x.ndvi30dStd = s.ndvi30dStd;

        x.month = s.month;
        x.monthSin = s.monthSin;
        x.monthCos = s.monthCos;
        x.doySin = s.doySin;
        x.doyCos = s.doyCos;
        x.isWeekend = s.isWeekend;
        return x;
    }

    private int computeSuspectedVoluntary(PredictionResult p, boolean fireObserved) {
        if (!fireObserved || p == null || p.riskLevel == null) {
            return 0;
        }
        return "LOW".equalsIgnoreCase(p.riskLevel) ? 1 : 0;
    }

    private String slotFromTime(Long eventTime) {
        if (eventTime == null) return null;
        int hour = Instant.ofEpochMilli(eventTime)
                .atZone(ZoneId.of("Africa/Algiers"))
                .getHour();

        if (hour == 8) return "08:00";
        if (hour == 14) return "14:00";
        if (hour == 20) return "20:00";
        return String.format("%02d:00", hour);
    }

    private boolean bool(Boolean value) {
        return Boolean.TRUE.equals(value);
    }

    @Override
    public void close() throws Exception {
        if (predictor != null) {
            predictor.close();
        }
    }
}