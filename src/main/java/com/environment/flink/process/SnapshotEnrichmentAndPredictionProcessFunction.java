package com.environment.flink.process;

import com.environment.flink.model.FeatureSnapshot;
import com.environment.flink.model.PredictionResult;
import com.environment.flink.util.OnnxOrdinalPredictorNoHistory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SnapshotEnrichmentAndPredictionProcessFunction
        extends KeyedProcessFunction<String, FeatureSnapshot, PredictionResult> {

    public static final OutputTag<FeatureSnapshot> SNAPSHOT_OUTPUT_TAG =
            new OutputTag<FeatureSnapshot>("feature-snapshots") {};

    private transient OnnxOrdinalPredictorNoHistory predictor;

    public SnapshotEnrichmentAndPredictionProcessFunction() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.predictor = new OnnxOrdinalPredictorNoHistory();
    }

    @Override
    public void processElement(
            FeatureSnapshot snapshot,
            Context ctx,
            Collector<PredictionResult> out
    ) throws Exception {

        if (snapshot == null) {
            return;
        }

        ctx.output(SNAPSHOT_OUTPUT_TAG, snapshot);

        String triggerSource = "window";

        try {
            System.out.println(
                    "DEBUG before embedded predict: h3Cell=" + snapshot.h3Cell +
                    ", h3Commune=" + snapshot.h3Commune +
                    ", eventDate=" + snapshot.eventDate
            );

            PredictionResult result = predictor.predict(snapshot, triggerSource);

            if (result != null) {
                out.collect(result);
                System.out.println(
                        "PREDICTION emitted: h3=" + result.h3Cell +
                        ", class=" + result.prediction +
                        ", probability=" + result.probability +
                        ", risk=" + result.riskLevel
                );
            } else {
                System.out.println("Predictor returned null result for h3=" + snapshot.h3Cell);
            }

        } catch (Exception e) {
            System.err.println(
                    "ERROR during embedded prediction for h3=" + snapshot.h3Cell +
                    ", h3Commune=" + snapshot.h3Commune +
                    ", eventDate=" + snapshot.eventDate
            );
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        if (predictor != null) {
            predictor.close();
        }
    }
}