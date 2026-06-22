package com.environment.flink.model;

import java.io.Serializable;

public class SnapshotPredictionRecord implements Serializable {
    public FeatureSnapshot snapshot;
    public PredictionResult prediction;

    public SnapshotPredictionRecord() {
    }

    public SnapshotPredictionRecord(FeatureSnapshot snapshot, PredictionResult prediction) {
        this.snapshot = snapshot;
        this.prediction = prediction;
    }
}