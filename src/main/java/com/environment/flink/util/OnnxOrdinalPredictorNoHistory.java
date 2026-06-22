package com.environment.flink.util;

import com.environment.flink.model.FeatureSnapshot;
import com.environment.flink.model.PredictionResult;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import ai.onnxruntime.OnnxMap;
import ai.onnxruntime.OnnxSequence;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.FloatBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OnnxOrdinalPredictorNoHistory implements AutoCloseable {

    private static final String MODEL_GE_1_RESOURCE = "model_ge_1_no_neighbors.onnx";
    private static final String MODEL_GE_2_RESOURCE = "model_ge_2_no_neighbors.onnx";
    private static final String FEATURES_RESOURCE = "model_features_no_neighbors.json";
    private static final String ENCODER_RESOURCE = "encoders_no_neighbors.json";
    private static final String THRESHOLDS_RESOURCE = "thresholds_no_neighbors.json";

    private final OrtEnvironment ortEnv;
    private final OrtSession sessionGe1;
    private final OrtSession sessionGe2;

    private final List<String> features;
    private final Map<String, Integer> h3EncoderMap;

    private final double mediumThreshold;
    private final double highThreshold;

    private final String inputNameGe1;
    private final String inputNameGe2;

    private final Gson gson = new Gson();

    public OnnxOrdinalPredictorNoHistory() throws Exception {
        this.ortEnv = OrtEnvironment.getEnvironment();

        OrtSession.SessionOptions sessionOptions = new OrtSession.SessionOptions();
        sessionOptions.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);

        byte[] modelGe1Bytes = readResourceBytes(MODEL_GE_1_RESOURCE);
        byte[] modelGe2Bytes = readResourceBytes(MODEL_GE_2_RESOURCE);

        this.sessionGe1 = ortEnv.createSession(modelGe1Bytes, sessionOptions);
        this.sessionGe2 = ortEnv.createSession(modelGe2Bytes, sessionOptions);

        this.inputNameGe1 = sessionGe1.getInputNames().iterator().next();
        this.inputNameGe2 = sessionGe2.getInputNames().iterator().next();

        this.features = loadStringListResource(FEATURES_RESOURCE);

        Map<String, List<String>> encoders = loadEncodersResource(ENCODER_RESOURCE);
        List<String> h3Classes = encoders.get("h3_commune");
        if (h3Classes == null || h3Classes.isEmpty()) {
            throw new RuntimeException("Missing h3_commune encoder in resource: " + ENCODER_RESOURCE);
        }
        this.h3EncoderMap = buildEncoderMap(h3Classes);

        Map<String, Double> thresholds = loadThresholdsResource(THRESHOLDS_RESOURCE);
        Double mt = thresholds.get("MEDIUM_THRESHOLD");
        Double ht = thresholds.get("HIGH_THRESHOLD");
        if (mt == null || ht == null) {
            throw new RuntimeException("Missing thresholds in resource: " + THRESHOLDS_RESOURCE);
        }
        this.mediumThreshold = mt;
        this.highThreshold = ht;

        System.out.println("ONNX predictor initialized successfully");
        System.out.println("Loaded features count = " + features.size());
        System.out.println("Loaded h3 encoder classes count = " + h3EncoderMap.size());
        System.out.println("Loaded thresholds: medium=" + mediumThreshold + ", high=" + highThreshold);
        System.out.println("Model 1 input name = " + inputNameGe1);
        System.out.println("Model 2 input name = " + inputNameGe2);
    }

    public PredictionResult predict(FeatureSnapshot snapshot, String triggerSource) throws Exception {
        normalizeH3(snapshot);

        String effectiveH3 = firstNonBlank(snapshot.h3Commune, snapshot.h3Cell);
        System.out.println("DEBUG INPUT H3 = " + effectiveH3);

        float[] inputVector = buildInputVector(snapshot);
        System.out.println("DEBUG FEATURES COUNT = " + inputVector.length);

        double pGe1 = predictPositiveProbability(sessionGe1, inputNameGe1, inputVector);
        double pGe2 = predictPositiveProbability(sessionGe2, inputNameGe2, inputVector);

        pGe2 = Math.min(pGe2, pGe1);

        double p0 = Math.max(0.0, 1.0 - pGe1);
        double p1 = Math.max(0.0, pGe1 - pGe2);
        double p2 = Math.max(0.0, pGe2);

        double total = p0 + p1 + p2;
        if (total > 0) {
            p0 /= total;
            p1 /= total;
            p2 /= total;
        }

        int predictionClass = 0;
        if (pGe1 >= mediumThreshold) {
            predictionClass = 1;
        }
        if (pGe2 >= highThreshold) {
            predictionClass = 2;
        }

        double probability;
        String riskLevel;

        if (predictionClass == 2) {
            probability = p2;
            riskLevel = "HIGH";
        } else if (predictionClass == 1) {
            probability = p1;
            riskLevel = "MEDIUM";
        } else {
            probability = p0;
            riskLevel = "LOW";
        }

        PredictionResult result = new PredictionResult();
        result.h3Cell = snapshot.h3Cell;
        result.eventTime = snapshot.eventTime;
        result.eventDate = snapshot.eventDate;
        result.prediction = predictionClass;
        result.probability = round4(probability);
        result.riskLevel = riskLevel;
        result.suspectedVoluntary =
                "fire".equalsIgnoreCase(triggerSource) && predictionClass == 0 ? 1 : 0;

        return result;
    }

    private double predictPositiveProbability(
            OrtSession session,
            String inputName,
            float[] inputVector
    ) throws Exception {

        long[] shape = new long[]{1, inputVector.length};

        try (OnnxTensor inputTensor = OnnxTensor.createTensor(
                ortEnv,
                FloatBuffer.wrap(inputVector),
                shape
        )) {
            Map<String, OnnxTensor> inputs = Collections.singletonMap(inputName, inputTensor);

            try (OrtSession.Result result = session.run(inputs)) {
                return extractPositiveClassProbability(result);
            }
        }
    }

    private double extractPositiveClassProbability(OrtSession.Result result) throws OrtException {
        System.out.println("ONNX OUTPUT COUNT = " + result.size());

        for (Map.Entry<String, OnnxValue> entry : result) {
            String key = entry.getKey();
            OnnxValue value = entry.getValue();

            System.out.println("DEBUG ONNX OUTPUT KEY = " + key + ", VALUE CLASS = " + value.getClass().getName());

            if ("probabilities".equals(key) && value instanceof OnnxSequence) {
                OnnxSequence sequence = (OnnxSequence) value;
                Object seqValue = sequence.getValue();

                System.out.println("DEBUG ONNX SEQUENCE VALUE CLASS = " + seqValue.getClass().getName());

                if (seqValue instanceof List) {
                    List<?> list = (List<?>) seqValue;
                    if (!list.isEmpty()) {
                        Object first = list.get(0);

                        if (first instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<Object, Object> probMap = (Map<Object, Object>) first;

                            Object p1 = probMap.get(1L);
                            if (p1 == null) {
                                p1 = probMap.get(1);
                            }

                            if (p1 instanceof Float) return ((Float) p1).doubleValue();
                            if (p1 instanceof Double) return (Double) p1;
                            if (p1 instanceof Number) return ((Number) p1).doubleValue();

                            throw new RuntimeException("Could not find class 1 probability in ONNX probability map: " + probMap);
                        }

                        if (first instanceof OnnxMap) {
                            @SuppressWarnings("unchecked")
                            Map<Object, Object> probMap = (Map<Object, Object>) ((OnnxMap) first).getValue();

                            Object p1 = probMap.get(1L);
                            if (p1 == null) {
                                p1 = probMap.get(1);
                            }

                            if (p1 instanceof Float) return ((Float) p1).doubleValue();
                            if (p1 instanceof Double) return (Double) p1;
                            if (p1 instanceof Number) return ((Number) p1).doubleValue();

                            throw new RuntimeException("Could not find class 1 probability in ONNX OnnxMap: " + probMap);
                        }
                    }
                }
            }

            if (!(value instanceof OnnxTensor)) {
                continue;
            }

            OnnxTensor tensor = (OnnxTensor) value;
            Object tensorValue = tensor.getValue();

            if (tensorValue == null) {
                continue;
            }

            System.out.println("DEBUG ONNX TENSOR VALUE CLASS = " + tensorValue.getClass().getName());

            if (tensorValue instanceof float[][]) {
                float[][] probs = (float[][]) tensorValue;
                if (probs.length > 0 && probs[0].length >= 2) return probs[0][1];
            }

            if (tensorValue instanceof double[][]) {
                double[][] probs = (double[][]) tensorValue;
                if (probs.length > 0 && probs[0].length >= 2) return probs[0][1];
            }

            if (tensorValue instanceof float[]) {
                float[] probs = (float[]) tensorValue;
                if (probs.length >= 2) return probs[1];
            }

            if (tensorValue instanceof double[]) {
                double[] probs = (double[]) tensorValue;
                if (probs.length >= 2) return probs[1];
            }
        }

        System.err.println("ONNX OUTPUT DEBUG START");
        for (Map.Entry<String, OnnxValue> entry : result) {
            System.err.println("Key: " + entry.getKey() + " -> " + entry.getValue());
        }
        System.err.println("ONNX OUTPUT DEBUG END");

        throw new RuntimeException(
                "Could not extract probability from ONNX output. " +
                "Expected either tensor probabilities or sequence-of-map probabilities."
        );
    }

    private float[] buildInputVector(FeatureSnapshot s) {
        float[] vector = new float[features.size()];

        for (int i = 0; i < features.size(); i++) {
            String feature = features.get(i);
            vector[i] = getFeatureValue(feature, s);
        }

        return vector;
    }

    private float getFeatureValue(String feature, FeatureSnapshot s) {
        switch (feature) {
            case "h3_commune":
                return encodeH3(firstNonBlank(s.h3Commune, s.h3Cell));
            case "h3_center_lat":
                return toFloatSafe(s.h3CenterLat);
            case "h3_center_lon":
                return toFloatSafe(s.h3CenterLon);
            case "altitude_moy":
                return toFloatSafe(s.altitudeMoy);
            case "pente_moy":
                return toFloatSafe(s.penteMoy);
            case "dist_route_m":
                return toFloatSafe(s.distRouteM);
            case "dist_urbain_m":
                return toFloatSafe(s.distUrbainM);
            case "humidity_min(%)":
                return toFloatSafe(s.humidityMin);
            case "temperature_moy(°C)":
                return toFloatSafe(s.temperatureMoy);
            case "wind_speed_max(km/h)":
                return toFloatSafe(s.windSpeedMax);
            case "precipitation_1d_mm":
                return toFloatSafe(s.precipitation1dMm);
            case "precipitation_7d_sum_mm":
                return toFloatSafe(s.precipitation7dSumMm);
            case "soil_moisture":
                return toFloatSafe(s.soilMoisture);
            case "VPD_kPa":
                return toFloatSafe(s.vpdKpa);
            case "NDVI_30d_mean":
                return toFloatSafe(s.ndvi30dMean);
            case "NDVI_30d_min":
                return toFloatSafe(s.ndvi30dMin);
            case "NDVI_30d_std":
                return toFloatSafe(s.ndvi30dStd);
            case "month":
                return toFloatSafe(s.month);
            case "month_sin":
                return toFloatSafe(s.monthSin);
            case "month_cos":
                return toFloatSafe(s.monthCos);
            case "doy_sin":
                return toFloatSafe(s.doySin);
            case "doy_cos":
                return toFloatSafe(s.doyCos);
            case "is_weekend":
                return toFloatSafe(s.isWeekend);
            default:
                throw new RuntimeException("Unsupported feature in ONNX feature list: " + feature);
        }
    }

    private float encodeH3(String h3Value) {
        if (isBlank(h3Value)) {
            throw new RuntimeException("Missing h3_commune / h3Cell for encoding");
        }

        Integer encoded = h3EncoderMap.get(h3Value);
        if (encoded == null) {
            throw new RuntimeException("Unknown H3 value for encoder: " + h3Value);
        }

        return encoded.floatValue();
    }

    private void normalizeH3(FeatureSnapshot s) {
        if (s == null) {
            return;
        }

        if (isBlank(s.h3Commune) && !isBlank(s.h3Cell)) {
            s.h3Commune = s.h3Cell;
        }

        if (isBlank(s.h3Cell) && !isBlank(s.h3Commune)) {
            s.h3Cell = s.h3Commune;
        }
    }

    private float toFloatSafe(Number value) {
        return value == null ? 0.0f : value.floatValue();
    }

    private String firstNonBlank(String first, String fallback) {
        if (!isBlank(first)) {
            return first;
        }
        return fallback;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private double round4(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }

    private Map<String, Integer> buildEncoderMap(List<String> encoderClasses) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < encoderClasses.size(); i++) {
            map.put(encoderClasses.get(i), i);
        }
        return map;
    }

    private List<String> loadStringListResource(String resourceName) throws IOException {
        try (InputStream is = getRequiredResource(resourceName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

            Type listType = new TypeToken<List<String>>() {}.getType();
            List<String> values = gson.fromJson(reader, listType);

            if (values == null || values.isEmpty()) {
                throw new RuntimeException("Resource is empty: " + resourceName);
            }

            return values;
        }
    }

    private Map<String, List<String>> loadEncodersResource(String resourceName) throws IOException {
        try (InputStream is = getRequiredResource(resourceName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

            Type mapType = new TypeToken<Map<String, List<String>>>() {}.getType();
            Map<String, List<String>> values = gson.fromJson(reader, mapType);

            if (values == null || values.isEmpty()) {
                throw new RuntimeException("Encoder resource is empty: " + resourceName);
            }

            return values;
        }
    }

    private Map<String, Double> loadThresholdsResource(String resourceName) throws IOException {
        try (InputStream is = getRequiredResource(resourceName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

            Type mapType = new TypeToken<Map<String, Double>>() {}.getType();
            Map<String, Double> values = gson.fromJson(reader, mapType);

            if (values == null || values.isEmpty()) {
                throw new RuntimeException("Threshold resource is empty: " + resourceName);
            }

            return values;
        }
    }

    private byte[] readResourceBytes(String resourceName) throws IOException {
        try (InputStream is = getRequiredResource(resourceName)) {
            return is.readAllBytes();
        }
    }

    private InputStream getRequiredResource(String resourceName) {
        InputStream is = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(resourceName);

        if (is == null) {
            throw new RuntimeException("Resource not found on classpath: " + resourceName);
        }

        return is;
    }

    @Override
    public void close() throws Exception {
        closeQuietly(sessionGe1);
        closeQuietly(sessionGe2);
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (Objects.nonNull(closeable)) {
            try {
                closeable.close();
            } catch (Exception ignored) {
            }
        }
    }
}