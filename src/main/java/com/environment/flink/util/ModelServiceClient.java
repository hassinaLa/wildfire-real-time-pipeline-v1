package com.environment.flink.util;

import com.environment.flink.model.FeatureSnapshot;
import com.environment.flink.model.PredictionResult;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public final class ModelServiceClient {

    private static final Gson GSON = new Gson();

    private ModelServiceClient() {
    }

    public static PredictionResult predict(
            FeatureSnapshot snapshot,
            String modelServiceUrl,
            String triggerSource
    ) throws Exception {

        HttpURLConnection connection = null;

        try {
            URL url = new URL(modelServiceUrl);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(10000);
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");

            String requestBody = toWrappedModelRequestJson(snapshot);

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = requestBody.getBytes(StandardCharsets.UTF_8);
                os.write(input);
            }

            int status = connection.getResponseCode();
            String responseBody = readResponseBody(
                    status >= 200 && status < 300
                            ? connection.getInputStream()
                            : connection.getErrorStream()
            );

            if (status < 200 || status >= 300) {
                throw new RuntimeException(
                        "Model service error. HTTP " + status + " body=" + responseBody
                );
            }

            JsonObject json = GSON.fromJson(responseBody, JsonObject.class);
            if (json == null) {
                throw new RuntimeException("Model service returned empty JSON");
            }

            String predictionStatus = getAsString(json, "prediction_status");
            Boolean knownH3 = getAsBoolean(json, "known_h3");

            if ("rejected_unknown_h3".equals(predictionStatus) || Boolean.FALSE.equals(knownH3)) {
                PredictionResult rejected = new PredictionResult();
                rejected.h3Cell = snapshot.h3Cell;
                rejected.eventTime = snapshot.eventTime;
                rejected.eventDate = snapshot.eventDate;
                rejected.prediction = 0;
                rejected.probability = 0.0;
                rejected.riskLevel = "REJECTED_UNKNOWN_H3";
                rejected.suspectedVoluntary = 0;
                return rejected;
            }

            JsonObject prediction = json.has("prediction") && json.get("prediction").isJsonObject()
                    ? json.getAsJsonObject("prediction")
                    : null;

            if (prediction == null) {
                throw new RuntimeException("Missing 'prediction' object in model response: " + responseBody);
            }

            Integer riskLevelInt = getAsInteger(prediction, "risk_level");
            JsonObject probabilities = prediction.has("probabilities") && prediction.get("probabilities").isJsonObject()
                    ? prediction.getAsJsonObject("probabilities")
                    : null;

            double p0 = probabilities != null ? getAsDouble(probabilities, "class_0", 0.0) : 0.0;
            double p1 = probabilities != null ? getAsDouble(probabilities, "class_1", 0.0) : 0.0;
            double p2 = probabilities != null ? getAsDouble(probabilities, "class_2", 0.0) : 0.0;

            int predictionClass = (riskLevelInt == null) ? argmax(p0, p1, p2) : riskLevelInt;

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

        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    public static String toWrappedModelRequestJson(FeatureSnapshot s) {
        JsonObject data = new JsonObject();

        String effectiveH3Commune = firstNonBlank(s.h3Commune, s.h3Cell);

        addString(data, "h3_commune", effectiveH3Commune);
        addNumber(data, "h3_center_lat", s.h3CenterLat);
        addNumber(data, "h3_center_lon", s.h3CenterLon);
        addNumber(data, "altitude_moy", s.altitudeMoy);
        addNumber(data, "pente_moy", s.penteMoy);
        addNumber(data, "dist_route_m", s.distRouteM);
        addNumber(data, "dist_urbain_m", s.distUrbainM);

        addNumber(data, "humidity_min(%)", s.humidityMin);
        addNumber(data, "temperature_moy(°C)", s.temperatureMoy);
        addNumber(data, "wind_speed_max(km/h)", s.windSpeedMax);
        addNumber(data, "precipitation_1d_mm", s.precipitation1dMm);
        addNumber(data, "precipitation_7d_sum_mm", s.precipitation7dSumMm);
        addNumber(data, "soil_moisture", s.soilMoisture);
        addNumber(data, "VPD_kPa", s.vpdKpa);
        addNumber(data, "NDVI_30d_mean", s.ndvi30dMean);
        addNumber(data, "NDVI_30d_min", s.ndvi30dMin);
        addNumber(data, "NDVI_30d_std", s.ndvi30dStd);

        addInteger(data, "month", s.month);
        addNumber(data, "month_sin", s.monthSin);
        addNumber(data, "month_cos", s.monthCos);
        addNumber(data, "doy_sin", s.doySin);
        addNumber(data, "doy_cos", s.doyCos);
        addInteger(data, "is_weekend", s.isWeekend);

        JsonObject wrapper = new JsonObject();
        wrapper.add("data", data);

        return GSON.toJson(wrapper);
    }

    private static String firstNonBlank(String first, String fallback) {
        if (first != null && !first.trim().isEmpty()) {
            return first;
        }
        if (fallback != null && !fallback.trim().isEmpty()) {
            return fallback;
        }
        return null;
    }

    private static void addString(JsonObject json, String key, String value) {
        if (value == null) {
            json.add(key, null);
        } else {
            json.addProperty(key, value);
        }
    }

    private static void addNumber(JsonObject json, String key, Double value) {
        if (value == null) {
            json.add(key, null);
        } else {
            json.addProperty(key, value);
        }
    }

    private static void addInteger(JsonObject json, String key, Integer value) {
        if (value == null) {
            json.add(key, null);
        } else {
            json.addProperty(key, value);
        }
    }

    private static String readResponseBody(InputStream inputStream) throws Exception {
        if (inputStream == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }

    private static String getAsString(JsonObject json, String key) {
        try {
            JsonElement e = json.get(key);
            return e != null && !e.isJsonNull() ? e.getAsString() : null;
        } catch (Exception ex) {
            return null;
        }
    }

    private static Boolean getAsBoolean(JsonObject json, String key) {
        try {
            JsonElement e = json.get(key);
            return e != null && !e.isJsonNull() ? e.getAsBoolean() : null;
        } catch (Exception ex) {
            return null;
        }
    }

    private static Integer getAsInteger(JsonObject json, String key) {
        try {
            JsonElement e = json.get(key);
            return e != null && !e.isJsonNull() ? e.getAsInt() : null;
        } catch (Exception ex) {
            return null;
        }
    }

    private static double getAsDouble(JsonObject json, String key, double defaultValue) {
        try {
            JsonElement e = json.get(key);
            return e != null && !e.isJsonNull() ? e.getAsDouble() : defaultValue;
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    private static int argmax(double p0, double p1, double p2) {
        if (p2 >= p1 && p2 >= p0) {
            return 2;
        }
        if (p1 >= p0) {
            return 1;
        }
        return 0;
    }

    private static double round4(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }
}