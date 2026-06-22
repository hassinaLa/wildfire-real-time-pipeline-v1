package com.environment.simulator;

import com.environment.simulator.model.MeteoEvent;
import com.environment.simulator.util.EventTimePolicy;
import com.environment.simulator.util.ReplayClock;
import com.environment.simulator.util.TimeMode;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class MeteoApiProducer {

    private static final String TOPIC = "meteo-events";

    // Historical archive API
    private static final String ARCHIVE_API_BASE = "https://archive-api.open-meteo.com/v1/archive";

    // Live / forecast API
    private static final String FORECAST_API_BASE = "https://api.open-meteo.com/v1/forecast";

    private static final Gson GSON = new Gson();

    private static final LocalDate START_DATE = LocalDate.of(2025, 5, 1);
    private static final LocalDate END_DATE = LocalDate.of(2025, 10, 31);

    private static final long PER_POINT_DELAY_MS = 0L;
    private static final long REPLAY_DELAY_MS = 1000L;

    private static final long API_COOLDOWN_MS = 30 * 60 * 1000L;

    // 3 fixed meteo slots per day
    private static final int[] DAILY_HOURS = {8, 14, 20};

    private final KafkaProducer<String, String> producer;
    private final String kafkaBootstrap;
    private final List<AllowedH3Loader.PointLocation> points;

    private final TimeMode timeMode;
    private final EventTimePolicy timePolicy;
    private final ReplayClock replayClock;

    // cache for historical archive responses only
    private final Map<String, String> responseCache = new HashMap<>();
    private long apiCooldownUntil = 0L;

    public MeteoApiProducer() throws Exception {
        this.timeMode = EventTimePolicy.readModeFromEnv();
        this.timePolicy = new EventTimePolicy(timeMode);
        this.replayClock = timePolicy.isHistorical2025() ? timePolicy.newReplayClock() : null;

        this.kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        this.points = AllowedH3Loader.load();

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBootstrap);
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
    }

    public void startProducing() throws Exception {
        System.out.println("Starting MeteoApiProducer on " + kafkaBootstrap);
        System.out.println("Loaded allowed H3 points: " + points.size());
        System.out.println("MeteoApiProducer TIME_MODE=" + timeMode);

        if (timePolicy.isLive()) {
            startLiveProducing();
        } else {
            startHistoricalProducing();
        }
    }

    private void startLiveProducing() throws Exception {
        while (true) {
            for (AllowedH3Loader.PointLocation point : points) {
                sendLiveApiMeteo(point);
                Thread.sleep(PER_POINT_DELAY_MS);
            }

            producer.flush();
            System.out.println("Finished one live meteo API cycle. Waiting before next cycle...");
            Thread.sleep(REPLAY_DELAY_MS);
        }
    }

    private void startHistoricalProducing() throws Exception {
        System.out.println("Historical meteo replay range: " + START_DATE + " -> " + END_DATE);

        while (true) {
            LocalDate replayDate = replayClock.currentDate();
            System.out.println("MeteoApiProducer replayDate=" + replayDate);

            for (AllowedH3Loader.PointLocation point : points) {
                sendHistoricalDailyMeteoForDate(point, replayDate);
                Thread.sleep(PER_POINT_DELAY_MS);
            }

            producer.flush();
            replayClock.advanceDay();

            System.out.println("Finished one historical meteo replay day. Waiting before next replay day...");
            Thread.sleep(REPLAY_DELAY_MS);
        }
    }

    private void sendLiveApiMeteo(AllowedH3Loader.PointLocation point) {
        try {
            String urlStr = FORECAST_API_BASE
                    + "?latitude=" + point.getLat()
                    + "&longitude=" + point.getLon()
                    + "&daily="
                    + "temperature_2m_mean,"
                    + "relative_humidity_2m_min,"
                    + "wind_speed_10m_max,"
                    + "precipitation_sum,"
                    + "vapour_pressure_deficit_max"
                    + "&hourly=soil_moisture_0_to_1cm"
                    + "&timezone=Africa%2FAlgiers"
                    + "&forecast_days=1";

            String response = doGet(urlStr);

            if (response == null) {
                System.err.println("Live meteo API failed for h3=" + point.getH3Cell() + ". Using live fallback.");
                sendThreeLiveFallbackEvents(point, LocalDate.now(ZoneId.of("Africa/Algiers")));
                return;
            }

            JsonObject root = GSON.fromJson(response, JsonObject.class);

            if (root == null || !root.has("daily")) {
                System.err.println("No live daily meteo data for h3=" + point.getH3Cell() + ". Using live fallback.");
                sendThreeLiveFallbackEvents(point, LocalDate.now(ZoneId.of("Africa/Algiers")));
                return;
            }

            JsonObject daily = root.getAsJsonObject("daily");
            JsonArray dates = daily.getAsJsonArray("time");
            JsonArray temperatureMean = daily.getAsJsonArray("temperature_2m_mean");
            JsonArray humidityMin = daily.getAsJsonArray("relative_humidity_2m_min");
            JsonArray windMax = daily.getAsJsonArray("wind_speed_10m_max");
            JsonArray precipitationSum = daily.getAsJsonArray("precipitation_sum");
            JsonArray vpdMax = daily.getAsJsonArray("vapour_pressure_deficit_max");

            JsonObject hourly = root.has("hourly") ? root.getAsJsonObject("hourly") : null;
            JsonArray hourlyTimes = hourly != null && hourly.has("time") ? hourly.getAsJsonArray("time") : null;
            JsonArray hourlySoil = hourly != null && hourly.has("soil_moisture_0_to_1cm")
                    ? hourly.getAsJsonArray("soil_moisture_0_to_1cm") : null;

            if (dates == null || dates.size() == 0) {
                System.err.println("Empty live meteo data for h3=" + point.getH3Cell() + ". Using live fallback.");
                sendThreeLiveFallbackEvents(point, LocalDate.now(ZoneId.of("Africa/Algiers")));
                return;
            }

            String eventDate = dates.get(0).getAsString();
            LocalDate liveDate = LocalDate.parse(eventDate);

            Double temperature = getNullableDouble(temperatureMean, 0);
            Double humidity = getNullableDouble(humidityMin, 0);
            Double windSpeed = getNullableDouble(windMax, 0);
            Double precipitation = getNullableDouble(precipitationSum, 0);
            Double soilMoisture = avgSoilMoistureForDay(eventDate, hourlyTimes, hourlySoil);
            Double vpd = getNullableDouble(vpdMax, 0);

            sendThreeEventsForDay(
                    point,
                    liveDate,
                    temperature,
                    humidity,
                    windSpeed,
                    precipitation,
                    soilMoisture,
                    vpd
            );

        } catch (Exception e) {
            System.err.println("Error sending live API meteo for h3=" + point.getH3Cell() + ". Using live fallback.");
            e.printStackTrace();
            sendThreeLiveFallbackEvents(point, LocalDate.now(ZoneId.of("Africa/Algiers")));
        }
    }

    private void sendHistoricalDailyMeteoForDate(AllowedH3Loader.PointLocation point, LocalDate replayDate) {
        try {
            String response = null;

            if (responseCache.containsKey(point.getH3Cell())) {
                response = responseCache.get(point.getH3Cell());
                System.out.println("Using cached historical meteo response for h3=" + point.getH3Cell());
            } else {
                if (isApiInCooldown()) {
                    System.err.println("Archive API in cooldown. Using historical fallback for h3=" + point.getH3Cell());
                    sendThreeHistoricalFallbackEvents(point, replayDate);
                    return;
                }

                String urlStr = ARCHIVE_API_BASE
                        + "?latitude=" + point.getLat()
                        + "&longitude=" + point.getLon()
                        + "&start_date=" + START_DATE
                        + "&end_date=" + END_DATE
                        + "&daily="
                        + "temperature_2m_mean,"
                        + "relative_humidity_2m_min,"
                        + "wind_speed_10m_max,"
                        + "precipitation_sum,"
                        + "vapour_pressure_deficit_max"
                        + "&hourly=soil_moisture_0_to_1cm"
                        + "&timezone=Africa%2FAlgiers";

                response = doGet(urlStr);

                if (response != null) {
                    responseCache.put(point.getH3Cell(), response);
                }
            }

            if (response == null) {
                System.err.println("Using historical fallback meteo data for h3=" + point.getH3Cell());
                sendThreeHistoricalFallbackEvents(point, replayDate);
                return;
            }

            JsonObject root = GSON.fromJson(response, JsonObject.class);

            if (root == null || !root.has("daily")) {
                System.err.println("No historical daily meteo data for h3=" + point.getH3Cell() + ". Using historical fallback.");
                sendThreeHistoricalFallbackEvents(point, replayDate);
                return;
            }

            JsonObject daily = root.getAsJsonObject("daily");
            JsonArray dates = daily.getAsJsonArray("time");
            JsonArray temperatureMean = daily.getAsJsonArray("temperature_2m_mean");
            JsonArray humidityMin = daily.getAsJsonArray("relative_humidity_2m_min");
            JsonArray windMax = daily.getAsJsonArray("wind_speed_10m_max");
            JsonArray precipitationSum = daily.getAsJsonArray("precipitation_sum");
            JsonArray vpdMax = daily.getAsJsonArray("vapour_pressure_deficit_max");

            JsonObject hourly = root.has("hourly") ? root.getAsJsonObject("hourly") : null;
            JsonArray hourlyTimes = hourly != null && hourly.has("time") ? hourly.getAsJsonArray("time") : null;
            JsonArray hourlySoil = hourly != null && hourly.has("soil_moisture_0_to_1cm")
                    ? hourly.getAsJsonArray("soil_moisture_0_to_1cm") : null;

            if (dates == null || dates.size() == 0) {
                System.err.println("Empty historical meteo data for h3=" + point.getH3Cell() + ". Using historical fallback.");
                sendThreeHistoricalFallbackEvents(point, replayDate);
                return;
            }

            int index = findDateIndex(dates, replayDate.toString());
            if (index < 0) {
                System.err.println("Replay date " + replayDate + " not found in meteo API response for h3=" + point.getH3Cell());
                sendThreeHistoricalFallbackEvents(point, replayDate);
                return;
            }

            Double temperature = getNullableDouble(temperatureMean, index);
            Double humidity = getNullableDouble(humidityMin, index);
            Double windSpeed = getNullableDouble(windMax, index);
            Double precipitation = getNullableDouble(precipitationSum, index);
            Double soilMoisture = avgSoilMoistureForDay(replayDate.toString(), hourlyTimes, hourlySoil);
            Double vpd = getNullableDouble(vpdMax, index);

            sendThreeEventsForDay(
                    point,
                    replayDate,
                    temperature,
                    humidity,
                    windSpeed,
                    precipitation,
                    soilMoisture,
                    vpd
            );

        } catch (Exception e) {
            System.err.println("Error fetching historical meteo for h3=" + point.getH3Cell() + ". Using historical fallback.");
            e.printStackTrace();
            sendThreeHistoricalFallbackEvents(point, replayDate);
        }
    }

    private long buildSlotTimestamp(LocalDate date, int hour) {
        return date.atTime(hour, 0)
                .atZone(ZoneId.of("Africa/Algiers"))
                .toInstant()
                .toEpochMilli();
    }

    private void sendThreeEventsForDay(
            AllowedH3Loader.PointLocation point,
            LocalDate date,
            Double temperature,
            Double humidity,
            Double windSpeed,
            Double precipitation,
            Double soilMoisture,
            Double vpd
    ) {
        for (int hour : DAILY_HOURS) {
            try {
                MeteoEvent event = new MeteoEvent();

                event.setH3Cell(point.getH3Cell());
                event.setLat(point.getLat());
                event.setLon(point.getLon());

                event.setEventDate(date.toString());
                event.setEventTime(buildSlotTimestamp(date, hour));

                event.setTemperature(temperature);
                event.setHumidity(humidity);
                event.setWindSpeed(windSpeed);
                event.setPrecipitation(precipitation);
                event.setSoilMoisture(soilMoisture);
                event.setVpd(vpd);

                String json = GSON.toJson(event);
                producer.send(new ProducerRecord<>(TOPIC, point.getH3Cell(), json));

                System.out.println("Meteo slot " + hour + ":00 sent -> " + json);

            } catch (Exception e) {
                System.err.println("Error sending slot " + hour + " for h3=" + point.getH3Cell());
                e.printStackTrace();
            }
        }
    }

    private void sendThreeLiveFallbackEvents(AllowedH3Loader.PointLocation point, LocalDate date) {
        sendThreeEventsForDay(
                point,
                date,
                round(randomBetween(26.0, 38.0)),
                round(randomBetween(15.0, 45.0)),
                round(randomBetween(4.0, 22.0)),
                round(randomBetween(0.0, 3.0)),
                round(randomBetween(0.08, 0.35)),
                round(randomBetween(1.0, 4.0))
        );
    }

    private void sendThreeHistoricalFallbackEvents(AllowedH3Loader.PointLocation point, LocalDate replayDate) {
        sendThreeEventsForDay(
                point,
                replayDate,
                round(randomBetween(26.0, 34.0)),
                round(randomBetween(20.0, 45.0)),
                round(randomBetween(5.0, 20.0)),
                round(randomBetween(0.0, 2.0)),
                round(randomBetween(0.10, 0.35)),
                round(randomBetween(1.0, 3.5))
        );
    }

    private boolean isApiInCooldown() {
        return System.currentTimeMillis() < apiCooldownUntil;
    }

    private void activateApiCooldown() {
        apiCooldownUntil = System.currentTimeMillis() + API_COOLDOWN_MS;
        System.err.println("Meteo API cooldown activated until " + apiCooldownUntil);
    }

    private int findDateIndex(JsonArray dates, String targetDate) {
        for (int i = 0; i < dates.size(); i++) {
            if (targetDate.equals(dates.get(i).getAsString())) {
                return i;
            }
        }
        return -1;
    }

    private double randomBetween(double min, double max) {
        return ThreadLocalRandom.current().nextDouble(min, max);
    }

    private Double avgSoilMoistureForDay(String date, JsonArray hourlyTimes, JsonArray hourlySoil) {
        if (hourlyTimes == null || hourlySoil == null) return null;

        double sum = 0.0;
        int count = 0;

        for (int i = 0; i < hourlyTimes.size() && i < hourlySoil.size(); i++) {
            String ts = hourlyTimes.get(i).getAsString();
            if (ts != null && ts.startsWith(date) && !hourlySoil.get(i).isJsonNull()) {
                sum += hourlySoil.get(i).getAsDouble();
                count++;
            }
        }

        return count == 0 ? null : round(sum / count);
    }

    private Double getNullableDouble(JsonArray array, int index) {
        if (array == null || index < 0 || index >= array.size() || array.get(index).isJsonNull()) {
            return null;
        }
        return round(array.get(index).getAsDouble());
    }

    private double round(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }

    private String doGet(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setConnectTimeout(15000);
        con.setReadTimeout(30000);

        int status = con.getResponseCode();

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        status >= 200 && status < 300
                                ? con.getInputStream()
                                : con.getErrorStream()
                )
        );

        StringBuilder response = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            response.append(line);
        }

        reader.close();
        con.disconnect();

        if (status == 429) {
            System.err.println("Meteo API quota exceeded (429). Falling back.");
            activateApiCooldown();
            return null;
        }

        if (status < 200 || status >= 300) {
            System.err.println("Meteo API error: HTTP " + status + " -> " + response);
            return null;
        }

        return response.toString();
    }

    public void close() {
        producer.close();
    }
}