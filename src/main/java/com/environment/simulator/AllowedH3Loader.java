package com.environment.simulator;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class AllowedH3Loader {

    public static List<PointLocation> load() throws Exception {
        List<PointLocation> points = new ArrayList<>();

        InputStream is = AllowedH3Loader.class.getClassLoader()
                .getResourceAsStream("allowed_h3_cells.csv");

        if (is == null) {
            throw new IllegalStateException("h3_cells_forest_only.csv not found in src/main/resources");
        }

        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line = br.readLine(); // header
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                String[] p = line.split(",");
                if (p.length < 3) continue;

                String h3Cell = p[0].trim();
                double lat = Double.parseDouble(p[1].trim());
                double lon = Double.parseDouble(p[2].trim());

                points.add(new PointLocation(h3Cell, lat, lon));
            }
        }

        if (points.isEmpty()) {
            throw new IllegalStateException("No rows found in h3_cells_forest_only.csv");
        }

        return points;
    }

    public static class PointLocation {
        private final String h3Cell;
        private final double lat;
        private final double lon;

        public PointLocation(String h3Cell, double lat, double lon) {
            this.h3Cell = h3Cell;
            this.lat = lat;
            this.lon = lon;
        }

        public String getH3Cell() {
            return h3Cell;
        }

        public double getLat() {
            return lat;
        }

        public double getLon() {
            return lon;
        }
    }
}