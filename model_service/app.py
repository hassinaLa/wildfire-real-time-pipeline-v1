from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
from predictor_neighbors import OrdinalLightGBMPredictor

app = FastAPI(title="Wildfire Risk Model Service")

FEATURES_PATH = "lightgbm_neighbors_only_features.pkl"
ENCODERS_PATH = "lightgbm_neighbors_only_encoders.pkl"
THRESHOLDS_PATH = "lightgbm_neighbors_only_thresholds.pkl"
MODEL_Y_GE_1_PATH = "lightgbm_neighbors_only_y_ge_1.pkl"
MODEL_Y_GE_2_PATH = "lightgbm_neighbors_only_y_ge_2.pkl"

expected_features = joblib.load(FEATURES_PATH)
encoders = joblib.load(ENCODERS_PATH)

predictor = OrdinalLightGBMPredictor(
    model1_path=MODEL_Y_GE_1_PATH,
    model2_path=MODEL_Y_GE_2_PATH,
    features_path=FEATURES_PATH,
    encoders_path=ENCODERS_PATH,
    thresholds_path=THRESHOLDS_PATH,
)

FEATURE_ALIASES = {
    "temperature_moy": "temperature_moy(°C)",
    "humidity_min": "humidity_min(%)",
    "wind_speed_max": "wind_speed_max(km/h)",
    "precipitation_1d_mm": "precipitation_1d_mm",
    "precipitation_7d_sum_mm": "precipitation_7d_sum_mm",
    "soil_moisture": "soil_moisture",
    "VPD_kPa": "VPD_kPa",
    "NDVI_30d_mean": "NDVI_30d_mean",
    "NDVI_30d_min": "NDVI_30d_min",
    "NDVI_30d_std": "NDVI_30d_std",
    "h3_commune": "h3_commune",
    "h3_center_lat": "h3_center_lat",
    "h3_center_lon": "h3_center_lon",
    "altitude_moy": "altitude_moy",
    "pente_moy": "pente_moy",
    "dist_route_m": "dist_route_m",
    "dist_urbain_m": "dist_urbain_m",
    "month": "month",
    "month_sin": "month_sin",
    "month_cos": "month_cos",
    "doy_sin": "doy_sin",
    "doy_cos": "doy_cos",
    "is_weekend": "is_weekend",
    "fires_cell_last_7d": "fires_cell_last_7d",
    "fires_cell_last_30d": "fires_cell_last_30d",
    "days_since_last_fire": "days_since_last_fire",
    "seasonal_fire_count": "seasonal_fire_count",
    "never_burned_before": "never_burned_before",
}


class PredictionInput(BaseModel):
    data: dict


def normalize_feature_names(data: dict) -> dict:
    normalized = dict(data)

    for src, dst in FEATURE_ALIASES.items():
        if src in normalized and dst not in normalized:
            normalized[dst] = normalized[src]

    return normalized


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/features")
def features():
    return {
        "expected_features": list(expected_features),
        "count": len(expected_features),
    }


@app.post("/predict")
def predict(payload: PredictionInput):
    data = payload.data
    if not isinstance(data, dict):
        raise HTTPException(
            status_code=400,
            detail={"error": "Payload field 'data' must be a JSON object"}
        )

    data = normalize_feature_names(data)

    missing_features = [f for f in expected_features if f not in data]
    if missing_features:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Missing required features",
                "missing_features": missing_features,
            },
        )

    # إذا كان عندك categorical columns داخل encoders
    for col, encoder in encoders.items():
        if col in data:
            value = str(data[col])
            known_classes = set(encoder.classes_)
            if value not in known_classes:
                data[col] = "Unknown" if "Unknown" in known_classes else str(encoder.classes_[0])

    try:
        prediction = predictor.predict(data)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "Prediction failed", "message": str(e)},
        )

    return {
        "prediction_status": "ok",
        "prediction": prediction,
    }