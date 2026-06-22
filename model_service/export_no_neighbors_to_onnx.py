import json
from pathlib import Path
import joblib

BASE = Path(__file__).resolve().parent

FEATURES_PKL = BASE / "lightgbm_no_neighbors_features.pkl"
ENCODERS_PKL = BASE / "lightgbm_no_neighbors_encoders.pkl"
THRESHOLDS_PKL = BASE / "lightgbm_no_neighbors_thresholds.pkl"
MODEL1_PKL = BASE / "lightgbm_no_neighbors_y_ge_1.pkl"
MODEL2_PKL = BASE / "lightgbm_no_neighbors_y_ge_2.pkl"

OUT_MODEL1 = BASE / "model_ge_1_no_neighbors.onnx"
OUT_MODEL2 = BASE / "model_ge_2_no_neighbors.onnx"
OUT_FEATURES_JSON = BASE / "model_features_no_neighbors.json"
OUT_ENCODERS_JSON = BASE / "encoders_no_neighbors.json"
OUT_THRESHOLDS_JSON = BASE / "thresholds_no_neighbors.json"


def load_obj(path: Path):
    return joblib.load(path)


def save_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def export_lightgbm_to_onnx(model, out_path: Path, n_features: int):
    from onnxmltools import convert_lightgbm
    from onnxmltools.convert.common.data_types import FloatTensorType

    initial_types = [("input", FloatTensorType([None, n_features]))]
    onnx_model = convert_lightgbm(model, initial_types=initial_types)

    with open(out_path, "wb") as f:
        f.write(onnx_model.SerializeToString())


def main():
    features = load_obj(FEATURES_PKL)
    encoders = load_obj(ENCODERS_PKL)
    thresholds = load_obj(THRESHOLDS_PKL)
    model1 = load_obj(MODEL1_PKL)
    model2 = load_obj(MODEL2_PKL)

    if not isinstance(features, list):
        features = list(features)

    encoders_json = {}
    for col, encoder in encoders.items():
        if hasattr(encoder, "classes_"):
            encoders_json[col] = [str(x) for x in encoder.classes_]
        else:
            raise ValueError(f"Encoder for column {col} does not expose classes_")

    print("Loaded features:", len(features))
    print("Loaded encoders:", len(encoders_json))
    print("Loaded thresholds:", thresholds)

    export_lightgbm_to_onnx(model1, OUT_MODEL1, len(features))
    export_lightgbm_to_onnx(model2, OUT_MODEL2, len(features))

    save_json(OUT_FEATURES_JSON, features)
    save_json(OUT_ENCODERS_JSON, encoders_json)
    save_json(OUT_THRESHOLDS_JSON, thresholds)

    print("Saved:", OUT_MODEL1)
    print("Saved:", OUT_MODEL2)
    print("Saved:", OUT_FEATURES_JSON)
    print("Saved:", OUT_ENCODERS_JSON)
    print("Saved:", OUT_THRESHOLDS_JSON)


if __name__ == "__main__":
    main()
