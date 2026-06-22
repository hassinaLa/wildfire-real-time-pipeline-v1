import joblib
import numpy as np
import pandas as pd


class OrdinalLightGBMPredictor:
    def __init__(
        self,
        model1_path,
        model2_path,
        features_path,
        encoders_path,
        thresholds_path
    ):
        print("Loading models...")

        self.model_ge_1 = joblib.load(model1_path)
        self.model_ge_2 = joblib.load(model2_path)
        self.features = joblib.load(features_path)
        self.encoders = joblib.load(encoders_path)
        self.thresholds = joblib.load(thresholds_path)

        self.medium_threshold = self.thresholds["MEDIUM_THRESHOLD"]
        self.high_threshold = self.thresholds["HIGH_THRESHOLD"]

        print("Models loaded successfully")
        print(f"MEDIUM_THRESHOLD = {self.medium_threshold}")
        print(f"HIGH_THRESHOLD   = {self.high_threshold}")

    def preprocess_input(self, input_dict):
        row = {}

        for feature in self.features:
            if feature not in input_dict:
                raise ValueError(f"Missing required feature: {feature}")
            row[feature] = input_dict[feature]

        df = pd.DataFrame([row], columns=self.features)

        for col, encoder in self.encoders.items():
            if col in df.columns:
                value = str(df.at[0, col])

                known_classes = set(encoder.classes_)
                if value not in known_classes:
                    if "Unknown" in known_classes:
                        value = "Unknown"
                    else:
                        value = encoder.classes_[0]

                df[col] = encoder.transform([value])

        return df

    def predict_one(self, input_dict):
        X = self.preprocess_input(input_dict)

        p_ge_1 = float(self.model_ge_1.predict_proba(X)[0][1])
        p_ge_2 = float(self.model_ge_2.predict_proba(X)[0][1])

        p_ge_2 = min(p_ge_2, p_ge_1)

        p0 = max(0.0, 1.0 - p_ge_1)
        p1 = max(0.0, p_ge_1 - p_ge_2)
        p2 = max(0.0, p_ge_2)

        total = p0 + p1 + p2
        if total > 0:
            p0 /= total
            p1 /= total
            p2 /= total

        risk_level = 0
        if p_ge_1 >= self.medium_threshold:
            risk_level = 1
        if p_ge_2 >= self.high_threshold:
            risk_level = 2

        labels = {0: "low", 1: "medium", 2: "high"}

        return {
            "risk_level": int(risk_level),
            "risk_label": labels[int(risk_level)],
            "scores": {
                "p_ge_1": float(p_ge_1),
                "p_ge_2": float(p_ge_2)
            },
            "probabilities": {
                "class_0": float(p0),
                "class_1": float(p1),
                "class_2": float(p2)
            },
            "thresholds": {
                "medium_threshold": float(self.medium_threshold),
                "high_threshold": float(self.high_threshold)
            }
        }