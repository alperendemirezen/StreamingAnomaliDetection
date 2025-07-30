# features.py
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import joblib
import os

encoder_path = "combo_encoder.pkl"

def fit_encoder(combo_series):
    """Yeni bir encoder oluşturup kaydeder."""
    encoder = LabelEncoder()
    encoder.fit(combo_series)
    joblib.dump(encoder, encoder_path)
    return encoder

def load_encoder():
    """Varolan encoder'ı yükler."""
    if os.path.exists(encoder_path):
        return joblib.load(encoder_path)
    else:
        raise FileNotFoundError("combo_encoder.pkl not found. Train first.")

def extract_features(df_raw: pd.DataFrame, encoder=None) -> pd.DataFrame:
    df = df_raw.copy()

    # usage_amt temizleme
    df['usage_amt'] = df['usage_amt'].astype(str).str.lstrip("0").replace('', '0')
    df['amount'] = df['usage_amt'].astype(float)

    # combo özelliği
    df['combo'] = df['route_code'].astype(str) + "_" + df['customer_flag'].astype(str)

    if encoder is not None:
        df['combo_encoded'] = encoder.transform(df['combo'])
    else:
        df['combo_encoded'] = df['combo'].astype('category').cat.codes

    return df[['combo_encoded', 'amount']]
