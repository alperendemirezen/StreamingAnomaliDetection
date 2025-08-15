import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import oracledb
from datetime import datetime, timedelta
import time
import numpy as np
import json
import os
from contextlib import contextmanager

import config_loader

# Sayfa konfigürasyonu
st.set_page_config(
    page_title="🚌 Transit Anomaly Dashboard",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS ile modern görünüm
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        margin: 0.5rem;
    }
    .status-good { color: #28a745; font-weight: bold; }
    .status-warning { color: #ffc107; font-weight: bold; }
    .status-critical { color: #dc3545; font-weight: bold; }
</style>
""", unsafe_allow_html=True)


# Oracle bağlantısı için context manager
@contextmanager
def get_db_connection():
    """Oracle veritabanı bağlantısını güvenli şekilde yönet"""
    conn = None
    try:
        cfg = config_loader.load_config("config.ini")
        host = cfg["database"]["host"]
        port = cfg["database"]["port"]
        service = cfg["database"]["service"]
        dsn = f"{host}:{port}/{service}"

        conn = oracledb.connect(
            user=cfg["database"]["user"],
            password=cfg["database"]["password"],
            dsn=dsn,
        )
        yield conn
    except Exception as e:
        st.error(f"Veritabanı bağlantı hatası: {e}")
        yield None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass  # Bağlantı zaten kapalı olabilir


# Model performans verilerini çek
@st.cache_data(ttl=30)
def get_model_performance():
    """Model performansını pkl dosyasından veya loglardan oku"""
    try:
        # Model pkl dosyası varsa oku
        if os.path.exists("amount_predictor.pkl"):
            import pickle
            with open("amount_predictor.pkl", "rb") as f:
                snapshot = pickle.load(f)

            stats = snapshot.get("stats", {})
            metrics = snapshot.get("metrics", {})

            return {
                'mae': metrics.get('mae', {}).get() if hasattr(metrics.get('mae', {}), 'get') else 0,
                'rmse': metrics.get('rmse', {}).get() if hasattr(metrics.get('rmse', {}), 'get') else 0,
                'r2': metrics.get('r2', {}).get() if hasattr(metrics.get('r2', {}), 'get') else 0,
                'total_processed': stats.get('total_processed', 0),
                'anomaly_count': stats.get('anomaly_count', 0),
                'validation_errors': stats.get('validation_errors', 0),
                'system_health': 'GOOD'  # Bunu hesapla
            }
    except Exception as e:
        st.warning(f"Model dosyası okunamadı: {e}")

    # Default değerler
    return {
        'mae': 0.0, 'rmse': 0.0, 'r2': 0.0,
        'total_processed': 0, 'anomaly_count': 0,
        'validation_errors': 0, 'system_health': 'UNKNOWN'
    }


# Oracle'dan anomali verilerini çek
@st.cache_data(ttl=30)
def get_anomaly_data():
    """Oracle'dan anomali verilerini çek"""
    try:
        with get_db_connection() as conn:
            if not conn:
                return pd.DataFrame()

            # Anomali tablosundan veri çek - DOĞRU TABLO VE KOLON İSİMLERİ
            query = """
            SELECT 
                boarding_date_time,
                route_code,
                customer_flag, 
                tariff_number,
                rider,
                usage_amount,
                predicted_amount,
                error_amount,
                error_percentage,
                state,
                msg_offset
            FROM detected_anomalies 
            WHERE boarding_date_time >= SYSDATE - 1
            ORDER BY boarding_date_time DESC
            """

            df = pd.read_sql(query, conn)
            df['boarding_date_time'] = pd.to_datetime(df['boarding_date_time'])
            return df

    except Exception as e:
        st.error(f"Anomali verileri alınırken hata: {e}")
        return pd.DataFrame()


# Zaman bazlı trend verileri
@st.cache_data(ttl=30)
def get_time_series_data():
    """Saatlik anomali trendini çek"""
    try:
        with get_db_connection() as conn:
            if not conn:
                return pd.DataFrame()

            # ORACLE SQL SYNTAX DÜZELTME
            query = """
            SELECT 
                TRUNC(boarding_date_time, 'HH') as hour,
                COUNT(*) as anomaly_count,
                AVG(error_percentage) as avg_error_pct
            FROM detected_anomalies
            WHERE boarding_date_time >= SYSDATE - 1
            GROUP BY TRUNC(boarding_date_time, 'HH')
            ORDER BY hour
            """

            df = pd.read_sql(query, conn)
            df['hour'] = pd.to_datetime(df['hour'])
            return df

    except Exception as e:
        st.error(f"Trend verileri alınırken hata: {e}")
        return pd.DataFrame()


# Route bazlı istatistikler
@st.cache_data(ttl=30)
def get_route_stats():
    """Route bazlı anomali istatistikleri"""
    try:
        with get_db_connection() as conn:
            if not conn:
                return pd.DataFrame()

            query = """
            SELECT 
                route_code,
                COUNT(*) as total_anomalies,
                AVG(usage_amount) as avg_amount,
                AVG(error_percentage) as avg_error_pct,
                MAX(boarding_date_time) as last_anomaly
            FROM detected_anomalies
            WHERE boarding_date_time >= SYSDATE - 7
            GROUP BY route_code
            ORDER BY total_anomalies DESC
            """

            df = pd.read_sql(query, conn)
            return df

    except Exception as e:
        st.error(f"Route verileri alınırken hata: {e}")
        return pd.DataFrame()


# Streamlit uygulaması başlangıcı
def main():
    # Sidebar - Konfigürasyon
    st.sidebar.header("⚙️ Dashboard Ayarları")
    auto_refresh = st.sidebar.checkbox("🔄 Otomatik Yenileme", value=False)  # Başlangıçta kapalı
    refresh_interval = st.sidebar.slider("Yenileme Süresi (saniye)", 5, 60, 30)
    date_range = st.sidebar.date_input("📅 Tarih Aralığı",
                                       value=[datetime.now() - timedelta(days=1), datetime.now()])

    # Ana başlık
    st.markdown('<h1 class="main-header">🚌 Transit Anomaly Detection Dashboard</h1>',
                unsafe_allow_html=True)

    # Model performans verileri
    perf_data = get_model_performance()
    anomaly_rate = (perf_data['anomaly_count'] / max(perf_data['total_processed'], 1) * 100)

    # KPI Kartları
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("📊 Toplam İşlem",
                  f"{perf_data['total_processed']:,}")

    with col2:
        st.metric("🚨 Anomali Sayısı",
                  f"{perf_data['anomaly_count']:,}")

    with col3:
        st.metric("📈 Anomali Oranı",
                  f"{anomaly_rate:.2f}%")

    with col4:
        health_color = "normal" if perf_data['system_health'] == 'GOOD' else "inverse"
        st.metric("💚 Sistem Sağlığı", perf_data['system_health'])

    # Ana grafikler
    st.markdown("---")

    # İki sütunlu layout
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("📈 Anomali Trendi (Son 24 Saat)")

        ts_data = get_time_series_data()

        if not ts_data.empty:
            fig = px.line(ts_data, x='hour', y='anomaly_count',
                          title="Saatlik Anomali Sayısı",
                          markers=True)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Henüz trend verisi bulunmuyor.")

    with col_right:
        st.subheader("🎯 Model Performansı")

        # R² Score gauge
        r2_score = perf_data['r2'] * 100 if perf_data['r2'] > 0 else 0

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=r2_score,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "R² Score (%)"},
            gauge={'axis': {'range': [None, 100]},
                   'bar': {'color': "darkblue"},
                   'steps': [
                       {'range': [0, 50], 'color': "lightgray"},
                       {'range': [50, 80], 'color': "yellow"},
                       {'range': [80, 100], 'color': "green"}]}))
        fig_gauge.update_layout(height=300)
        st.plotly_chart(fig_gauge, use_container_width=True)

        # MAE ve RMSE
        st.metric("🎯 MAE", f"{perf_data['mae']:.3f} TL")
        st.metric("📊 RMSE", f"{perf_data['rmse']:.3f} TL")

    # Route analizi
    st.markdown("---")
    st.subheader("🗺️ Route Bazlı Analiz")

    route_data = get_route_stats()

    if not route_data.empty:
        col1, col2 = st.columns(2)

        with col1:
            # En problematik route'lar
            fig_routes = px.bar(route_data.head(10),
                                x='route_code', y='total_anomalies',
                                title="En Çok Anomali Olan Route'lar",
                                color='avg_error_pct',
                                color_continuous_scale='Reds')
            fig_routes.update_layout(height=400)
            st.plotly_chart(fig_routes, use_container_width=True)

        with col2:
            # Error yüzdesi dağılımı
            fig_scatter = px.scatter(route_data,
                                     x='total_anomalies', y='avg_error_pct',
                                     size='avg_amount',
                                     hover_name='route_code',
                                     title="Anomali Sayısı vs Ortalama Error %")
            fig_scatter.update_layout(height=400)
            st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.info("Route analizi için henüz yeterli veri bulunmuyor.")

    # Son anomaliler tablosu
    st.markdown("---")
    st.subheader("🚨 Son Tespit Edilen Anomaliler")

    recent_anomalies = get_anomaly_data()

    if not recent_anomalies.empty:
        # State'e göre renklendirme fonksiyonu
        def highlight_state(row):
            if row['state'] == 'critical':
                return ['background-color: #ffebee'] * len(row)
            elif row['state'] == 'major':
                return ['background-color: #fff3e0'] * len(row)
            elif row['state'] == 'minor':
                return ['background-color: #f3e5f5'] * len(row)
            return [''] * len(row)

        # Tabloyu formatla ve göster - DOĞRU KOLON İSİMLERİ
        display_columns = ['boarding_date_time', 'route_code', 'customer_flag',
                           'usage_amount', 'predicted_amount', 'error_percentage', 'state']

        styled_df = recent_anomalies[display_columns].head(50).style.apply(highlight_state, axis=1)
        st.dataframe(styled_df, use_container_width=True, height=400)
    else:
        st.info("Son 24 saatte anomali tespit edilmemiş.")

    # Footer
    st.markdown("---")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.info(f"📅 Son Güncelleme: {datetime.now().strftime('%H:%M:%S')}")

    with col2:
        if perf_data['system_health'] == 'GOOD':
            st.success("✅ Sistem Normal")
        else:
            st.warning("⚠️ Dikkat Gerekli")

    with col3:
        # Model dosyası yaşını hesapla
        if os.path.exists("amount_predictor.pkl"):
            model_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime("amount_predictor.pkl"))
            st.metric("⚡ Model Yaşı", f"{model_age.seconds // 3600}h {(model_age.seconds // 60) % 60}m")
        else:
            st.metric("⚡ Model", "Bulunamadı")

    # Otomatik yenileme - sadece checkbox işaretliyse
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


# Uygulamayı çalıştır
if __name__ == "__main__":
    main()