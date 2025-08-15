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

st.set_page_config(
    page_title="🚌 Transit Anomaly Dashboard",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded"
)

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


@contextmanager
def get_db_connection():
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
    except Exception as e:
        st.error(f"Veritabanı bağlantı hatası: {e}")
    try:
        yield conn
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


@st.cache_data(ttl=30)
def get_model_performance():
    try:
        if os.path.exists("amount_predictor.pkl"):
            import pickle
            with open("amount_predictor.pkl", "rb") as f:
                snapshot = pickle.load(f)

            metrics = snapshot.get("metrics", {})
            stats = snapshot.get("stats", {})

            mae = metrics.get('mae').get() if metrics.get('mae') and hasattr(metrics.get('mae'), 'get') else 0.0
            rmse = metrics.get('rmse').get() if metrics.get('rmse') and hasattr(metrics.get('rmse'), 'get') else 0.0
            r2 = metrics.get('r2').get() if metrics.get('r2') and hasattr(metrics.get('r2'), 'get') else 0.0

            total_processed = stats.get('total_processed', 0)
            anomaly_count = stats.get('anomaly_count', 0)
            validation_errors = stats.get('validation_errors', 0)

            anomaly_rate = (anomaly_count / max(total_processed, 1)) * 100

            health_status = "GOOD"
            issues = []

            if mae > 1.0:
                health_status = "WARNING"
                issues.append(f"High MAE: {mae:.3f} TL")

            if r2 < 0.5:
                health_status = "CRITICAL"
                issues.append(f"Low R²: {r2:.3f}")

            if anomaly_rate > 5:
                health_status = "WARNING" if health_status == "GOOD" else health_status
                issues.append(f"High anomaly rate: {anomaly_rate:.1f}%")

            return {
                'mae': mae,
                'rmse': rmse,
                'r2': r2,
                'total_processed': total_processed,
                'anomaly_count': anomaly_count,
                'validation_errors': validation_errors,
                'anomaly_rate': anomaly_rate,
                'system_health': health_status,
                'issues': issues
            }
    except Exception as e:
        st.warning(f"Model dosyası okunamadı: {e}")

    return {
        'mae': 0.0, 'rmse': 0.0, 'r2': 0.0,
        'total_processed': 0, 'anomaly_count': 0,
        'validation_errors': 0, 'anomaly_rate': 0.0,
        'system_health': 'UNKNOWN', 'issues': []
    }


@st.cache_data(ttl=30)
def get_anomaly_data():
    try:
        with get_db_connection() as conn:
            if not conn:
                return pd.DataFrame()

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
            WHERE TO_DATE(boarding_date_time, 'YYYY-MM-DD HH24:MI:SS') >= SYSDATE - 1
            ORDER BY TO_DATE(boarding_date_time, 'YYYY-MM-DD HH24:MI:SS') DESC
            """

            df = pd.read_sql(query, conn)

            df.columns = df.columns.str.lower()

            if 'boarding_date_time' in df.columns:
                df['boarding_date_time'] = pd.to_datetime(df['boarding_date_time'], errors='coerce')

            return df

    except Exception as e:
        st.error(f"Anomali verileri alınırken hata: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def get_time_series_data():
    try:
        with get_db_connection() as conn:
            if not conn:
                return pd.DataFrame()

            query = """
            SELECT 
                TO_CHAR(TO_DATE(SUBSTR(boarding_date_time, 1, 13) || ':00:00', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') AS hour_str,
                COUNT(*) AS anomaly_count,
                AVG(error_percentage) AS avg_error_pct
            FROM detected_anomalies
            WHERE TO_DATE(boarding_date_time, 'YYYY-MM-DD HH24:MI:SS') >= SYSDATE - 1
            GROUP BY TO_CHAR(TO_DATE(SUBSTR(boarding_date_time, 1, 13) || ':00:00', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')
            ORDER BY hour_str
            """

            df = pd.read_sql(query, conn)


            df.columns = df.columns.str.lower()

            if 'hour_str' in df.columns:
                df['hour'] = pd.to_datetime(df['hour_str'])
                df = df.drop('hour_str', axis=1)

            return df

    except Exception as e:
        st.error(f"Trend verileri alınırken hata: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def get_route_stats():
    try:
        with get_db_connection() as conn:
            if not conn:
                return pd.DataFrame()

            query = """
            SELECT 
                route_code,
                COUNT(*) AS total_anomalies,
                AVG(usage_amount) AS avg_amount,
                AVG(error_percentage) AS avg_error_pct,
                MAX(boarding_date_time) AS last_anomaly
            FROM detected_anomalies
            WHERE TO_DATE(boarding_date_time, 'YYYY-MM-DD HH24:MI:SS') >= SYSDATE - 7
            GROUP BY route_code
            ORDER BY total_anomalies DESC
            """

            df = pd.read_sql(query, conn)

            df.columns = df.columns.str.lower()

            if 'last_anomaly' in df.columns:
                df['last_anomaly'] = pd.to_datetime(df['last_anomaly'], errors='coerce')

            return df

    except Exception as e:
        st.error(f"Route verileri alınırken hata: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def get_state_distribution():
    try:
        with get_db_connection() as conn:
            if not conn:
                return pd.DataFrame()

            query = """
            SELECT 
                state,
                COUNT(*) AS count,
                AVG(error_percentage) AS avg_error_pct
            FROM detected_anomalies
            WHERE TO_DATE(boarding_date_time, 'YYYY-MM-DD HH24:MI:SS') >= SYSDATE - 1
            GROUP BY state
            ORDER BY count DESC
            """

            df = pd.read_sql(query, conn)
            df.columns = df.columns.str.lower()
            return df

    except Exception as e:
        st.error(f"State dağılımı alınırken hata: {e}")
        return pd.DataFrame()


def main():
    st.sidebar.header("⚙️ Dashboard Ayarları")
    auto_refresh = st.sidebar.checkbox("🔄 Otomatik Yenileme", value=False)
    refresh_interval = st.sidebar.slider("Yenileme Süresi (saniye)", 5, 60, 30)
    date_range = st.sidebar.date_input(
        "📅 Tarih Aralığı",
        value=[datetime.now() - timedelta(days=1), datetime.now()]
    )

    st.markdown('<h1 class="main-header">🚌 Transit Anomaly Detection Dashboard</h1>',
                unsafe_allow_html=True)

    perf_data = get_model_performance()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("📊 Toplam İşlem",
                  f"{perf_data['total_processed']:,}")

    with col2:
        st.metric("🚨 Anomali Sayısı",
                  f"{perf_data['anomaly_count']:,}")

    with col3:
        st.metric("📈 Anomali Oranı",
                  f"{perf_data['anomaly_rate']:.2f}%")

    with col4:
        health_color = "🟢" if perf_data['system_health'] == 'GOOD' else (
            "🟡" if perf_data['system_health'] == 'WARNING' else "🔴")
        st.metric("💚 Sistem Sağlığı", f"{health_color} {perf_data['system_health']}")

    if perf_data['system_health'] != 'UNKNOWN':
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("🎯 MAE", f"{perf_data['mae']:.3f} TL")
        with col2:
            st.metric("📊 RMSE", f"{perf_data['rmse']:.3f} TL")
        with col3:
            st.metric("📈 R² Score", f"{perf_data['r2']:.3f}")

        if perf_data['issues']:
            st.warning("⚠️ **Tespit Edilen Sorunlar:**")
            for issue in perf_data['issues']:
                st.write(f"• {issue}")

    st.markdown("---")

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

        state_data = get_state_distribution()
        if not state_data.empty:
            fig_pie = px.pie(state_data, values='count', names='state',
                             title="Anomali Türü Dağılımı")
            fig_pie.update_layout(height=250)
            st.plotly_chart(fig_pie, use_container_width=True)

    st.markdown("---")
    st.subheader("🗺️ Route Bazlı Analiz")

    route_data = get_route_stats()

    if not route_data.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig_routes = px.bar(route_data.head(10),
                                x='route_code', y='total_anomalies',
                                title="En Çok Anomali Olan Route'lar",
                                color='avg_error_pct',
                                color_continuous_scale='Reds')
            fig_routes.update_layout(height=400)
            st.plotly_chart(fig_routes, use_container_width=True)

        with col2:
            fig_scatter = px.scatter(route_data,
                                     x='total_anomalies', y='avg_error_pct',
                                     size='avg_amount',
                                     hover_name='route_code',
                                     title="Anomali Sayısı vs Ortalama Error %")
            fig_scatter.update_layout(height=400)
            st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.info("Route analizi için henüz yeterli veri bulunmuyor.")

    st.markdown("---")
    st.subheader("🚨 Son Tespit Edilen Anomaliler")

    recent_anomalies = get_anomaly_data()

    if not recent_anomalies.empty:
        def highlight_state(row):
            if row['state'] == 'critical':
                return ['background-color: #ffebee'] * len(row)
            elif row['state'] == 'major':
                return ['background-color: #fff3e0'] * len(row)
            elif row['state'] == 'minor':
                return ['background-color: #f3e5f5'] * len(row)
            return [''] * len(row)

        display_columns = ['boarding_date_time', 'route_code', 'customer_flag',
                           'usage_amount', 'predicted_amount', 'error_percentage', 'state']

        if all(col in recent_anomalies.columns for col in display_columns):
            styled_df = recent_anomalies[display_columns].head(50).style.apply(highlight_state, axis=1)
            st.dataframe(styled_df, use_container_width=True, height=400)
        else:
            st.dataframe(recent_anomalies.head(50), use_container_width=True, height=400)
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
        elif perf_data['system_health'] == 'WARNING':
            st.warning("⚠️ Dikkat Gerekli")
        else:
            st.error("🚨 Kritik Durum")

    with col3:
        if os.path.exists("amount_predictor.pkl"):
            model_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime("amount_predictor.pkl"))
            st.metric("⚡ Model Yaşı", f"{model_age.seconds // 3600}h {(model_age.seconds // 60) % 60}m")
        else:
            st.metric("⚡ Model", "Bulunamadı")

    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main()