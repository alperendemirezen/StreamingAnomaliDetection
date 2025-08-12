import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import oracledb
from datetime import datetime, timedelta
import time
import numpy as np

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

# Sidebar - Konfigürasyon
st.sidebar.header("⚙️ Dashboard Ayarları")
auto_refresh = st.sidebar.checkbox("🔄 Otomatik Yenileme", value=True)
refresh_interval = st.sidebar.slider("Yenileme Süresi (saniye)", 5, 60, 30)
date_range = st.sidebar.date_input("📅 Tarih Aralığı",
                                   value=[datetime.now() - timedelta(days=1), datetime.now()])


# Mock veri fonksiyonları (gerçekte Oracle'dan gelecek)
@st.cache_data(ttl=30)  # 30 saniye cache
def get_mock_performance_data():
    """Model performans verileri"""
    return {
        'mae': 0.125,
        'rmse': 0.234,
        'r2': 0.876,
        'total_processed': 15847,
        'anomaly_count': 234,
        'validation_errors': 12,
        'system_health': 'GOOD'
    }


@st.cache_data(ttl=30)
def get_mock_time_series():
    """Zaman bazlı anomali verileri"""
    dates = pd.date_range(start=datetime.now() - timedelta(hours=24),
                          end=datetime.now(), freq='1H')
    np.random.seed(42)
    anomaly_counts = np.random.poisson(8, len(dates))  # Poisson dağılımı
    processing_counts = np.random.normal(500, 50, len(dates))  # Normal dağılım

    return pd.DataFrame({
        'time': dates,
        'anomaly_count': anomaly_counts,
        'processing_count': processing_counts.astype(int),
        'anomaly_rate': (anomaly_counts / processing_counts.astype(int) * 100).round(2)
    })


@st.cache_data(ttl=30)
def get_mock_route_data():
    """Route bazlı veriler"""
    routes = ['M1_Metro', 'Bus_34C', 'Bus_500T', 'M2_Metro', 'Bus_15F',
              'Metrobus_34', 'Bus_28', 'M3_Metro', 'Bus_110', 'Ferry_1']
    np.random.seed(123)
    return pd.DataFrame({
        'route_code': routes,
        'total_transactions': np.random.randint(1000, 5000, len(routes)),
        'anomaly_count': np.random.randint(10, 100, len(routes)),
        'avg_amount': np.random.uniform(2.5, 15.0, len(routes)).round(2)
    })


@st.cache_data(ttl=30)
def get_mock_recent_anomalies():
    """Son anomaliler"""
    np.random.seed(456)
    routes = ['M1_Metro', 'Bus_34C', 'Bus_500T', 'M2_Metro', 'Bus_15F']
    states = ['critical', 'major', 'minor']

    data = []
    for i in range(20):
        data.append({
            'time': datetime.now() - timedelta(minutes=i * 15),
            'route_code': np.random.choice(routes),
            'customer_flag': np.random.choice(['student', 'adult', 'senior']),
            'usage_amount': round(np.random.uniform(15, 50), 2),
            'predicted_amount': round(np.random.uniform(2, 8), 2),
            'error': round(np.random.uniform(10, 45), 2),
            'state': np.random.choice(states, p=[0.2, 0.3, 0.5])
        })

    return pd.DataFrame(data)


# Ana başlık
st.markdown('<h1 class="main-header">🚌 Transit Anomaly Detection Dashboard</h1>',
            unsafe_allow_html=True)

# Performans verileri
perf_data = get_mock_performance_data()
anomaly_rate = (perf_data['anomaly_count'] / perf_data['total_processed'] * 100)

# KPI Kartları
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("📊 Toplam İşlem",
              f"{perf_data['total_processed']:,}",
              delta="1,234 (son saat)")

with col2:
    st.metric("🚨 Anomali Sayısı",
              f"{perf_data['anomaly_count']:,}",
              delta="23 (son saat)")

with col3:
    st.metric("📈 Anomali Oranı",
              f"{anomaly_rate:.2f}%",
              delta="-0.15%")

with col4:
    system_color = "normal" if perf_data['system_health'] == 'GOOD' else "inverse"
    st.metric("💚 Sistem Sağlığı",
              perf_data['system_health'])

# Ana grafikler
st.markdown("---")

# İki sütunlu layout
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("📈 Anomali Trendi (Son 24 Saat)")

    # Time series verisi
    ts_data = get_mock_time_series()

    # Plotly ile dual-axis grafik
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Anomali sayısı
    fig.add_trace(
        go.Scatter(x=ts_data['time'], y=ts_data['anomaly_count'],
                   name="Anomali Sayısı",
                   line=dict(color='#ff6b6b', width=3),
                   fill='tonexty'),
        secondary_y=False,
    )

    # İşlem sayısı
    fig.add_trace(
        go.Scatter(x=ts_data['time'], y=ts_data['processing_count'],
                   name="İşlem Sayısı",
                   line=dict(color='#4ecdc4', width=2),
                   opacity=0.7),
        secondary_y=True,
    )

    fig.update_xaxes(title_text="Zaman")
    fig.update_yaxes(title_text="Anomali Sayısı", secondary_y=False)
    fig.update_yaxes(title_text="İşlem Sayısı", secondary_y=True)
    fig.update_layout(height=400, hovermode='x unified')

    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("🎯 Model Performansı")

    # Gauge chart
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=perf_data['r2'] * 100,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "R² Score (%)"},
        delta={'reference': 85},
        gauge={'axis': {'range': [None, 100]},
               'bar': {'color': "darkblue"},
               'steps': [
                   {'range': [0, 50], 'color': "lightgray"},
                   {'range': [50, 80], 'color': "yellow"},
                   {'range': [80, 100], 'color': "green"}],
               'threshold': {'line': {'color': "red", 'width': 4},
                             'thickness': 0.75, 'value': 90}}))
    fig_gauge.update_layout(height=300)
    st.plotly_chart(fig_gauge, use_container_width=True)

    # MAE ve RMSE
    st.metric("🎯 MAE", f"{perf_data['mae']:.3f} TL")
    st.metric("📊 RMSE", f"{perf_data['rmse']:.3f} TL")

# Route analizi
st.markdown("---")
st.subheader("🗺️ Route Bazlı Analiz")

route_data = get_mock_route_data()
route_data['anomaly_rate'] = (route_data['anomaly_count'] / route_data['total_transactions'] * 100).round(2)

col1, col2 = st.columns(2)

with col1:
    # Bar chart - En problematik route'lar
    fig_routes = px.bar(route_data.nlargest(8, 'anomaly_count'),
                        x='route_code', y='anomaly_count',
                        title="En Çok Anomali Olan Route'lar",
                        color='anomaly_rate',
                        color_continuous_scale='Reds')
    fig_routes.update_layout(height=400)
    st.plotly_chart(fig_routes, use_container_width=True)

with col2:
    # Scatter plot - Anomali oranı vs işlem sayısı
    fig_scatter = px.scatter(route_data,
                             x='total_transactions', y='anomaly_rate',
                             size='anomaly_count', color='avg_amount',
                             hover_name='route_code',
                             title="İşlem Sayısı vs Anomali Oranı")
    fig_scatter.update_layout(height=400)
    st.plotly_chart(fig_scatter, use_container_width=True)

# Son anomaliler tablosu
st.markdown("---")
st.subheader("🚨 Son Tespit Edilen Anomaliler")

recent_anomalies = get_mock_recent_anomalies()


# State'e göre renklendirme
def highlight_state(val):
    colors = {
        'critical': 'background-color: #ffebee; color: #c62828',
        'major': 'background-color: #fff3e0; color: #ef6c00',
        'minor': 'background-color: #f3e5f5; color: #7b1fa2'
    }
    return colors.get(val, '')


# Tabloyu formatlama (uyarıyı gidermek için)
styled_df = recent_anomalies.style.map(highlight_state, subset=['state'])

st.dataframe(styled_df, use_container_width=True, height=400)

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
    st.metric("⚡ Uptime", "2d 14h 23m")

# Otomatik yenileme
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()