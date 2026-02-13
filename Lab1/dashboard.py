import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# --------------------------------------------------
# Configuration de la Page
# --------------------------------------------------
st.set_page_config(
    page_title="Analytics Dashboard",
    page_icon="",
    layout="wide"
)

# --------------------------------------------------
# Gestion des Chemins (Auto-détection)
# --------------------------------------------------
# On récupère le dossier où se trouve dashboard.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")

APP_KPIS_PATH = os.path.join(PROCESSED_DIR, "app_level_kpis.csv")
DAILY_METRICS_PATH = os.path.join(PROCESSED_DIR, "daily_metrics.csv")

# --------------------------------------------------
# Chargement des Données
# --------------------------------------------------
@st.cache_data
def load_data():
    """Charge les datasets traités avec gestion d'erreurs"""
    try:
        app_kpis = pd.read_csv(APP_KPIS_PATH)
        daily_metrics = pd.read_csv(DAILY_METRICS_PATH)
        daily_metrics['date'] = pd.to_datetime(daily_metrics['date'])
        return app_kpis, daily_metrics
    except FileNotFoundError as e:
        st.error(f"Erreur : Fichier introuvable. Vérifiez le chemin : {e}")
        return None, None

# --------------------------------------------------
# Structure du Dashboard
# --------------------------------------------------
st.title("Analytics Dashboard")
app_kpis, daily_metrics = load_data()

if app_kpis is not None:
    # --- SECTION : VUE D'ENSEMBLE ---
    st.header("Overview")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(label="Total Apps Analyzed", value=len(app_kpis))
    with col2:
        st.metric(label="Total Reviews", value=f"{app_kpis['num_reviews'].sum():,}")
    with col3:
        st.metric(label="Average Rating (Overall)", value=f"{app_kpis['avg_rating'].mean():.2f}")
    with col4:
        date_range = f"{daily_metrics['date'].min().strftime('%Y-%m-%d')} to {daily_metrics['date'].max().strftime('%Y-%m-%d')}"
        st.metric(label="Date Range", value=date_range)

    st.divider()

    # --- SECTION 1 : PERFORMANCE DES APPS ---
    st.header("App Performance Analysis")
    
    app_kpis_sorted = app_kpis.sort_values('avg_rating', ascending=False)
    col1, col2 = st.columns(2)

    with col1:
        fig_rating = px.bar(
            app_kpis_sorted,
            x='avg_rating', y='app_name',
            orientation='h',
            title='Average Rating by Application',
            color='avg_rating',
            color_continuous_scale='RdYlGn',
            range_color=[1, 5]
        )
        # Correction 2026 : width='stretch'
        st.plotly_chart(fig_rating, width='stretch')

    with col2:
        fig_scatter = px.scatter(
            app_kpis,
            x='pct_low_rating', y='avg_rating',
            size='num_reviews',
            hover_data=['app_name'],
            title='Rating vs. % Low Ratings',
            color='avg_rating',
            color_continuous_scale='RdYlGn'
        )
        st.plotly_chart(fig_scatter, width='stretch')

    # Tableaux de données
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Top 5 Best Rated Apps**")
        st.dataframe(app_kpis_sorted.head(5), hide_index=True, width='stretch')
    with c2:
        st.markdown("**Bottom 5 Rated Apps**")
        st.dataframe(app_kpis_sorted.tail(5), hide_index=True, width='stretch')

    st.divider()

    # --- SECTION 2 : TENDANCES TEMPORELLES ---
    st.header("Rating Trends Over Time")
    
    daily_metrics_sorted = daily_metrics.sort_values('date')
    daily_metrics_sorted['rating_7day_ma'] = daily_metrics_sorted['daily_avg_rating'].rolling(window=7, min_periods=1).mean()

    fig_trends = go.Figure()
    fig_trends.add_trace(go.Scatter(
        x=daily_metrics_sorted['date'], y=daily_metrics_sorted['daily_avg_rating'],
        mode='lines', name='Daily Avg', line=dict(color='lightblue'), opacity=0.5
    ))
    fig_trends.add_trace(go.Scatter(
        x=daily_metrics_sorted['date'], y=daily_metrics_sorted['rating_7day_ma'],
        mode='lines', name='7-Day MA', line=dict(color='darkblue', width=3)
    ))
    
    fig_trends.update_layout(title="Evolution de la Note Moyenne", height=400)
    st.plotly_chart(fig_trends, width='stretch')

    st.divider()

    # --- SECTION 3 : VOLUME DE REVIEWS ---
    st.header("Review Volume Analysis")
    col_v1, col_v2 = st.columns(2)

    with col_v1:
        fig_vol = px.bar(
            app_kpis.sort_values('num_reviews'),
            x='num_reviews', y='app_name',
            title="Volume Total par App",
            color_discrete_sequence=['steelblue']
        )
        st.plotly_chart(fig_vol, width='stretch')
    
    with col_v2:
        fig_daily_vol = px.line(
            daily_metrics_sorted,
            x='date', y='daily_num_reviews',
            title="Volume Quotidien Total"
        )
        st.plotly_chart(fig_daily_vol, width='stretch')