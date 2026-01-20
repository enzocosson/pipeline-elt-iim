import sys
from io import BytesIO
from typing import List
import os
import subprocess
import sys
import os
from io import BytesIO
from typing import List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Make flows package importable when running from repo root
sys.path.insert(0, "flows")
from config import get_minio_client, BUCKET_GOLD


st.set_page_config(
    page_title="Dashboard ELT Pipeline",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


API_URL = os.environ.get("API_URL", "http://localhost:8000")


@st.cache_data(ttl=300)
def load_from_gold(object_name: str) -> pd.DataFrame:
    """Charge un tableau depuis l'API (collection Mongo). Accepte `name` ou `name.csv`."""
    import httpx

    try:
        coll = object_name
        if coll.endswith('.csv'):
            coll = coll[:-4]
        url = f"{API_URL}/collections/{coll}"
        with httpx.Client(timeout=10) as client:
            r = client.get(url)
            if r.status_code != 200:
                return pd.DataFrame()
            data = r.json()
            if not data:
                return pd.DataFrame()
            df = pd.DataFrame.from_records(data)
            # remove _id if present
            if '_id' in df.columns:
                df = df.drop(columns=['_id'])
            return df
    except Exception:
        return pd.DataFrame()


def get_refresh_info(object_name: str) -> dict:
    import httpx
    try:
        coll = object_name
        if coll.endswith('.csv'):
            coll = coll[:-4]
        url = f"{API_URL}/metadata/{coll}"
        with httpx.Client(timeout=5) as client:
            r = client.get(url)
            if r.status_code != 200:
                return {}
            return r.json()
    except Exception:
        return {}


def safe_sum(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return float(df[col].sum())


def main():
    st.title("📊 Dashboard ELT Pipeline")
    st.markdown("---")

    with st.spinner("Chargement des données depuis MinIO (bucket gold)..."):
        # fichiers produits par les flows gold du projet
        monthly_rev = load_from_gold("monthly_revenue.csv")
        volumes_day = load_from_gold("volumes_day.csv")
        volumes_month = load_from_gold("volumes_month.csv")
        ca_by_country = load_from_gold("ca_by_country.csv")

    # refresh metadata (via API)
    monthly_meta = get_refresh_info("monthly_revenue.csv")
    # monthly_meta keys: delta_source_to_ingest_seconds, delta_ingest_to_now_seconds
    refresh_delta = None
    ingest_age = None
    if monthly_meta:
        refresh_delta = monthly_meta.get("delta_source_to_ingest_seconds")
        ingest_age = monthly_meta.get("delta_ingest_to_now_seconds")

    # KPIs
    ca_total = 0.0
    nb_achats = 0
    panier_moyen = None
    taux_croissance = None

    if not monthly_rev.empty:
        # tenter de détecter colonne de CA
        if "ca_total" in monthly_rev.columns:
            ca_total = safe_sum(monthly_rev, "ca_total")
        elif "montant" in monthly_rev.columns:
            ca_total = safe_sum(monthly_rev, "montant")
        # taux de croissance mensuel si possible
        rev = monthly_rev.copy()
        date_cols = [c for c in rev.columns if "date" in c.lower() or "mois" in c.lower()]
        if not rev.empty and date_cols:
            col = date_cols[0]
            rev[col] = pd.to_datetime(rev[col], errors="coerce")
            rev = rev.sort_values(col)
            val_col = "ca_total" if "ca_total" in rev.columns else ("montant" if "montant" in rev.columns else None)
            if val_col:
                monthly = rev.groupby(rev[col].dt.to_period("M"))[val_col].sum().reset_index()
                monthly['pct'] = monthly[val_col].pct_change() * 100
                if len(monthly) >= 2:
                    taux_croissance = float(monthly['pct'].iloc[-1])

    # nb_achats: tenter depuis volumes
    if not volumes_month.empty:
        # chercher colonne 'volume' ou 'nb_achats' ou 'count'
        for c in ["volume", "nb_achats", "count", "nombre"]:
            if c in volumes_month.columns:
                nb_achats = int(volumes_month[c].sum())
                break
    elif not volumes_day.empty:
        for c in ["volume", "nb_achats", "count", "nombre"]:
            if c in volumes_day.columns:
                nb_achats = int(volumes_day[c].sum())
                break

    if nb_achats > 0:
        panier_moyen = ca_total / nb_achats

    # SECTION 1: KPIs
    st.header("📈 Indicateurs Clés (KPIs)")
    # styled KPI cards
    st.markdown(_CARD_CSS, unsafe_allow_html=True)
    kcol1, kcol2, kcol3, kcol4 = st.columns(4)

    with kcol1:
        styled_metric('💰 CA Total', f"{ca_total:,.2f} €")
    with kcol2:
        styled_metric('🛒 Nombre d\'achats', f"{nb_achats:,}")
    with kcol3:
        styled_metric('💵 Panier moyen', f"{panier_moyen:,.2f} €" if panier_moyen is not None else "N/A")
    with kcol4:
        styled_metric('📊 Croissance mensuelle', f"{taux_croissance:.2f} %" if taux_croissance is not None else "N/A")

    # Afficher temps de refresh si disponible
    if monthly_meta:
        try:
            rcol1, rcol2, _ = st.columns([1,1,2])
            with rcol1:
                if refresh_delta is not None:
                    st.metric('⏱️ Temps source -> ingest (s)', f"{int(refresh_delta)}")
                else:
                    st.write('⏱️ Temps source -> ingest: N/A')
            with rcol2:
                if ingest_age is not None:
                    st.metric('🕒 Âge de l\'ingestion (s)', f"{int(ingest_age)}")
                else:
                    st.write("🕒 Âge de l'ingestion: N/A")
        except Exception:
            pass

    st.markdown("---")

    # SECTION 2: EVOLUTION TEMPORELLE
    st.header("📅 Évolution Temporelle")
    gran = st.selectbox("Choisir la granularité", ["Par jour", "Par mois"], index=1)

    if gran == "Par jour" and not volumes_day.empty:
        # supposer une colonne 'date' et 'volume' ou 'nb_achats'
        df = volumes_day.copy()
        date_col = [c for c in df.columns if 'date' in c.lower()]
        if date_col:
            df[date_col[0]] = pd.to_datetime(df[date_col[0]], errors='coerce')
            df = df.sort_values(date_col[0])
            val_col = next((c for c in ['volume','nb_achats','count'] if c in df.columns), None)
            if val_col:
                fig = px.bar(df, x=date_col[0], y=val_col, title='Volume par jour')
                st.plotly_chart(fig, use_container_width=True)
    elif gran == "Par mois" and not monthly_rev.empty:
        # afficher monthly_revenue
        df = monthly_rev.copy()
        date_col = [c for c in df.columns if 'date' in c.lower() or 'mois' in c.lower()]
        if date_col:
            col = date_col[0]
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df = df.sort_values(col)
            val_col = 'ca_total' if 'ca_total' in df.columns else ('montant' if 'montant' in df.columns else None)
            if val_col:
                fig = px.line(df, x=col, y=val_col, title='Évolution du CA (mensuel)', markers=True)
                st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # SECTION 3: ANALYSE PAR PAYS
    st.header("🌍 CA par Pays")
    if not ca_by_country.empty:
        df = ca_by_country.copy()
        # normalize columns
        col_ca = next((c for c in ['ca_total','montant','ca'] if c in df.columns), None)
        col_pays = next((c for c in ['pays','country','country_name'] if c in df.columns), None)
        if col_ca and col_pays:
            df = df.sort_values(col_ca, ascending=False)
            fig = px.bar(df, x=col_pays, y=col_ca, title='CA par pays', color=col_ca, color_continuous_scale='Greens')
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df[[col_pays, col_ca]].rename(columns={col_pays: 'Pays', col_ca: 'CA'}), use_container_width=True)
    else:
        st.info('Aucun fichier `ca_by_country.csv` trouvé dans le bucket gold.')

    st.markdown("---")

    # SECTION 4: DISTRIBUTIONS
    st.header("📊 Distributions statistiques")
    # prefer fact-like table if exists in gold (monthly_rev may contain montants)
    base_df = None
    if not monthly_rev.empty and ('montant' in monthly_rev.columns or 'ca_total' in monthly_rev.columns):
        base_df = monthly_rev
    elif not volumes_day.empty:
        base_df = volumes_day

    if base_df is not None:
        numeric = base_df.select_dtypes(include=['number'])
        if not numeric.empty:
            col = st.selectbox('Choisir une colonne numérique pour la distribution', list(numeric.columns), key='dist_col')
            fig_hist = px.histogram(base_df, x=col, nbins=50, title=f'Histogramme de {col}')
            fig_box = px.box(base_df, y=col, title=f'Boxplot de {col}')
            c1, c2 = st.columns(2)
            with c1:
                st.plotly_chart(fig_hist, use_container_width=True)
            with c2:
                st.plotly_chart(fig_box, use_container_width=True)
        else:
            st.info('Aucune colonne numérique disponible pour les distributions.')
    else:
        st.info('Aucune table adaptée trouvée pour afficher les distributions.')

    st.markdown('---')
    # SECTION 5: DONNÉES BRUTES
    # advanced visuals
    show_advanced_visuals(monthly_rev, volumes_day, ca_by_country)

    st.markdown('---')

    # SECTION 5: DONNÉES BRUTES
    with st.expander('📋 Voir les données brutes'):
        tabs = st.tabs(['monthly_revenue', 'volumes_day', 'volumes_month', 'ca_by_country'])
        for tname, df in zip(['monthly_revenue', 'volumes_day', 'volumes_month', 'ca_by_country'], [monthly_rev, volumes_day, volumes_month, ca_by_country]):
            tab = tabs[['monthly_revenue', 'volumes_day', 'volumes_month', 'ca_by_country'].index(tname)]
            with tab:
                if df is None or df.empty:
                    st.write(f'Aucun fichier `{tname}.csv` chargé')
                else:
                    st.dataframe(df.head(200), use_container_width=True)
 

def show_advanced_visuals(monthly_rev, volumes_day, ca_by_country):
    """Add more advanced, professional charts: cumulative, moving averages,
    Pareto for top countries and a weekly heatmap of volumes.
    """
    st.markdown('---')
    st.header('🔬 Visualisations avancées')

    # --- cumulative revenue + moving average
    if not monthly_rev.empty and ('ca_total' in monthly_rev.columns or 'montant' in monthly_rev.columns):
        df = monthly_rev.copy()
        # normalize date and value column
        date_col = next((c for c in df.columns if 'date' in c.lower() or 'mois' in c.lower()), None)
        value_col = 'ca_total' if 'ca_total' in df.columns else ('montant' if 'montant' in df.columns else None)
        if date_col and value_col:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.sort_values(date_col)
            df['cumulative'] = df[value_col].cumsum()
            df['ma_3'] = df[value_col].rolling(3, min_periods=1).mean()

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df[date_col], y=df['cumulative'], mode='lines', name='CA cumulé'))
            fig.add_trace(go.Scatter(x=df[date_col], y=df['ma_3'], mode='lines', name='Moyenne mobile (3)', line=dict(dash='dash')))
            fig.update_layout(title='CA cumulé et Moyenne mobile', yaxis_title='CA (€)')
            st.plotly_chart(fig, use_container_width=True)

    # --- Pareto: top countries contribution
    if not ca_by_country.empty:
        df = ca_by_country.copy()
        val_col = next((c for c in ['ca_total','montant','ca'] if c in df.columns), None)
        country_col = next((c for c in ['pays','country','country_name'] if c in df.columns), None)
        if val_col and country_col:
            p = df[[country_col, val_col]].dropna()
            p = p.sort_values(val_col, ascending=False)
            p['cumperc'] = p[val_col].cumsum() / p[val_col].sum() * 100
            fig = make_pareto_chart(p, country_col, val_col)
            st.plotly_chart(fig, use_container_width=True)

    # --- weekly heatmap from volumes_day
    if not volumes_day.empty:
        df = volumes_day.copy()
        date_col = next((c for c in df.columns if 'date' in c.lower()), None)
        val_col = next((c for c in ['volume','nb_achats','count'] if c in df.columns), None)
        if date_col and val_col:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.dropna(subset=[date_col])
            df['dow'] = df[date_col].dt.day_name()
            df['week'] = df[date_col].dt.isocalendar().week
            pivot = df.pivot_table(index='dow', columns='week', values=val_col, aggfunc='sum').reindex(['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'])
            fig = px.imshow(pivot, aspect='auto', color_continuous_scale='Blues', labels=dict(x='Semaine', y='Jour', color=val_col))
            fig.update_layout(title='Heatmap hebdomadaire des volumes')
            st.plotly_chart(fig, use_container_width=True)


def make_pareto_chart(df, country_col, val_col):
    # df expected sorted desc
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df[country_col], y=df[val_col], name='CA'))
    fig.add_trace(go.Scatter(x=df[country_col], y=df['cumperc'], name='Cumulaire (%)', yaxis='y2', mode='lines+markers'))
    fig.update_layout(
        yaxis=dict(title='CA'),
        yaxis2=dict(title='Contribution cumulée (%)', overlaying='y', side='right', range=[0,100]),
        title='Pareto - contribution par pays',
        xaxis_tickangle=-45
    )
    return fig


# Small CSS to make KPI cards look more professional
_CARD_CSS = """
<style>
/* Dark, high-contrast KPI cards to ensure readability */
.kpi {background: linear-gradient(90deg,#0b1220,#0f172a); color: #ffffff; padding: 12px; border-radius: 8px; box-shadow: 0 6px 18px rgba(2,6,23,0.6);}
.kpi .label {color: #9ca3af; font-size: 13px; margin-bottom:6px}
.kpi .value {color: #ffffff; font-size: 20px; font-weight:700}
.kpi .delta {color: #a7f3d0; font-size:13px}
.kpi small {color: #9ca3af}
/* Ensure cards adapt when Streamlit theme is light */
.stApp .kpi {border: 1px solid rgba(255,255,255,0.06)}
</style>
"""


def styled_metric(label, value, delta=None):
    html = f"""
    <div class='kpi'>
      <div class='label'>{label}</div>
      <div class='value'>{value}</div>
      {f"<div class='delta'>{delta}</div>" if delta else ''}
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


if __name__ == '__main__':
    # if running directly, spawn streamlit to serve this file (keeps compatibility)
    if os.environ.get('RUN_BY_STREAMLIT') == '1':
        main()
    else:
        port = os.environ.get('STREAMLIT_PORT', '8502')
        cmd = [sys.executable, '-m', 'streamlit', 'run', __file__, '--server.port', port, '--server.headless', 'true']
        env = os.environ.copy()
        env['RUN_BY_STREAMLIT'] = '1'
        print('Lancement du serveur Streamlit sur le port', port)
        os.execve(sys.executable, [sys.executable, '-m', 'streamlit', 'run', __file__, '--server.port', port, '--server.headless', 'true'], env)
