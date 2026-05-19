# ============================================================
# PANEL ADMIN — DASHBOARD VISUALISATION HORS BARÈME
# ============================================================
# Dashboard Streamlit pour visualiser l'évolution des salaires
# des individus détectés hors barème.
#
# Usage :
#   streamlit run app_hors_bareme_dashboard.py
#
# Prérequis :
#   pip install streamlit polars boto3 python-dotenv plotly
# ============================================================

import io
import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

load_dotenv(".env")

# Configuration Streamlit - DOIT ÊTRE LE PREMIER APPEL
st.set_page_config(
    page_title="Dashboard - Individus Hors Barème",
    page_icon="📊",
    layout="wide"
)

# ============================================================
# CONFIGURATION
# ============================================================

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_GOLD    = os.getenv("BUCKET_GOLD", "gold")
BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")

KEY_PANEL_COMPLET   = "panel_admin/panel_complet.parquet"
KEY_HORS_BAREME     = "panel_admin/exports_gold/agents_au_dessus_max.parquet"

# ============================================================
# CLIENT S3
# ============================================================

@st.cache_resource
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url          = MINIO_ENDPOINT,
        aws_access_key_id     = MINIO_ACCESS_KEY,
        aws_secret_access_key = MINIO_SECRET_KEY,
        config                = Config(
            signature_version = "s3v4",
            retries           = {"max_attempts": 5, "mode": "adaptive"},
            connect_timeout   = 30,
            read_timeout      = 120,
        ),
        region_name = "us-east-1",
        verify      = False,
    )

s3 = get_s3_client()

# ============================================================
# CHARGEMENT DES DONNÉES
# ============================================================

@st.cache_data
def load_panel_complet():
    """Charge le panel complet."""
    buf = io.BytesIO()
    s3.download_fileobj(BUCKET_GOLD, KEY_PANEL_COMPLET, buf)
    buf.seek(0)
    return pl.read_parquet(buf)

@st.cache_data
def load_agents_hors_bareme():
    """Charge les agents hors barème. Retourne None si fichier absent."""
    try:
        buf = io.BytesIO()
        s3.download_fileobj(BUCKET_GOLD, KEY_HORS_BAREME, buf)
        buf.seek(0)
        return pl.read_parquet(buf)
    except Exception as e:
        return None

# ============================================================
# IDENTIFIER LES INDIVIDUS HORS BARÈME DEPUIS LE PANEL
# ============================================================

@st.cache_data
def get_individus_hors_bareme(panel, hb_file_exists=True):
    """Identifie les individus hors barème et crée un résumé."""
    
    # Si le fichier HB existe, l'utiliser
    if hb_file_exists:
        hb = load_agents_hors_bareme()
        if hb is not None:
            cols_summary = ["matricule", "cle_unique", "mois_annee", "annee", "mois",
                            "GRADE", "CODE_ORGANISME", "organisme", "CODE_EMPLOI", "emploi",
                            "montant_brut_bareme", "Statut", "min_bareme", "max_bareme"]
            cols_summary = [c for c in cols_summary if c in hb.columns]
            
            summary = hb.select(cols_summary).group_by("matricule").agg([
                pl.col("annee").min().alias("annee_premiere"),
                pl.col("annee").max().alias("annee_derniere"),
                pl.col("annee").n_unique().alias("nb_annees"),
                pl.col("mois_annee").n_unique().alias("nb_mois"),
                pl.col("GRADE").first().alias("GRADE"),
                pl.col("CODE_ORGANISME").first().alias("CODE_ORGANISME"),
                pl.col("organisme").first().alias("organisme"),
                pl.col("CODE_EMPLOI").first().alias("CODE_EMPLOI"),
                pl.col("emploi").first().alias("emploi"),
                pl.col("montant_brut_bareme").mean().alias("moyenne_salaire"),
            ])
            
            if "Statut" in hb.columns:
                statut_df = hb.select(["matricule", "Statut"]).group_by("matricule").agg(
                    pl.col("Statut").first().alias("Statut")
                )
                summary = summary.join(statut_df, on="matricule", how="left")
            
            return summary.sort("moyenne_salaire", descending=True), hb
    
    # Sinon, identifier depuis le panel directement via min_bareme/max_bareme
    if "min_bareme" in panel.columns and "max_bareme" in panel.columns:
        hb = panel.filter(
            (pl.col("min_bareme").is_not_null()) | (pl.col("max_bareme").is_not_null())
        ).with_columns(
            pl.when(pl.col("montant_brut_bareme") < pl.col("min_bareme"))
              .then(pl.lit("Inférieur au minimum du barème"))
            .when(pl.col("montant_brut_bareme") > pl.col("max_bareme"))
              .then(pl.lit("Supérieur au maximum du barème"))
            .otherwise(pl.lit("Conforme"))
            .alias("Statut")
        )
        
        cols_summary = ["matricule", "annee", "mois", "GRADE", "CODE_ORGANISME", 
                        "organisme", "CODE_EMPLOI", "emploi", "montant_brut_bareme", 
                        "Statut", "min_bareme", "max_bareme"]
        cols_summary = [c for c in cols_summary if c in hb.columns]
        
        summary = hb.select(cols_summary).group_by("matricule").agg([
            pl.col("annee").min().alias("annee_premiere"),
            pl.col("annee").max().alias("annee_derniere"),
            pl.col("annee").n_unique().alias("nb_annees"),
            pl.col("mois").n_unique().alias("nb_mois"),
            pl.col("GRADE").first().alias("GRADE"),
            pl.col("CODE_ORGANISME").first().alias("CODE_ORGANISME"),
            pl.col("organisme").first().alias("organisme"),
            pl.col("CODE_EMPLOI").first().alias("CODE_EMPLOI"),
            pl.col("emploi").first().alias("emploi"),
            pl.col("montant_brut_bareme").mean().alias("moyenne_salaire"),
            pl.col("Statut").first().alias("Statut"),
        ])
        
        return summary.sort("moyenne_salaire", descending=True), hb
    
    # Si rien n'est disponible
    return None, None

# ============================================================
# PAGE STREAMLIT
# ============================================================

st.title("📊 Dashboard — Individus Hors Barème")
st.markdown("Visualisation de l'évolution des salaires des agents détectés hors barème ANSTAT")

# Sidebar - Filtres
st.sidebar.header("Filtres")

# Chargement des données
with st.spinner("Chargement des données..."):
    try:
        panel = load_panel_complet()
        hb_file_exists = True
        
        # Essayer de charger le fichier HB
        hb_data_test = load_agents_hors_bareme()
        if hb_data_test is None:
            hb_file_exists = False
            st.warning("⚠️ Fichier agents_hors_bareme.parquet non trouvé - détection depuis panel")
        
        result = get_individus_hors_bareme(panel, hb_file_exists)
        
        if result[0] is not None:
            summary, hb_data = result
            st.success(f"✓ Données chargées : {len(panel):,} lignes panel")
            if hb_data is not None:
                st.success(f"✓ {len(hb_data):,} lignes identifiées hors barème")
        else:
            st.error("Aucun individu hors barème détecté")
            st.stop()
            
    except Exception as e:
        st.error(f"Erreur chargement données : {e}")
        st.stop()

# Statistiques globales
st.markdown("### 📈 Vue d'ensemble")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Individus concernés", len(summary))
with col2:
    st.metric("Lignes dans le panel", len(panel))
with col3:
    st.metric("Années couvertes", f"{panel['annee'].min()} - {panel['annee'].max()}")
with col4:
    if "Statut" in summary.columns:
        st.metric("Type de hors barème", summary["Statut"].first())

# Distribution par statut
if hb_data is not None and "Statut" in hb_data.columns:
    fig_statut = px.histogram(
        hb_data, 
        x="Statut", 
        title="Distribution par statut",
        color="Statut",
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    st.plotly_chart(fig_statut, use_container_width=True)

# Section : Sélection d'un individu
st.markdown("---")
st.markdown("### 🔍 Analyse individuelle")

# Filtres dans la sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("### Filtres")

grade_filter = "Tous"
org_filter = "Tous"

# Filtre par grade
if "GRADE" in summary.columns:
    grades = ["Tous"] + sorted(summary["GRADE"].drop_nulls().unique().to_list())
    grade_filter = st.sidebar.selectbox("Grade", grades)

# Filtre par organisme
if "organisme" in summary.columns:
    organismes = ["Tous"] + sorted(summary["organisme"].drop_nulls().unique().to_list())
    org_filter = st.sidebar.selectbox("Organisme", organismes)

# Appliquer les filtres
filtered_summary = summary.clone()
if grade_filter != "Tous" and "GRADE" in filtered_summary.columns:
    filtered_summary = filtered_summary.filter(pl.col("GRADE") == grade_filter)
if org_filter != "Tous" and "organisme" in filtered_summary.columns:
    filtered_summary = filtered_summary.filter(pl.col("organisme") == org_filter)

# Afficher le nombre d'individus filtrés
st.sidebar.markdown(f"**{len(filtered_summary)} individu(s)** après filtre")

# Sélecteur d'individu
individus_liste = filtered_summary["matricule"].to_list()

if len(individus_liste) == 0:
    st.warning("Aucun individu correspondant aux filtres")
    st.stop()

selected_individu = st.selectbox(
    "Sélectionnez un individu (matricule)",
    individus_liste,
    format_func=lambda x: f"{x} - {filtered_summary.filter(pl.col('matricule') == x).select('GRADE').item()} - {filtered_summary.filter(pl.col('matricule') == x).select('organisme').item()}"
)

# ============================================================
# Affichage du détail pour l'individu sélectionné
# ============================================================

if selected_individu:
    # Récupérer l'historique complet de l'individu dans le panel
    cle_panel = "matricule" if "matricule" in panel.columns else "cle_unique"
    
    histo_individu = panel.filter(pl.col(cle_panel) == selected_individu)
    
    # Trier par période
    histo_individu = histo_individu.with_columns(
        (pl.col("annee") * 100 + pl.col("mois")).alias("periode_tri")
    ).sort("periode_tri")
    
    # Créer une date pour l'axe X
    histo_individu = histo_individu.with_columns(
        (pl.col("annee").cast(pl.Utf8) + "-" + pl.col("mois").cast(pl.Utf8).str.zfill(2)).alias("periode_label")
    )
    
    # Récupérer les infos hors barème pour cet individu si disponibles
    min_bareme_val = None
    max_bareme_val = None
    
    if hb_data is not None and "matricule" in hb_data.columns:
        hb_individu = hb_data.filter(pl.col("matricule") == selected_individu)
        if "min_bareme" in hb_individu.columns:
            min_vals = hb_individu["min_bareme"].drop_nulls().to_list()
            max_vals = hb_individu["max_bareme"].drop_nulls().to_list()
            min_bareme_val = min_vals[0] if min_vals else None
            max_bareme_val = max_vals[0] if max_vals else None
    elif "min_bareme" in histo_individu.columns:
        min_vals = histo_individu["min_bareme"].drop_nulls().to_list()
        max_vals = histo_individu["max_bareme"].drop_nulls().to_list()
        min_bareme_val = min_vals[0] if min_vals else None
        max_bareme_val = max_vals[0] if max_vals else None
    
    # Afficher les infos de l'individu
    st.markdown(f"#### Individu : {selected_individu}")
    
    col_info1, col_info2, col_info3, col_info4 = st.columns(4)
    with col_info1:
        st.markdown(f"**Grade:** {histo_individu['GRADE'].first()}")
    with col_info2:
        st.markdown(f"**Organisme:** {histo_individu['organisme'].first()}")
    with col_info3:
        st.markdown(f"**Emploi:** {histo_individu['emploi'].first()}")
    with col_info4:
        st.markdown(f"**Période:** {histo_individu['annee'].min()} - {histo_individu['annee'].max()}")
    
    # Graphique principal : Evolution du salaire
    fig = go.Figure()
    
    # Courbe du salaire brut
    fig.add_trace(go.Scatter(
        x=histo_individu["periode_label"].to_list(),
        y=histo_individu["montant_brut_bareme"].to_list(),
        mode="lines+markers",
        name="Salaire brut",
        line=dict(color="#1f77b4", width=2),
        marker=dict(size=8)
    ))
    
    # Ajouter les lignes de référence min/max barème (si disponibles)
    if min_bareme_val is not None:
        fig.add_hline(y=min_bareme_val, line_dash="dash", line_color="green", 
                     annotation_text=f"Min barème: {min_bareme_val:,.0f}", annotation_position="bottom right")
    if max_bareme_val is not None:
        fig.add_hline(y=max_bareme_val, line_dash="dash", line_color="red",
                     annotation_text=f"Max barème: {max_bareme_val:,.0f}", annotation_position="top right")
    
    fig.update_layout(
        title=f"Évolution du salaire - {selected_individu}",
        xaxis_title="Période (AAAA-MM)",
        yaxis_title="Salaire brut (FCFA)",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Tableau détaillé
    st.markdown("#### Détail par période")
    
    cols_a_afficher = ["periode_label", "annee", "mois", "GRADE", "organisme", "emploi", 
                       "montant_brut_bareme", "NB_POSTES" if "NB_POSTES" in histo_individu.columns else None,
                       "min_bareme" if "min_bareme" in histo_individu.columns else None,
                       "max_bareme" if "max_bareme" in histo_individu.columns else None]
    cols_a_afficher = [c for c in cols_a_afficher if c is not None and c in histo_individu.columns]
    
    df_display = histo_individu.select(cols_a_afficher).to_pandas()
    st.dataframe(df_display, use_container_width=True, hide_index=True)
    
    # Analyser les variations
    st.markdown("#### 💡 Analyse des variations")
    
    histo_with_diff = histo_individu.with_columns(
        pl.col("montant_brut_bareme").diff().alias("variation")
    ).sort("periode_tri")
    
    # Afficher les mois avec forte variation
    variations = histo_with_diff.filter(pl.col("variation").is_not_null() & (pl.col("variation").abs() > 10000))
    
    if len(variations) > 0:
        st.markdown("**Mois avec variation significative (> 10 000 FCFA) :**")
        var_df = variations.select(["periode_label", "variation", "montant_brut_bareme"]).to_pandas()
        st.dataframe(var_df, use_container_width=True, hide_index=True)
    else:
        st.info("Aucune variation significative détectée.")

# ============================================================
# BAS DE PAGE
# ============================================================

st.markdown("---")
st.markdown("""
*Dashboard généré à partir des données Panel Admin - Panel Complet*
""")

# Pour exécuter : streamlit run app_hors_bareme_dashboard.py