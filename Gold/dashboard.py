# ============================================================
# PANEL ADMIN — DASHBOARD MASSE SALARIALE
# ============================================================
# Déploiement JupyterHub :
#   streamlit run dashboard.py \
#     --server.port 8501 \
#     --server.baseUrlPath /user/<username>/proxy/8501
#
# Dépendances :
#   pip install streamlit polars boto3 python-dotenv plotly
# ============================================================

import io
import os

import boto3
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv(".env")

# ── CONFIGURATION ────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")
BUCKET_GOLD      = "gold"
KEY_COMPLET      = "panel_admin/panel_complet.parquet"

PALETTE = [
    "#1A3A5C", "#2E6DA4", "#4A9FD4", "#7EC8E3",
    "#C8A951", "#E07B39", "#9B4F96", "#3DAA72",
    "#E84855", "#6B7FA3",
]

CRITERES_LABELS = {
    "GRADE":                "Grade",
    "sexe_std":             "Sexe",
    "organisme":            "Organisme",
    "situation_normalisee": "Situation administrative",
    "Code_CITP":            "Code CITP",
    "tranche_age":          "Tranche d'âge",
    "lieu_affectation":     "Lieu d'affectation",
    "emploi":               "Emploi",
}

# ── PAGE CONFIG ──────────────────────────────────────────────
st.set_page_config(
    page_title="Masse Salariale · Panel Admin",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── STYLES ───────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.main .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }

[data-testid="stSidebar"] { background: #0F2236; border-right: 1px solid #1E3A56; }
[data-testid="stSidebar"] > div { padding-top: 1.5rem; }
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stMarkdown p { color: #8BAAC8 !important; font-size:11px !important; font-weight:500 !important; text-transform:uppercase !important; letter-spacing:0.06em !important; }

.dash-header { display:flex; align-items:baseline; gap:12px; margin-bottom:1.5rem; padding-bottom:1rem; border-bottom:1px solid #E2E8F0; }
.dash-title  { font-size:20px; font-weight:600; color:#0F2236; letter-spacing:-0.02em; }
.dash-sub    { font-size:13px; color:#8892A0; }

.kpi-card { background:white; border-radius:6px; padding:16px 20px; border-top:3px solid #1A3A5C; box-shadow:0 1px 3px rgba(0,0,0,0.06); margin-bottom:8px; }
.kpi-card.g { border-top-color:#3DAA72; }
.kpi-card.o { border-top-color:#E07B39; }
.kpi-card.b { border-top-color:#4A9FD4; }
.kpi-label  { font-size:10px; font-weight:600; color:#8892A0; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:6px; }
.kpi-value  { font-family:'IBM Plex Mono',monospace; font-size:22px; font-weight:500; color:#0F2236; }
.kpi-delta  { font-size:11px; margin-top:3px; color:#8892A0; }
.up   { color:#3DAA72 !important; }
.down { color:#E84855 !important; }

.sec { font-size:11px; font-weight:600; color:#8892A0; text-transform:uppercase; letter-spacing:0.08em; margin:1.5rem 0 0.75rem; padding-bottom:6px; border-bottom:1px solid #EEF0F5; }
</style>
""", unsafe_allow_html=True)


# ── CLIENT S3 ────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_s3():
    return boto3.client(
        "s3",
        endpoint_url          = MINIO_ENDPOINT,
        aws_access_key_id     = MINIO_ACCESS_KEY,
        aws_secret_access_key = MINIO_SECRET_KEY,
        config                = Config(
            signature_version = "s3v4",
            retries           = {"max_attempts": 3, "mode": "adaptive"},
            connect_timeout   = 30, read_timeout = 300,
        ),
        region_name = "us-east-1",
        verify      = False,
    )


# ── CHARGEMENT ───────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def charger_panel() -> pl.DataFrame:
    s3  = get_s3()
    buf = io.BytesIO()
    s3.download_fileobj(BUCKET_GOLD, KEY_COMPLET, buf)
    buf.seek(0)
    df = pl.read_parquet(buf)

    # Extraction annee / mois depuis mois_annee (MMYYYY)
    df = df.with_columns([
        pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("annee"),
        pl.col("mois_annee").str.slice(0, 2).cast(pl.Int32, strict=False).alias("mois"),
    ]).filter(pl.col("annee").is_between(2010, 2030))

    # Tranche d'âge
    if "date_naissance" in df.columns:
        df = df.with_columns(
            pl.col("date_naissance").cast(pl.Utf8).str.slice(0, 4)
              .cast(pl.Int32, strict=False).alias("_naiss")
        ).with_columns(
            pl.when(pl.col("_naiss").is_null()).then(pl.lit("Inconnu"))
            .when((2024 - pl.col("_naiss")) < 30).then(pl.lit("< 30 ans"))
            .when((2024 - pl.col("_naiss")) < 40).then(pl.lit("30–39 ans"))
            .when((2024 - pl.col("_naiss")) < 50).then(pl.lit("40–49 ans"))
            .when((2024 - pl.col("_naiss")) < 60).then(pl.lit("50–59 ans"))
            .otherwise(pl.lit("60 ans +"))
            .alias("tranche_age")
        ).drop("_naiss")
    else:
        df = df.with_columns(pl.lit("N/A").alias("tranche_age"))

    # Sexe normalisé
    if "sexe" in df.columns:
        df = df.with_columns(
            pl.col("sexe").cast(pl.Utf8).str.to_uppercase().str.strip_chars()
            .map_elements(
                lambda s: "Homme" if s in {"MASCULIN","M","H","HOMME","1"}
                     else "Femme" if s in {"FEMININ","F","FEMME","FÉMININ","2"}
                     else "Inconnu",
                return_dtype=pl.Utf8
            ).alias("sexe_std")
        )
    else:
        df = df.with_columns(pl.lit("N/A").alias("sexe_std"))

    # Colonne salaire unifiée
    if "montant_brut" in df.columns:
        df = df.with_columns(pl.col("montant_brut").cast(pl.Float64, strict=False).alias("salaire"))
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("salaire"))

    return df


# ── UTILITAIRES ──────────────────────────────────────────────
def fmt(n, suf=""):
    if n is None: return "—"
    if abs(n) >= 1e9: return f"{n/1e9:.1f} Md{suf}"
    if abs(n) >= 1e6: return f"{n/1e6:.1f} M{suf}"
    if abs(n) >= 1e3: return f"{n/1e3:.1f} k{suf}"
    return f"{n:,.0f}{suf}"

def delta_html(now, prev):
    if not prev: return "", ""
    d = (now - prev) / abs(prev) * 100
    s = "▲" if d >= 0 else "▼"
    return f"{s} {abs(d):.1f}% vs an préc.", "up" if d >= 0 else "down"

def kpi(label, value, delta="", dcls="", acc=""):
    d = f'<div class="kpi-delta {dcls}">{delta}</div>' if delta else ""
    return f'<div class="kpi-card {acc}"><div class="kpi-label">{label}</div><div class="kpi-value">{value}</div>{d}</div>'

def theme(fig, h=360):
    fig.update_layout(
        height=h, paper_bgcolor="white", plot_bgcolor="#F8F9FC",
        font_family="IBM Plex Sans", font_color="#3D4F61",
        margin=dict(l=16, r=16, t=40, b=16),
        legend=dict(orientation="h", y=1.02, x=0, font_size=11),
        xaxis=dict(gridcolor="#EEF0F5", linecolor="#DEE2E8", tickfont_size=11),
        yaxis=dict(gridcolor="#EEF0F5", linecolor="#DEE2E8", tickfont_size=11),
        colorway=PALETTE,
    )
    return fig

def agr(df, groups_extra=[]):
    groups = ["annee", "mois"] + groups_extra
    cols_agg = [
        pl.len().alias("effectif"),
        pl.col("salaire").sum().alias("masse"),
        pl.col("salaire").mean().round(0).alias("moyen"),
        pl.col("salaire").median().round(0).alias("median"),
        pl.col("salaire").quantile(0.25).round(0).alias("p25"),
        pl.col("salaire").quantile(0.75).round(0).alias("p75"),
    ]
    if "matricule" in df.columns:
        cols_agg.append(pl.col("matricule").n_unique().alias("agents"))
    return (
        df.filter(pl.col("salaire").is_not_null() & (pl.col("salaire") > 0))
        .group_by(groups).agg(cols_agg).sort(groups)
        .with_columns(
            (pl.col("annee").cast(pl.Utf8) + "-" +
             pl.col("mois").cast(pl.Utf8).str.zfill(2)).alias("periode")
        )
    )

def appliquer_filtre(df, col, sel):
    if col in df.columns and sel and "Tous" not in sel:
        df = df.filter(pl.col(col).is_in(sel))
    return df

def top_vals(df, col, n=15):
    if col not in df.columns: return []
    return (
        df.filter(pl.col("salaire") > 0)
        .group_by(col).agg(pl.len().alias("n"))
        .sort("n", descending=True).head(n)[col].to_list()
    )


# ── CHARGEMENT DONNÉES ───────────────────────────────────────
with st.spinner("Chargement du panel depuis MinIO…"):
    try:
        df_raw = charger_panel()
    except Exception as e:
        st.error(f"Erreur de chargement : {e}")
        st.stop()

annees  = sorted(df_raw["annee"].drop_nulls().unique().to_list())
a_min, a_max = min(annees), max(annees)
criteres_dispo = {k: v for k, v in CRITERES_LABELS.items() if k in df_raw.columns}


# ── SIDEBAR ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 📊 Panel Admin")
    st.markdown(
        f"<p style='color:#5A7A9A;font-size:11px;'>{len(df_raw):,} obs · {a_min}–{a_max}</p>",
        unsafe_allow_html=True,
    )
    st.markdown("---")
    st.markdown("**PÉRIODE**")
    annees_sel = st.select_slider("Années", options=annees,
                                  value=(a_max-1, a_max), label_visibility="collapsed")
    st.markdown("---")
    st.markdown("**FILTRES**")

    def sidebar_multi(label, col, max_vals=120):
        opts = ["Tous"] + sorted(df_raw[col].drop_nulls().unique().to_list()[:max_vals]) \
               if col in df_raw.columns else ["Tous"]
        return st.multiselect(label, opts, default=["Tous"])

    f_grade  = sidebar_multi("Grade",              "GRADE")
    f_sexe   = sidebar_multi("Sexe",               "sexe_std")
    f_org    = sidebar_multi("Organisme",          "organisme", 150)
    f_sit    = sidebar_multi("Situation admin.",   "situation_normalisee")
    f_citp   = sidebar_multi("Code CITP",          "Code_CITP", 100)
    f_age    = sidebar_multi("Tranche d'âge",      "tranche_age")
    f_lieu   = sidebar_multi("Lieu d'affectation", "lieu_affectation", 100)
    f_emploi = sidebar_multi("Emploi",             "emploi", 100)

    st.markdown("---")
    st.markdown("**VENTILATION GRAPHIQUES**")
    ventilation = st.selectbox(
        "Ventiler par", ["Aucune"] + list(criteres_dispo.keys()),
        format_func=lambda x: criteres_dispo.get(x, "— Aucune"),
        label_visibility="collapsed",
    )


# ── APPLICATION DES FILTRES ──────────────────────────────────
df = df_raw.filter(pl.col("annee").is_between(annees_sel[0], annees_sel[1]))
df = appliquer_filtre(df, "GRADE",                f_grade)
df = appliquer_filtre(df, "sexe_std",             f_sexe)
df = appliquer_filtre(df, "organisme",            f_org)
df = appliquer_filtre(df, "situation_normalisee", f_sit)
df = appliquer_filtre(df, "Code_CITP",            f_citp)
df = appliquer_filtre(df, "tranche_age",          f_age)
df = appliquer_filtre(df, "lieu_affectation",     f_lieu)
df = appliquer_filtre(df, "emploi",               f_emploi)

df_sal = df.filter(pl.col("salaire").is_not_null() & (pl.col("salaire") > 0))

# Période précédente (pour deltas)
df_prev = df_raw.filter(
    pl.col("annee").is_between(annees_sel[0]-1, annees_sel[1]-1)
).filter(pl.col("salaire").is_not_null() & (pl.col("salaire") > 0))


# ── HEADER ───────────────────────────────────────────────────
periode_label = str(annees_sel[0]) if annees_sel[0]==annees_sel[1] else f"{annees_sel[0]}–{annees_sel[1]}"
st.markdown(
    f'<div class="dash-header">'
    f'<span class="dash-title">Masse Salariale · Panel Administratif</span>'
    f'<span class="dash-sub">{periode_label} · {len(df):,} observations</span>'
    f'</div>', unsafe_allow_html=True
)


# ── KPIs ─────────────────────────────────────────────────────
masse   = float(df_sal["salaire"].sum())   if len(df_sal) else 0
moyen   = float(df_sal["salaire"].mean())  if len(df_sal) else 0
median  = float(df_sal["salaire"].median()) if len(df_sal) else 0
n_lig   = len(df_sal)
n_ag    = df_sal["matricule"].n_unique() if "matricule" in df_sal.columns else n_lig

m_prev  = float(df_prev["salaire"].sum())  if len(df_prev) else 0
mo_prev = float(df_prev["salaire"].mean()) if len(df_prev) else 0

d_m, dc_m   = delta_html(masse, m_prev)
d_mo, dc_mo = delta_html(moyen, mo_prev)

c1,c2,c3,c4,c5 = st.columns(5)
for col, html in zip(
    [c1,c2,c3,c4,c5],
    [
        kpi("Masse salariale",  fmt(masse,  " FCFA"), d_m,  dc_m,  ""),
        kpi("Salaire moyen",    fmt(moyen,  " FCFA"), d_mo, dc_mo, "b"),
        kpi("Salaire médian",   fmt(median, " FCFA"), "",   "",    ""),
        kpi("Lignes de paie",   fmt(n_lig),           "",   "",    "o"),
        kpi("Agents uniques",   fmt(n_ag),            "",   "",    "g"),
    ]
):
    col.markdown(html, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ── ONGLETS ──────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📅  Vue mensuelle",
    "📈  Évolution temporelle",
    "📦  Distribution",
    "🔢  Tableau détail",
])

vent_col = [ventilation] if ventilation != "Aucune" and ventilation in df.columns else []


# ═══════════════════════════════════════════════════════════
# TAB 1 — VUE MENSUELLE
# ═══════════════════════════════════════════════════════════
with tab1:
    agr_m = agr(df, vent_col)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<div class="sec">Masse salariale mensuelle</div>', unsafe_allow_html=True)
        if vent_col:
            fig = px.bar(agr_m.to_pandas(), x="periode", y="masse", color=ventilation,
                         barmode="stack", color_discrete_sequence=PALETTE,
                         labels={"periode":"","masse":"FCFA"})
        else:
            fig = px.bar(agr_m.to_pandas(), x="periode", y="masse",
                         color_discrete_sequence=["#1A3A5C"],
                         labels={"periode":"","masse":"FCFA"})
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(theme(fig), use_container_width=True)

    with c2:
        st.markdown('<div class="sec">Effectif mensuel</div>', unsafe_allow_html=True)
        if vent_col:
            fig = px.area(agr_m.to_pandas(), x="periode", y="effectif", color=ventilation,
                          color_discrete_sequence=PALETTE, labels={"periode":"","effectif":"Lignes"})
        else:
            fig = px.area(agr_m.to_pandas(), x="periode", y="effectif",
                          color_discrete_sequence=["#2E6DA4"], labels={"periode":"","effectif":"Lignes"})
        fig.update_traces(line_width=1.5)
        st.plotly_chart(theme(fig), use_container_width=True)

    c3, c4 = st.columns(2)
    agr_g = agr(df)  # sans ventilation pour ces deux graphiques

    with c3:
        st.markdown('<div class="sec">Salaire moyen vs médian</div>', unsafe_allow_html=True)
        fig = go.Figure([
            go.Scatter(x=agr_g["periode"].to_list(), y=agr_g["moyen"].to_list(),
                       name="Moyen", mode="lines+markers",
                       line=dict(color="#1A3A5C", width=2), marker=dict(size=4)),
            go.Scatter(x=agr_g["periode"].to_list(), y=agr_g["median"].to_list(),
                       name="Médian", mode="lines+markers",
                       line=dict(color="#E07B39", width=2, dash="dot"), marker=dict(size=4)),
        ])
        st.plotly_chart(theme(fig), use_container_width=True)

    with c4:
        st.markdown('<div class="sec">Fourchette P25–P75</div>', unsafe_allow_html=True)
        p = agr_g.to_pandas()
        fig = go.Figure([
            go.Scatter(
                x=p["periode"].tolist() + p["periode"].tolist()[::-1],
                y=p["p75"].tolist() + p["p25"].tolist()[::-1],
                fill="toself", fillcolor="rgba(74,159,212,0.15)",
                line=dict(color="rgba(0,0,0,0)"), name="P25–P75",
            ),
            go.Scatter(x=p["periode"], y=p["median"], name="Médiane",
                       mode="lines", line=dict(color="#4A9FD4", width=2)),
        ])
        st.plotly_chart(theme(fig), use_container_width=True)


# ═══════════════════════════════════════════════════════════
# TAB 2 — ÉVOLUTION TEMPORELLE
# ═══════════════════════════════════════════════════════════
with tab2:
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        crit_evol = st.selectbox("Ventiler par", list(criteres_dispo.keys()),
                                 format_func=lambda x: criteres_dispo[x], key="ev_crit")
    with col_b:
        indic_evol = st.selectbox("Indicateur",
            ["moyen","median","masse","effectif"],
            format_func=lambda x: {"moyen":"Salaire moyen","median":"Salaire médian",
                                    "masse":"Masse salariale","effectif":"Effectif"}[x],
            key="ev_ind")
    with col_c:
        top_n = st.slider("Nb catégories", 3, 20, 8, key="ev_top")

    tv = top_vals(df, crit_evol, top_n)
    agr_ev = agr(df.filter(pl.col(crit_evol).is_in(tv)), [crit_evol])

    st.markdown(f'<div class="sec">Évolution — {criteres_dispo[crit_evol]}</div>', unsafe_allow_html=True)
    fig = px.line(agr_ev.to_pandas(), x="periode", y=indic_evol, color=crit_evol,
                  markers=True, color_discrete_sequence=PALETTE,
                  labels={"periode":"", indic_evol:"", crit_evol:""})
    fig.update_traces(line_width=2, marker_size=4)
    st.plotly_chart(theme(fig, 420), use_container_width=True)

    # Heatmap
    st.markdown('<div class="sec">Heatmap — salaire moyen par année</div>', unsafe_allow_html=True)
    heat = (
        df.filter(pl.col("salaire") > 0).filter(pl.col(crit_evol).is_in(tv))
        .group_by(["annee", crit_evol])
        .agg(pl.col("salaire").mean().round(0).alias("v"))
        .sort(["annee", crit_evol])
        .to_pandas().pivot(index=crit_evol, columns="annee", values="v")
    )
    fig_h = px.imshow(heat, color_continuous_scale=["#EEF4FB","#2E6DA4","#0F2236"],
                      aspect="auto", labels=dict(color="Moy."))
    fig_h.update_layout(height=max(250, 42*len(heat)))
    st.plotly_chart(theme(fig_h, max(250, 42*len(heat))), use_container_width=True)


# ═══════════════════════════════════════════════════════════
# TAB 3 — DISTRIBUTION
# ═══════════════════════════════════════════════════════════
with tab3:
    col_a, col_b = st.columns(2)
    with col_a:
        crit_dist = st.selectbox("Grouper par", list(criteres_dispo.keys()),
                                 format_func=lambda x: criteres_dispo[x], key="d_crit")
    with col_b:
        an_dist = st.selectbox("Année",
                               sorted(df["annee"].drop_nulls().unique().to_list(), reverse=True),
                               key="d_an")

    df_d = df.filter((pl.col("annee") == an_dist) & (pl.col("salaire") > 0))
    tv_d = top_vals(df_d, crit_dist, 15)
    df_d = df_d.filter(pl.col(crit_dist).is_in(tv_d))

    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<div class="sec">Boîtes à moustaches</div>', unsafe_allow_html=True)
        fig = px.box(df_d.to_pandas(), x=crit_dist, y="salaire", color=crit_dist,
                     color_discrete_sequence=PALETTE, points=False,
                     labels={crit_dist:"","salaire":"FCFA"})
        fig.update_traces(line_width=1.5)
        st.plotly_chart(theme(fig, 400), use_container_width=True)

    with c2:
        st.markdown('<div class="sec">Distribution en violon</div>', unsafe_allow_html=True)
        fig = px.violin(df_d.to_pandas(), x=crit_dist, y="salaire", color=crit_dist,
                        color_discrete_sequence=PALETTE, box=True,
                        labels={crit_dist:"","salaire":"FCFA"})
        st.plotly_chart(theme(fig, 400), use_container_width=True)

    # Histogramme global
    st.markdown('<div class="sec">Histogramme — densité des salaires</div>', unsafe_allow_html=True)
    fig = px.histogram(df_d.sample(min(50000, len(df_d))).to_pandas(),
                       x="salaire", color=crit_dist, nbins=80, barmode="overlay",
                       color_discrete_sequence=PALETTE, opacity=0.7,
                       labels={"salaire":"FCFA", crit_dist:""})
    fig.update_traces(marker_line_width=0)
    st.plotly_chart(theme(fig, 340), use_container_width=True)


# ═══════════════════════════════════════════════════════════
# TAB 4 — TABLEAU DÉTAIL
# ═══════════════════════════════════════════════════════════
with tab4:
    col_a, col_b = st.columns(2)
    with col_a:
        crit_tab = st.selectbox("Grouper par",
            ["(aucun)"] + list(criteres_dispo.keys()),
            format_func=lambda x: criteres_dispo.get(x, "(aucun)"),
            key="t_crit")
    with col_b:
        gran = st.selectbox("Granularité", ["Mensuelle","Annuelle"], key="t_gran")

    extra = [crit_tab] if crit_tab != "(aucun)" and crit_tab in df.columns else []

    if gran == "Mensuelle":
        agr_t = agr(df, extra)
    else:
        grps = ["annee"] + extra
        agr_t = (
            df.filter(pl.col("salaire").is_not_null() & (pl.col("salaire") > 0))
            .group_by(grps)
            .agg([
                pl.len().alias("effectif"),
                pl.col("salaire").sum().alias("masse"),
                pl.col("salaire").mean().round(0).alias("moyen"),
                pl.col("salaire").median().round(0).alias("median"),
                pl.col("salaire").quantile(0.25).round(0).alias("p25"),
                pl.col("salaire").quantile(0.75).round(0).alias("p75"),
            ])
            .sort(grps)
        )

    # Formatage colonnes monétaires pour affichage
    df_disp = agr_t.to_pandas()
    for col in ["masse","moyen","median","p25","p75"]:
        if col in df_disp.columns:
            df_disp[col] = df_disp[col].apply(lambda x: f"{x:,.0f}" if x == x else "")

    st.dataframe(df_disp, use_container_width=True, height=480)

    st.download_button(
        "⬇️  Télécharger CSV",
        data=agr_t.write_csv(),
        file_name=f"indicateurs_{annees_sel[0]}_{annees_sel[1]}.csv",
        mime="text/csv",
    )
