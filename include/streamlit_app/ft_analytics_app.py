import os
import duckdb
import streamlit as st
from typing import Tuple
import pandas as pd
import json

# ---------------------------
# Config & connection
# ---------------------------

# 1) Chemin fichier, pas dossier. Surchargable via variable d'env.
DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    "/usr/local/airflow/dwh" #TODO"   # <- fichier .duckdb explicite
)

st.set_page_config(
    page_title="France Travail – ROME × Marché du travail",
    layout="wide",
)

@st.cache_resource(show_spinner=False)
def get_con(read_only: bool = True):
    try:
        # Connexion (passe read_only=False si tu veux autoriser la création de la base)
        con = duckdb.connect(DUCKDB_PATH, read_only=read_only)
        return con
    except Exception as e:
        st.error(
            "Impossible d’ouvrir la base DuckDB.\n\n"
            f"Chemin: `{DUCKDB_PATH}`\n\n"
            f"Détail: {type(e).__name__}: {e}\n\n"
            "➡️ Vérifie que le fichier existe, que le volume est monté dans le container, "
            "et que les permissions permettent la lecture.",
            icon="🚫",
        )
        st.stop()


# CSS : élargit la sidebar ET autorise le retour à la ligne dans les options
st.markdown("""
<style>
/* largeur de la sidebar (ajuste la valeur si besoin) */
[data-testid="stSidebar"] {
  min-width: 380px;
  max-width: 380px;
}

/* autoriser le retour à la ligne dans les selectbox/combobox de la sidebar */
[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] > div {
  white-space: normal;   /* au lieu de nowrap */
}

/* idem pour les multiselect si tu en as */
[data-testid="stSidebar"] .stMultiSelect div[data-baseweb="select"] > div {
  white-space: normal;
}
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=600, show_spinner=False)
def load_filters() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    con = get_con()
    rome = con.execute("""
        SELECT code_activite AS code, COALESCE(lib_activite, code_activite) AS lib
        FROM mart.dim_activite
        WHERE type_activite='ROME' AND is_current=TRUE
        ORDER BY lib
    """).fetchdf()

    terr = con.execute("""
        SELECT DISTINCT type_territoire, code_territoire, COALESCE(lib_territoire, code_territoire) AS lib
        FROM mart.dim_territoire
        ORDER BY type_territoire, code_territoire
    """).fetchdf()

    per = con.execute("""
        SELECT code_periode, type_periode, annee, trimestre, mois
        FROM mart.dim_periode
        ORDER BY annee NULLS LAST, trimestre NULLS LAST, mois NULLS LAST, code_periode
    """).fetchdf()
    return rome, terr, per

def no_data(msg="Aucune donnée disponible."):
    st.info(msg, icon="ℹ️")

# ---------------------------
# UI – sidebar
# ---------------------------
st.title("Cartographie des embauches par métier (ROME)")

rome_df, terr_df, per_df = load_filters()
if rome_df.empty or terr_df.empty or per_df.empty:
    no_data("Le data mart est vide. Lance d’abord les DAGs d’ingestion & de load.")
    st.stop()

with st.sidebar:
    st.header("Filtres")
    rome_label = st.selectbox(
        "Métier (ROME)",
        options=rome_df["lib"],
        index=0 if not rome_df.empty else None,
    )
    sel_rome = rome_df.loc[rome_df["lib"] == rome_label, "code"].iloc[0]

    type_terr = st.selectbox(
        "Type de territoire",
        options=sorted(terr_df["type_territoire"].unique()),
        index=0,
    )
    terr_codes = terr_df.loc[terr_df["type_territoire"] == type_terr, "code_territoire"].tolist()
    code_terr = st.selectbox("Code territoire", options=terr_codes, index=0)

    # borne période
    periodes = per_df["code_periode"].tolist()
    if len(periodes) >= 2:
        p_min, p_max = st.select_slider("Période", options=periodes, value=(periodes[0], periodes[-1]))
    else:
        p_min, p_max = (periodes[0], periodes[0]) if periodes else (None, None)

# ---------------------------
# Queries helpers
# ---------------------------
@st.cache_data(ttl=600, show_spinner=False)
def q_trend(rome: str, t_type: str, t_code: str, p_min: str, p_max: str) -> pd.DataFrame:
    con = get_con()
    return con.execute("""
        SELECT dp.code_periode AS periode,
               SUM(f.nb_embauches) AS nb_embauches
        FROM mart.fact_embauches f
        JOIN mart.dim_periode dp    ON dp.sk_periode=f.sk_periode
        JOIN mart.dim_activite da   ON da.sk_activite=f.sk_activite
        JOIN mart.dim_territoire dt ON dt.sk_territoire=f.sk_territoire
        WHERE da.type_activite='ROME'
          AND da.code_activite = ?
          AND dt.type_territoire = ?
          AND dt.code_territoire = ?
          AND dp.code_periode BETWEEN ? AND ?
        GROUP BY 1
        ORDER BY 1
    """, (rome, t_type, t_code, p_min, p_max)).fetchdf()

@st.cache_data(ttl=600, show_spinner=False)
def q_part_cdi(rome: str, t_type: str, t_code: str, p_min: str, p_max: str) -> pd.DataFrame:
    con = get_con()
    return con.execute("""
        WITH base AS (
          SELECT dp.code_periode AS periode,
                 SUM(CASE WHEN dc.type_caract='TYPECTR' THEN c.nb ELSE 0 END) AS nb_ctr_total,
                 SUM(CASE WHEN dc.type_caract='TYPECTR' AND dc.code_caract='CDI' THEN c.nb ELSE 0 END) AS nb_cdi
          FROM mart.fact_embauches_caract c
          JOIN mart.dim_caracteristique dc ON dc.sk_caracteristique=c.sk_caracteristique
          JOIN mart.dim_periode dp         ON dp.sk_periode=c.sk_periode
          JOIN mart.dim_activite da        ON da.sk_activite=c.sk_activite
          JOIN mart.dim_territoire dt      ON dt.sk_territoire=c.sk_territoire
          WHERE da.type_activite='ROME'
            AND da.code_activite = ?
            AND dt.type_territoire = ?
            AND dt.code_territoire = ?
            AND dp.code_periode BETWEEN ? AND ?
          GROUP BY 1
        )
        SELECT periode,
               nb_cdi,
               nb_ctr_total,
               CASE WHEN nb_ctr_total>0 THEN nb_cdi * 1.0 / nb_ctr_total END AS part_cdi
        FROM base
        ORDER BY periode
    """, (rome, t_type, t_code, p_min, p_max)).fetchdf()

@st.cache_data(ttl=600, show_spinner=False)
def q_breakdown_latest(rome: str, t_type: str, t_code: str, p_max: str) -> pd.DataFrame:
    con = get_con()
    # répartition par type de contrat au dernier point
    return con.execute("""
        SELECT dc.code_caract AS contrat,
               SUM(c.nb) AS nb
        FROM mart.fact_embauches_caract c
        JOIN mart.dim_caracteristique dc ON dc.sk_caracteristique=c.sk_caracteristique
        JOIN mart.dim_periode dp         ON dp.sk_periode=c.sk_periode
        JOIN mart.dim_activite da        ON da.sk_activite=c.sk_activite
        JOIN mart.dim_territoire dt      ON dt.sk_territoire=c.sk_territoire
        WHERE da.type_activite='ROME'
          AND da.code_activite=?
          AND dt.type_territoire=? AND dt.code_territoire=?
          AND dp.code_periode=?
          AND dc.type_caract='TYPECTR'
        GROUP BY 1
        ORDER BY nb DESC
    """, (rome, t_type, t_code, p_max)).fetchdf()

# ---------------------------
# KPI strip
# ---------------------------
col1, col2, col3 = st.columns(3)

df_tr = q_trend(sel_rome, type_terr, code_terr, p_min, p_max)
if df_tr.empty:
    no_data("Aucune donnée d’embauches pour cette sélection.")
    st.stop()

total = float(df_tr["nb_embauches"].sum())
last  = float(df_tr["nb_embauches"].iloc[-1]) if not df_tr.empty else 0.0
prev  = float(df_tr["nb_embauches"].iloc[-2]) if len(df_tr) >= 2 else None
delta = (last - prev) / prev * 100 if prev and prev != 0 else None

with col1:
    st.metric("Total embauches", f"{int(total):,}".replace(",", " "), delta=None)
with col2:
    st.metric(f"Dernière période ({df_tr['periode'].iloc[-1]})", f"{int(last):,}".replace(",", " "),
              delta=(f"{delta:.1f} %") if delta is not None else None)
with col3:
    st.write("")

# ---------------------------
# Charts
# ---------------------------
st.subheader("Tendance des embauches")
st.line_chart(df_tr, x="periode", y="nb_embauches")

df_cdi = q_part_cdi(sel_rome, type_terr, code_terr, p_min, p_max)
st.subheader("Part de CDI")
if df_cdi.empty:
    no_data("Aucune ventilation de contrats disponible.")
else:
    st.area_chart(df_cdi[["periode", "part_cdi"]].set_index("periode"))

st.subheader(f"Répartition des contrats – {p_max}")
df_ctr = q_breakdown_latest(sel_rome, type_terr, code_terr, p_max)
if df_ctr.empty:
    no_data("Pas de détail par type de contrat sur la dernière période.")
else:
    st.bar_chart(df_ctr, x="contrat", y="nb")

# ---------------------------
# Détails (table)
# ---------------------------
with st.expander("Voir les lignes (fact_embauches)"):
    st.dataframe(df_tr, use_container_width=True)
