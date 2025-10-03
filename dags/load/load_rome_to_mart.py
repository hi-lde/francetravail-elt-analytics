from __future__ import annotations

from airflow.decorators import dag, task
from pendulum import datetime
import json, hashlib
from include.global_variables import global_variables as gv
from include.clients.minio_client import get_minio_client
import duckdb

MINIO = get_minio_client()

def _stable_id(prefix: str, label: str) -> str:
    h = hashlib.md5((prefix + "|" + (label or "")).encode("utf-8")).hexdigest()
    return f"{prefix}_{h}"

@dag(
    start_date=datetime(2025, 9, 1),
    schedule=[gv.DS_ROME_JOB_DATA_MINIO],
    catchup=False,
    max_active_runs=1,
    default_args=gv.default_args,
    tags=["load","rome","duckdb","mart"],
    description="Load ROME all-in-one JSON (MinIO) -> dim_activite SCD2 + référentiels & bridges"
)
def load_rome_to_mart():

    @task(pool="duckdb")
    def upsert_from_minio():
        con = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)

        # Fichier écrit par in_rome_job_list
        obj = "list_rome_job.json"
        raw = MINIO.get_object(gv.ROME_JOB_BUCKET_NAME, obj).read()
        payload = json.loads(raw)

        fiches = payload.get("fiches") if isinstance(payload, dict) else payload
        if not isinstance(fiches, list):
            con.close()
            return "no_fiches"

        for f in fiches:
            f = f or {}
            # codes et libellés robustes
            code = f.get("codeMetier") or f.get("code") or f.get("rome") or f.get("codeROME")
            lib  = (f.get("libelleMetier")
                    or f.get("libelle")
                    or f.get("intitule")
                    or (f.get("metier") or {}).get("libelle"))
            if not code:
                continue

            # --- dim_activite : SCD2 léger
            row = con.execute("""
                SELECT sk_activite, lib_activite
                FROM mart.dim_activite
                WHERE type_activite='ROME' AND code_activite=? AND is_current=TRUE
            """, (code,)).fetchone()

            if row is None:
                con.execute("""
                    INSERT INTO mart.dim_activite
                      (type_activite, code_activite, lib_activite, dat_maj_source, is_current)
                    VALUES ('ROME', ?, ?, NOW(), TRUE)
                """, (code, lib))
            else:
                sk, lib_cur = row
                if lib and lib_cur != lib:
                    # clôture ancienne version
                    con.execute("""
                        UPDATE mart.dim_activite
                           SET valid_to = CURRENT_DATE - INTERVAL 1 DAY, is_current = FALSE
                         WHERE sk_activite = ?;
                    """, (sk,))
                    # nouvelle version
                    con.execute("""
                        INSERT INTO mart.dim_activite
                          (type_activite, code_activite, lib_activite, dat_maj_source, is_current)
                        VALUES ('ROME', ?, ?, NOW(), TRUE)
                    """, (code, lib))

            # --- appellations
            for a in (f.get("appellations") or []):
                lib_a = (a or {}).get("libelle") or a.get("intitule") or a.get("label")
                if not lib_a:
                    continue
                app_id = a.get("id") or a.get("code") or _stable_id("app", lib_a)

                con.execute("""
                    INSERT INTO mart.dim_appellation (appellation_id, appellation_libelle)
                    SELECT ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM mart.dim_appellation WHERE appellation_id=?
                    );
                """, (app_id, lib_a, app_id))

                con.execute("""
                    INSERT OR IGNORE INTO mart.bridge_metier_appellation (code_rome, appellation_id)
                    VALUES (?, ?);
                """, (code, app_id))

            # --- compétences clés
            for c in (f.get("competencesCles") or []):
                lib_c = (c or {}).get("libelle") or c.get("intitule") or c.get("label")
                if not lib_c:
                    continue
                comp_id = c.get("id") or _stable_id("compcle", lib_c)

                con.execute("""
                    INSERT INTO mart.dim_competence (competence_id, competence_libelle)
                    SELECT ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM mart.dim_competence WHERE competence_id=?
                    );
                """, (comp_id, lib_c, comp_id))

                con.execute("""
                    INSERT OR IGNORE INTO mart.bridge_metier_competence (code_rome, competence_id)
                    VALUES (?, ?);
                """, (code, comp_id))

            # --- groupes de compétences mobilisées
            for g in (f.get("groupesCompetencesMobilisees") or []):
                for c in (g.get("competences") or []):
                    lib_c = (c or {}).get("libelle") or c.get("intitule") or c.get("label")
                    if not lib_c:
                        continue
                    comp_id = c.get("id") or _stable_id("comp", lib_c)

                    con.execute("""
                        INSERT INTO mart.dim_competence (competence_id, competence_libelle)
                        SELECT ?, ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM mart.dim_competence WHERE competence_id=?
                        );
                    """, (comp_id, lib_c, comp_id))

                    con.execute("""
                        INSERT OR IGNORE INTO mart.bridge_metier_competence (code_rome, competence_id)
                        VALUES (?, ?);
                    """, (code, comp_id))

        con.close()
        return "ok"

    upsert_from_minio()

load_rome_to_mart()
