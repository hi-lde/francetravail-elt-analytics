from __future__ import annotations

from airflow.decorators import dag, task
from pendulum import datetime
import json
from include.global_variables import global_variables as gv
from include.clients.minio_client import get_minio_client
import duckdb
import os

# --- Constantes liées à la nouvelle arborescence d'ingestion ---
# Les fichiers sont désormais écrits en payload brut sous :
#   marche/by_rome/<run_id>/<code_rome>.json
# On lit uniquement le dernier <run_id> disponible (par nom trié décroissant).
OUTPUT_PREFIX = "marche/by_rome/"


def _latest_run_prefix(minio, bucket: str) -> str | None:
    """Retourne le préfixe 'marche/by_rome/<run_id>/' le plus récent trouvé.
    On se base sur l'ordre lexicographique des dossiers <run_id>.
    """
    run_ids: set[str] = set()
    for obj in minio.list_objects(bucket, prefix=OUTPUT_PREFIX, recursive=True):
        # on attend un chemin: marche/by_rome/<run_id>/... (au moins 3 segments)
        parts = (obj.object_name or "").split("/")
        if len(parts) >= 3:
            run_ids.add(parts[2])
    if not run_ids:
        return None
    latest = sorted(run_ids)[-1]
    return f"{OUTPUT_PREFIX}{latest}/"


@dag(
    start_date=datetime(2025, 9, 1),
    schedule=[gv.DS_JOB_MARKET_DATA_MINIO],  # déclenché par l'ingestion FT
    catchup=False,
    max_active_runs=1,
    default_args=gv.default_args,
    tags=["load","marche","duckdb","mart"],
    description="Load Marché du travail JSON (MinIO) -> mart.stg_* -> dims -> facts (compatible nouveaux fichiers)"
)
def load_market_to_mart():

    @task(pool="duckdb")
    def truncate_staging():
        con = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        con.execute("DELETE FROM mart.stg_embauches;")
        con.execute("DELETE FROM mart.stg_embauches_caract;")
        con.close()

    @task(pool="duckdb")
    def stage_from_minio():
        """
        Lit les JSON bruts écrits par l'ingestion FT (OUTPUT_PREFIX/<run_id>/...) et
        les charge dans mart.stg_embauches / mart.stg_embauches_caract.
        - Le champ batch_id est alimenté avec <run_id> dérivé du chemin.
        - La source est figée à 'france_travail'.
        - **Progression détaillée dans les logs**: compte de fichiers, cadence, ETA, lignes insérées.
        """
        import time
        import logging
        from airflow.operators.python import get_current_context

        log = logging.getLogger("airflow.task")
        con = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        minio = get_minio_client()  # éviter les objets globaux non sérialisables

        # Déterminer le dernier run à charger
        latest_prefix = _latest_run_prefix(minio, gv.JOB_MARKET_BUCKET_NAME)
        if not latest_prefix:
            con.close()
            log.info("Aucun préfixe trouvé sous %s", OUTPUT_PREFIX)
            return {"loaded_files": 0, "prefix": None}

        # Paramètres de progression/transaction
        COMMIT_EVERY = int(os.getenv("STAGE_COMMIT_EVERY", "200"))
        LOG_EVERY = int(os.getenv("STAGE_LOG_EVERY", "50"))
        HARD_LIMIT = int(os.getenv("STAGE_MAX_FILES", "0"))  # 0 = pas de limite

        source = "france_travail"
        run_id = latest_prefix.rstrip("/").split("/")[-1]

        # Lister d'abord tous les fichiers pour connaître la taille et pouvoir estimer l'ETA
        all_names = [
            o.object_name
            for o in minio.list_objects(gv.JOB_MARKET_BUCKET_NAME, prefix=latest_prefix, recursive=True)
            if o.object_name.endswith(".json")
        ]
        if HARD_LIMIT > 0:
            all_names = all_names[:HARD_LIMIT]
        total_files = len(all_names)
        if total_files == 0:
            con.close()
            log.info("Aucun JSON sous le préfixe %s", latest_prefix)
            return {"loaded_files": 0, "prefix": latest_prefix}

        log.info("Stage_from_minio: %d fichiers à traiter sous %s", total_files, latest_prefix)

        # SQL paramétrés
        main_sql = """
            INSERT INTO mart.stg_embauches
            (type_territoire, code_territoire, lib_territoire,
             type_activite,  code_activite,  lib_activite,
             code_nomenclature, lib_nomenclature,
             type_periode, code_periode, lib_periode,
             valeur_nombre, valeur_pct, est_masque,
             dat_maj, source, batch_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?, ?, ?);
            """

        car_sql = """
            INSERT INTO mart.stg_embauches_caract
            (type_territoire, code_territoire,
             type_activite,  code_activite,
             code_nomenclature,
             type_periode, code_periode,
             type_caract, code_caract, lib_caract,
             nombre, pourcentage, est_masque,
             source, batch_id)
            VALUES (?,?,?,?,?, ?,?,?, ?, ?, ?, ?, ?, ?, ?);
            """

        # Compteurs pour logs
        files_done = 0
        rows_main = 0
        rows_car = 0
        t0 = time.time()
        con.execute("BEGIN TRANSACTION;")

        for idx, key in enumerate(all_names, start=1):
            obj = minio.get_object(gv.JOB_MARKET_BUCKET_NAME, key)
            try:
                data = obj.read()
            finally:
                obj.close(); obj.release_conn()
            if not data:
                continue
            try:
                payload = json.loads(data)
            except Exception:
                log.exception("JSON invalide pour %s", key)
                continue

            datMaj = (payload or {}).get("datMaj")
            liste = (payload or {}).get("listeValeursParPeriode", []) or []

            for rec in liste:
                rec = rec or {}
                con.execute(
                    main_sql,
                    [
                        rec.get("codeTypeTerritoire"), rec.get("codeTerritoire"), rec.get("libTerritoire"),
                        rec.get("codeTypeActivite"),  rec.get("codeActivite"),  rec.get("libActivite"),
                        rec.get("codeNomenclature"),  rec.get("libNomenclature"),
                        rec.get("codeTypePeriode"),   rec.get("codePeriode"),   rec.get("libPeriode"),
                        rec.get("valeurPrincipaleNombre"), rec.get("valeurSecondairePourcentage"),
                        bool(rec.get("estMasque", False)),
                        datMaj, source, run_id,
                    ],
                )
                rows_main += 1

                for c in (rec.get("listeValeurParCaract") or []):
                    c = c or {}
                    con.execute(
                        car_sql,
                        [
                            rec.get("codeTypeTerritoire"), rec.get("codeTerritoire"),
                            rec.get("codeTypeActivite"),   rec.get("codeActivite"),
                            rec.get("codeNomenclature"),
                            rec.get("codeTypePeriode"),    rec.get("codePeriode"),
                            c.get("codeTypeCaract"),       c.get("codeCaract"), c.get("libCaract"),
                            c.get("nombre"),               c.get("pourcentage"),
                            bool(c.get("estMasque", False)),
                            source, run_id,
                        ],
                    )
                    rows_car += 1

            files_done += 1

            # Commit périodique
            if files_done % COMMIT_EVERY == 0:
                con.execute("COMMIT; BEGIN TRANSACTION;")

            # Logs de progression
            if files_done % LOG_EVERY == 0 or files_done == total_files:
                dt = time.time() - t0
                rps = files_done / dt if dt > 0 else 0.0
                eta = (total_files - files_done) / rps if rps > 0 else float("inf")
                log.info(
                    "Progress %d/%d fichiers (%.1f%%) | %.2f f/s | ETA ~%.1fs | rows main=%d, car=%d",
                    files_done, total_files, 100.0 * files_done / total_files, rps, eta, rows_main, rows_car,
                )

        # Commit final
        con.execute("COMMIT;")
        con.close()
        total_time = time.time() - t0
        return {"loaded_files": files_done, "rows_main": rows_main, "rows_car": rows_car, "seconds": total_time, "prefix": latest_prefix}

    @task(pool="duckdb")
    def upsert_dimensions():
        con = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)

        # --- dim_periode
        con.execute(
            """
            INSERT INTO mart.dim_periode
              (type_periode, code_periode, lib_periode, annee, trimestre, mois, dat_maj_source)
            SELECT DISTINCT
              s.type_periode,
              s.code_periode,
              s.lib_periode,
              TRY_CAST(substr(s.code_periode,1,4) AS INTEGER) AS annee,
              CASE WHEN upper(s.type_periode) LIKE 'TRIM%' THEN
                     TRY_CAST(regexp_extract(upper(s.code_periode), 'T(\\d+)', 1) AS INTEGER)
              END AS trimestre,
              CASE WHEN upper(s.type_periode) LIKE 'MOIS%' THEN
                     TRY_CAST(substr(s.code_periode,6,2) AS INTEGER)
              END AS mois,
              MAX(s.dat_maj) OVER ()
            FROM mart.stg_embauches s
            LEFT JOIN mart.dim_periode d
              ON d.type_periode=s.type_periode AND d.code_periode=s.code_periode
            WHERE d.sk_periode IS NULL;
            """
        )

        # --- dim_territoire
        con.execute(
            """
            INSERT INTO mart.dim_territoire
              (type_territoire, code_territoire, lib_territoire, niveau, code_parent, dat_maj_source)
            SELECT DISTINCT
              s.type_territoire, s.code_territoire, s.lib_territoire,
              CASE
                WHEN upper(s.type_territoire) IN ('DEP','DEPARTEMENT') THEN 'departement'
                WHEN upper(s.type_territoire) IN ('REG','REGION') THEN 'region'
                WHEN upper(s.type_territoire) IN ('FR','PAYS','NAT') THEN 'pays'
                ELSE s.type_territoire
              END AS niveau,
              NULL,
              MAX(s.dat_maj) OVER ()
            FROM mart.stg_embauches s
            LEFT JOIN mart.dim_territoire d
              ON d.type_territoire=s.type_territoire AND d.code_territoire=s.code_territoire
            WHERE d.sk_territoire IS NULL;
            """
        )

        # --- dim_nomenclature
        con.execute(
            """
            INSERT INTO mart.dim_nomenclature
              (code_nomenclature, lib_nomenclature, dat_maj_source)
            SELECT DISTINCT s.code_nomenclature, s.lib_nomenclature, MAX(s.dat_maj) OVER ()
            FROM mart.stg_embauches s
            LEFT JOIN mart.dim_nomenclature d
              ON d.code_nomenclature=s.code_nomenclature
            WHERE d.sk_nomenclature IS NULL;
            """
        )

        # --- dim_caracteristique
        con.execute(
            """
            INSERT INTO mart.dim_caracteristique
              (type_caract, code_caract, lib_caract, dat_maj_source)
            SELECT DISTINCT c.type_caract, c.code_caract, c.lib_caract, MAX(s.dat_maj) OVER ()
            FROM mart.stg_embauches_caract c
            JOIN mart.stg_embauches s
              ON s.type_territoire=c.type_territoire AND s.code_territoire=c.code_territoire
             AND s.type_activite=c.type_activite   AND s.code_activite=c.code_activite
             AND s.code_nomenclature=c.code_nomenclature
             AND s.type_periode=c.type_periode     AND s.code_periode=c.code_periode
            LEFT JOIN mart.dim_caracteristique d
              ON d.type_caract=c.type_caract AND d.code_caract=c.code_caract
            WHERE d.sk_caracteristique IS NULL;
            """
        )

        # --- dim_activite (création si absente ; SCD2 complet côté ROME)
        con.execute(
            """
            INSERT INTO mart.dim_activite
              (type_activite, code_activite, lib_activite, dat_maj_source, is_current)
            SELECT DISTINCT s.type_activite, s.code_activite, s.lib_activite, MAX(s.dat_maj) OVER (), TRUE
            FROM mart.stg_embauches s
            LEFT JOIN mart.dim_activite d
              ON d.type_activite=s.type_activite AND d.code_activite=s.code_activite AND d.is_current=TRUE
            WHERE d.sk_activite IS NULL;
            """
        )

        con.close()

    @task(pool="duckdb", outlets=[gv.DS_DUCKDB_JOB_MARKET])
    def load_facts():
        con = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)

        # --- fact_embauches
        con.execute(
            """
            INSERT INTO mart.fact_embauches
            SELECT
              dp.sk_periode, dt.sk_territoire, da.sk_activite, dn.sk_nomenclature,
              s.valeur_nombre, s.valeur_pct, s.est_masque, s.source, s.batch_id, NOW()
            FROM mart.stg_embauches s
            JOIN mart.dim_periode    dp ON dp.type_periode=s.type_periode AND dp.code_periode=s.code_periode
            JOIN mart.dim_territoire dt ON dt.type_territoire=s.type_territoire AND dt.code_territoire=s.code_territoire
            JOIN mart.dim_activite   da ON da.type_activite=s.type_activite AND da.code_activite=s.code_activite AND da.is_current=TRUE
            JOIN mart.dim_nomenclature dn ON dn.code_nomenclature=s.code_nomenclature
            LEFT JOIN mart.fact_embauches f
              ON f.sk_periode=dp.sk_periode
             AND f.sk_territoire=dt.sk_territoire
             AND f.sk_activite=da.sk_activite
             AND f.sk_nomenclature=dn.sk_nomenclature
            WHERE f.sk_periode IS NULL;
            """
        )

        # --- fact_embauches_caract
        con.execute(
            """
            INSERT INTO mart.fact_embauches_caract
            SELECT
              dp.sk_periode, dt.sk_territoire, da.sk_activite, dn.sk_nomenclature, dc.sk_caracteristique,
              c.nombre, c.pourcentage, c.est_masque, c.source, c.batch_id, NOW()
            FROM mart.stg_embauches_caract c
            JOIN mart.dim_periode    dp ON dp.type_periode=c.type_periode AND dp.code_periode=c.code_periode
            JOIN mart.dim_territoire dt ON dt.type_territoire=c.type_territoire AND dt.code_territoire=c.code_territoire
            JOIN mart.dim_activite   da ON da.type_activite=c.type_activite AND da.code_activite=c.code_activite AND da.is_current=TRUE
            JOIN mart.dim_nomenclature dn ON dn.code_nomenclature=c.code_nomenclature
            JOIN mart.dim_caracteristique dc ON dc.type_caract=c.type_caract AND dc.code_caract=c.code_caract
            LEFT JOIN mart.fact_embauches_caract f
              ON f.sk_periode=dp.sk_periode
             AND f.sk_territoire=dt.sk_territoire
             AND f.sk_activite=da.sk_activite
             AND f.sk_nomenclature=dn.sk_nomenclature
             AND f.sk_caracteristique=dc.sk_caracteristique
            WHERE f.sk_periode IS NULL;
            """
        )

        con.close()
        return "loaded"

    truncate_staging() >> stage_from_minio() >> upsert_dimensions() >> load_facts()


load_market_to_mart()
