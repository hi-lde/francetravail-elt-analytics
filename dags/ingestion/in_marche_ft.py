"""
Ingestion Marché du travail par métier (ROME) après fiches métiers

- Dépend du dataset des fiches métiers "ALL" (gv.DS_ROME_FICHES_ALL_RAW)
- Boucle sur tous les codes ROME extraits du dernier fichier fiches_all
- Respecte ~≤10 appels/seconde (pool + sleep configurable)
- Écrit les payloads bruts dans MinIO (prefix: marche/by_rome/<run_id>/)
- Publie gv.DS_MARCHE_RAW pour déclencher le load
"""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from __future__ import annotations

import io
import itertools
import json
import math
import os
import time
import logging
from typing import Any, Dict, Iterable, List

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from pendulum import datetime

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv
from include.custom_operators.minio import MinIOHook
from include.clients.ft_client import FTClient, FTNotFound
from include.clients.minio_client import get_minio_client


# ------------- #
# CONFIG / ENV  #
# ------------- #

# API Marché du travail (adapter à l’habilitation FT.io)
MKT_ENDPOINT = os.getenv(
    "FT_MKT_ENDPOINT",
    "stats-offres-demandes-emploi/v1/indicateur/stat-embauches",
)
# Granularité/paramétrage par défaut (peut être surchargé dans build_params)
MKT_DEFAULTS = {
    "codeTypeTerritoire": os.getenv("FT_MKT_CODE_TYPE_TERRITOIRE", "DEP"),
    "codeTerritoire": os.getenv("FT_MKT_CODE_TERRITOIRE", "44"),
    "codeTypePeriode": os.getenv("FT_MKT_TYPE_PERIODE", "TRIMESTRE"),
    "codeTypeActivite": os.getenv("FT_MKT_TYPE_ACTIVITE", "ROME"),
    "codeTypeNomenclature": os.getenv("FT_MKT_TYPE_NOMENCLATURE", "CATCANDxDUREEEMP"),
}

# Throttling & parallelisme
SUBBATCH = int(os.getenv("FT_SUBBATCH", "10"))  # nb codes par sous-manifest
API_POOL_NAME = os.getenv("FT_API_POOL", "ft_api_seq")
SLEEP_PER_CALL = float(os.getenv("FT_RATE_SLEEP", "0.11"))  # 0.11s ≈ max ~9 rps avec 10 slots
BATCH_SIZE = int(os.getenv("FT_BATCH_SIZE", "1000"))

# Préfixes MinIO
MANIFEST_PREFIX = "manifests"
OUTPUT_PREFIX = "marche/by_rome"


# --------- #
# HELPERS   #
# --------- #

def _chunked(iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, size))
        if not batch:
            break
        yield batch


def _put_json(bucket: str, key: str, data: dict | list) -> None:
    cli = MinIOHook().get_conn()
    payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
    bio = io.BytesIO(payload)
    cli.put_object(bucket, key, bio, length=len(payload), part_size=10 * 1024 * 1024)


def _get_json(bucket: str, key: str) -> Any:
    cli = MinIOHook().get_conn()
    obj = cli.get_object(bucket, key)
    try:
        return json.loads(obj.read().decode("utf-8"))
    finally:
        obj.close()
        obj.release_conn()


def _build_params(code_rome: str) -> Dict[str, Any]:
    """
    Construit les paramètres pour l’API Marché du travail.
    Adapter si nécessaire selon l’API FT.
    """
    return {
        "codeActivite": code_rome,
        **MKT_DEFAULTS,
    }


# --- DAG ---
@dag(
    start_date=datetime(2025, 9, 1),
    schedule=[gv.DS_ROME_JOB_DATA_MINIO],  # Dataset-based scheduling
    catchup=False,
    default_args=gv.default_args,
    tags=["ingestion", "france-travail", "market", "job", "minio"],
    doc_md=__doc__,
)
def in_marche_ft_by_rome():
    log = logging.getLogger("airflow.task")

    @task
    def ensure_bucket() -> None:
        """Crée le bucket cible si nécessaire (idempotent)."""
        minio = get_minio_client()  # pas d’objet global non sérialisable
        if not minio.bucket_exists(gv.JOB_MARKET_BUCKET_NAME):
            minio.make_bucket(gv.JOB_MARKET_BUCKET_NAME)
            log.info("Bucket créé: %s", gv.JOB_MARKET_BUCKET_NAME)
        else:
            log.info("Bucket déjà présent: %s", gv.JOB_MARKET_BUCKET_NAME)

    @task
    def prepare_manifests() -> List[str]:
        """
        Lit la liste des ROME depuis gv.ROME_JOB_BUCKET_NAME/list_rome_job.json
        et écrit des manifests par batches dans gv.JOB_MARKET_BUCKET_NAME.
        Retourne la liste des clés de manifests.
        """
        ctx = get_current_context()
        run_id = ctx["run_id"]

        obj = "list_rome_job.json"
        minio = get_minio_client()
        data = minio.get_object(gv.ROME_JOB_BUCKET_NAME, obj).read()
        payload = json.loads(data)

        fiches = payload.get("fiches") if isinstance(payload, dict) else payload
        codes: set[str] = set()

        if isinstance(fiches, list):
            for f in fiches:
                d = f or {}
                code = d.get("codeMetier") or d.get("code") or d.get("rome") or d.get("codeROME")
                if code:
                    codes.add(str(code).strip())

        codes_list = sorted(codes)
        codes_list = codes_list[:20] #TODO pour tests
        n = len(codes_list)
        chunks = max(1, math.ceil(n / BATCH_SIZE))
        prefix = f"{MANIFEST_PREFIX}/{run_id}/"

        manifest_keys: List[str] = []
        for i in range(chunks):
            start = i * BATCH_SIZE
            end = min(n, (i + 1) * BATCH_SIZE)
            key = f"{prefix}{i:05d}.json"
            _put_json(
                gv.JOB_MARKET_BUCKET_NAME,
                key,
                {"index": i, "total": chunks, "count": end - start, "codes": codes_list[start:end]},
            )
            manifest_keys.append(key)

        log.info("Préparé %d manifests (%d codes)", len(manifest_keys), n)
        return manifest_keys  # léger (list de str)

    @task
    def list_subbatches(manifest_key: str) -> List[str]:
        """Éclate un manifest en sous-manifests (SUBBATCH) et renvoie leurs clés."""
        manifest = _get_json(gv.JOB_MARKET_BUCKET_NAME, manifest_key)
        base = manifest_key.rsplit(".", 1)[0]  # ex: manifests/<run>/00000
        keys: List[str] = []
        for j, sub in enumerate(_chunked(manifest["codes"], SUBBATCH)):
            key = f"{base}/sub/{j:05d}.json"
            _put_json(gv.JOB_MARKET_BUCKET_NAME, key, {"codes": sub})
            keys.append(key)
        return keys

    @task
    def flatten_subkeys(all_keys: List[List[str]]) -> List[str]:
        """Aplati la liste des listes de clés (évite gros XComs)."""
        return [k for sub in (all_keys or []) for k in (sub or [])]
    
    @task(pool=API_POOL_NAME)
    def fetch_and_store_subbatch(sub_key: str) -> Dict[str, Any]:
        """
        Pour chaque code du sous-manifest :
        - construit le payload
        - appelle l’API FT
        - persiste le JSON brut dans MinIO sous OUTPUT_PREFIX/<run_id>/<code>.json
        Throttling: sleep SLEEP_PER_CALL entre chaque requête.
        """
        ctx = get_current_context()
        run_id = ctx["run_id"]

        sub = _get_json(gv.JOB_MARKET_BUCKET_NAME, sub_key)
        codes: List[str] = sub["codes"]

        ft = FTClient()
        ok = failed = skipped_404 = 0

        for code in codes:
            try:
                params = _build_params(code)
                data = ft.post(MKT_ENDPOINT, payload=params)
                out_key = f"{OUTPUT_PREFIX}/{run_id}/{code}.json"
                _put_json(gv.JOB_MARKET_BUCKET_NAME, out_key, data)
                ok += 1

            except FTNotFound as nf:
                logging.warning("Code %s: 404 Not Found -> skip. Détail: %s", code, nf)
                # (Optionnel) écrire un marqueur d'erreur pour audit/debug
                err_key = f"{OUTPUT_PREFIX}/{run_id}/errors/{code}.json"
                _put_json(
                    gv.JOB_MARKET_BUCKET_NAME,
                    err_key,
                    {"code": code, "error": "404", "message": str(nf), "endpoint": MKT_ENDPOINT},
                )
                skipped_404 += 1

            except (FTRetryable, FTHTTPError, Exception) as exc:
                # Tous les autres cas => echec (déjà retenté au besoin au niveau client)
                logging.exception("Échec pour code %s: %s", code, exc)
                failed += 1

            finally:
                time.sleep(SLEEP_PER_CALL)

        return {
            "ok": ok,
            "failed": failed,
            "skipped_404": skipped_404,
            "sub_manifest": sub_key
        }


    @task(outlets=[gv.DS_JOB_MARKET_DATA_MINIO])
    def finalize_and_publish(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Agrège les compteurs de tous les subbatches, écrit un résumé dans MinIO,
        et publie le dataset (via 'outlets').
        """
        ctx = get_current_context()
        run_id = ctx["run_id"]

        total_ok = sum(r.get("ok", 0) for r in (results or []))
        total_failed = sum(r.get("failed", 0) for r in (results or []))
        total_404 = sum(r.get("skipped_404", 0) for r in (results or []))
        summary = {
            "run_id": run_id,
            "subbatches": len(results or []),
            "ok": total_ok,
            "failed": total_failed,
            "skipped_404": total_404,
            "generated_at": time.time(),
        }

        summary_key = f"{OUTPUT_PREFIX}/{run_id}/_summary.json"
        _put_json(gv.JOB_MARKET_BUCKET_NAME, summary_key, summary)
        logging.info("Résumé écrit: s3://%s/%s", gv.JOB_MARKET_BUCKET_NAME, summary_key)

        # Le simple fait que cette task se termine avec outlets publie le Dataset
        return summary

    # -------- Orchestration --------
    ensure_bucket()

    manifest_keys = prepare_manifests()  # -> List[str]

    subkeys_lists = list_subbatches.expand(manifest_key=manifest_keys)  # mapping #1

    flat_subkeys = flatten_subkeys(subkeys_lists)

    # mapping #2
    fetch_results = fetch_and_store_subbatch.expand(sub_key=flat_subkeys)

    # tâche de "join" qui publie le dataset une fois
    finalize_and_publish(fetch_results)


in_marche_ft_by_rome()
