"""DAG that creates the duckdb pool, initializes the mart schema and kicks off the pipeline by producing to the start dataset."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from pendulum import datetime
import os
import duckdb

from airflow.exceptions import AirflowFailException
# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # after being unpaused this DAG will run once, afterwards it can be run
    # manually with the play button in the Airflow UI
    schedule="@once",
    catchup=False,
    default_args=gv.default_args,
    description="Run this DAG to initialize DuckDB mart schema and kick off the pipeline!",
    tags=["start"],
)
def start():

    # 1) Crée le pool DuckDB (1 slot) pour sérialiser les accès DB
    create_duckdb_pool = BashOperator(
        task_id="create_duckdb_pool",
        bash_command="airflow pools list | grep -q 'duckdb' || airflow pools set duckdb 1 'Pool for duckdb'",
    )

    # 1) Crée le pool DuckDB (1 slot) pour sérialiser les accès DB
    create_ft_pool = BashOperator(
        task_id="create_ft_pool",
        bash_command="airflow pools set ft_api_seq 1 'FT API: exécuter les sous-lots en séquence'",
    )
    # 2) Exécute le DDL mart.* puis publie DS_START
    @task(task_id="init_mart_schema")
    def init_mart_schema():
        # Chemins par défaut (surchageables par env)
        ddl_path = os.getenv("MART_DDL_PATH", "/usr/local/airflow/include/sql/00_ddl_mart.sql")

        if not os.path.exists(ddl_path):
            raise FileNotFoundError(f"DDL file not found at {ddl_path}")

        sql = open(ddl_path, "r", encoding="utf-8").read()

        print(sql)
        # Exécution du DDL (idempotent)
        con = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        try:
            con.execute(sql)
            con.commit()
        finally:
            con.close()

        # Le retour de la task + outlets => produit gv.DS_START
        print(f"Initialized mart schema from {ddl_path} into duckdb")

    @task(task_id="verify_mart_tables")
    def verify_mart_tables():
        # Liste des tables attendues dans le schéma mart
        expected = {
            "dim_periode",
            "dim_territoire",
            "dim_activite",
            "dim_nomenclature",
            "dim_caracteristique",
            "fact_embauches",
            "fact_embauches_caract",
            "stg_embauches",
            "stg_embauches_caract",
            "dim_appellation",
            "dim_competence",
            "bridge_metier_appellation",
            "bridge_metier_competence",
        }


        con = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        try:
            existing = set(
                row[0]
                for row in con.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'mart' AND table_type = 'BASE TABLE'
                """).fetchall()
            )
        finally:
            con.close()

        missing = expected - existing
        extra   = existing - expected

        print(f"[DuckDB] Tables trouvées dans mart: {sorted(existing)}")
        if missing:
            raise AirflowFailException(
                f"Tables manquantes dans mart: {sorted(missing)}"
            )
        # Pas bloquant, mais utile pour l’info
        if extra:
            print(f"[DuckDB] Tables supplémentaires non listées: {sorted(extra)}")

        return f"Vérification OK ({len(expected)} tables présentes)."


    # 3) Exécute le DDL mart.* puis publie DS_START #TODO
    @task(outlets=[gv.DS_START], task_id="add_index_mart")
    def add_index_mart():
        # Chemins par défaut (surchageables par env)
        ddl_path = os.getenv("INDEX_MART_DDL_PATH", "/usr/local/airflow/include/sql/01_ddl_index_mart.sql") #TODO

        if not os.path.exists(ddl_path):
            raise FileNotFoundError(f"DDL file not found at {ddl_path}")

        sql = open(ddl_path, "r", encoding="utf-8").read()

        print(sql)

        # Exécution du DDL (idempotent)
        con = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        try:
            con.execute(sql)
            con.commit()
        finally:
            con.close()

        # Le retour de la task + outlets => produit gv.DS_START
        return f"Added index to mart from {ddl_path} into duckdb"


    @task
    def where_db():
        con = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        rows = con.execute("PRAGMA database_list").fetchall()
        print("DUCKDB_PATH (gv):", gv.DUCKDB_INSTANCE_NAME)
        print("PRAGMA database_list:", rows)  # colonne 'file' => chemin absolu réel
        con.close()

    create_duckdb_pool >> create_ft_pool >> init_mart_schema() >> add_index_mart() >> where_db() >> verify_mart_tables()


# when using the @dag decorator, the decorated function needs to be
# called after the function definition
start()
