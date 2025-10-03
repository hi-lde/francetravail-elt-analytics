from airflow.decorators import dag, task
from datetime import datetime
import duckdb
from include.global_variables import global_variables as gv

@dag(start_date=datetime(2025,9,1), schedule=[gv.DS_DUCKDB_JOB_MARKET], catchup=False, tags=["transform"])
def transform_dim_periode():
    @task
    def fill_calendar():
        con = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        # Exemple: pour code '2025T1' on met mois_iso = '2025-01' ... '2025-03', dates approximées
        con.execute("""
            UPDATE mart.dim_periode
               SET mois_iso = CASE 
                  WHEN type_periode ILIKE 'TRIM%' THEN substr(code_periode,1,4) || '-' ||
                       lpad(CAST( ( (CAST(regexp_extract(code_periode, 'T(\\d+)', 1) AS INT)-1)*3 + 1 ) AS VARCHAR), 2, '0')
                  ELSE mois_iso END
        """)
        con.close()
    fill_calendar()

transform_dim_periode()
