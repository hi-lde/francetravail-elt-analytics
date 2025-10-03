-- =========================================================
-- DuckDB - DDL #2 : Indexes (compatibles DuckDB)
-- - pas d'index partiels (WHERE ...)
-- - un seul champ/expression par index
-- =========================================================

------------------------------------------------------------
-- Dimensions
------------------------------------------------------------

-- Période
CREATE INDEX IF NOT EXISTS ix_dim_periode_type_periode ON mart.dim_periode(type_periode);
CREATE INDEX IF NOT EXISTS ix_dim_periode_code_periode ON mart.dim_periode(code_periode);
CREATE INDEX IF NOT EXISTS ix_dim_periode_annee        ON mart.dim_periode(annee);
CREATE INDEX IF NOT EXISTS ix_dim_periode_trimestre    ON mart.dim_periode(trimestre);
CREATE INDEX IF NOT EXISTS ix_dim_periode_mois         ON mart.dim_periode(mois);

-- Territoire
CREATE INDEX IF NOT EXISTS ix_dim_territoire_type ON mart.dim_territoire(type_territoire);
CREATE INDEX IF NOT EXISTS ix_dim_territoire_code ON mart.dim_territoire(code_territoire);
CREATE INDEX IF NOT EXISTS ix_dim_territoire_parent ON mart.dim_territoire(code_parent);

-- Activité
CREATE INDEX IF NOT EXISTS ix_dim_activite_type ON mart.dim_activite(type_activite);
CREATE INDEX IF NOT EXISTS ix_dim_activite_code ON mart.dim_activite(code_activite);
CREATE INDEX IF NOT EXISTS ix_dim_activite_is_current ON mart.dim_activite(is_current);

-- Nomenclature
CREATE INDEX IF NOT EXISTS ix_dim_nomenclature_code ON mart.dim_nomenclature(code_nomenclature);

-- Caractéristique
CREATE INDEX IF NOT EXISTS ix_dim_caract_type ON mart.dim_caracteristique(type_caract);
CREATE INDEX IF NOT EXISTS ix_dim_caract_code ON mart.dim_caracteristique(code_caract);

------------------------------------------------------------
-- Faits
------------------------------------------------------------

-- Fact embauches
CREATE INDEX IF NOT EXISTS ix_f_emb_sk_periode       ON mart.fact_embauches(sk_periode);
CREATE INDEX IF NOT EXISTS ix_f_emb_sk_territoire    ON mart.fact_embauches(sk_territoire);
CREATE INDEX IF NOT EXISTS ix_f_emb_sk_activite      ON mart.fact_embauches(sk_activite);
CREATE INDEX IF NOT EXISTS ix_f_emb_sk_nomenclature  ON mart.fact_embauches(sk_nomenclature);

-- Fact embauches par caractéristique
CREATE INDEX IF NOT EXISTS ix_f_embc_sk_periode         ON mart.fact_embauches_caract(sk_periode);
CREATE INDEX IF NOT EXISTS ix_f_embc_sk_territoire      ON mart.fact_embauches_caract(sk_territoire);
CREATE INDEX IF NOT EXISTS ix_f_embc_sk_activite        ON mart.fact_embauches_caract(sk_activite);
CREATE INDEX IF NOT EXISTS ix_f_embc_sk_nomenclature    ON mart.fact_embauches_caract(sk_nomenclature);
CREATE INDEX IF NOT EXISTS ix_f_embc_sk_caracteristique ON mart.fact_embauches_caract(sk_caracteristique);

------------------------------------------------------------
-- Staging (lookups ETL)
------------------------------------------------------------

-- stg_embauches : index unitaires sur les clés de jointure
CREATE INDEX IF NOT EXISTS ix_stg_emb_type_territoire   ON mart.stg_embauches(type_territoire);
CREATE INDEX IF NOT EXISTS ix_stg_emb_code_territoire   ON mart.stg_embauches(code_territoire);
CREATE INDEX IF NOT EXISTS ix_stg_emb_type_activite     ON mart.stg_embauches(type_activite);
CREATE INDEX IF NOT EXISTS ix_stg_emb_code_activite     ON mart.stg_embauches(code_activite);
CREATE INDEX IF NOT EXISTS ix_stg_emb_code_nomenclature ON mart.stg_embauches(code_nomenclature);
CREATE INDEX IF NOT EXISTS ix_stg_emb_type_periode      ON mart.stg_embauches(type_periode);
CREATE INDEX IF NOT EXISTS ix_stg_emb_code_periode      ON mart.stg_embauches(code_periode);

-- stg_embauches_caract
CREATE INDEX IF NOT EXISTS ix_stg_embc_type_territoire   ON mart.stg_embauches_caract(type_territoire);
CREATE INDEX IF NOT EXISTS ix_stg_embc_code_territoire   ON mart.stg_embauches_caract(code_territoire);
CREATE INDEX IF NOT EXISTS ix_stg_embc_type_activite     ON mart.stg_embauches_caract(type_activite);
CREATE INDEX IF NOT EXISTS ix_stg_embc_code_activite     ON mart.stg_embauches_caract(code_activite);
CREATE INDEX IF NOT EXISTS ix_stg_embc_code_nomenclature ON mart.stg_embauches_caract(code_nomenclature);
CREATE INDEX IF NOT EXISTS ix_stg_embc_type_periode      ON mart.stg_embauches_caract(type_periode);
CREATE INDEX IF NOT EXISTS ix_stg_embc_code_periode      ON mart.stg_embauches_caract(code_periode);
CREATE INDEX IF NOT EXISTS ix_stg_embc_type_caract       ON mart.stg_embauches_caract(type_caract);
CREATE INDEX IF NOT EXISTS ix_stg_embc_code_caract       ON mart.stg_embauches_caract(code_caract);

------------------------------------------------------------
-- Bridges ROME
------------------------------------------------------------

CREATE INDEX IF NOT EXISTS ix_bma_rome         ON mart.bridge_metier_appellation(code_rome);
CREATE INDEX IF NOT EXISTS ix_bmc_rome         ON mart.bridge_metier_competence(code_rome);
CREATE INDEX IF NOT EXISTS ix_bma_appellation  ON mart.bridge_metier_appellation(appellation_id);
CREATE INDEX IF NOT EXISTS ix_bmc_competence   ON mart.bridge_metier_competence(competence_id);
