-- =========================================================
-- DuckDB - DDL #1 : création du schéma et des tables seules
-- (aucune vue, aucun index, aucun MERGE/INSERT)
-- =========================================================

PRAGMA enable_object_cache;
DROP SCHEMA IF EXISTS mart CASCADE;
-- Schéma logique du data mart
CREATE SCHEMA IF NOT EXISTS mart;

-- =========================
-- 1) Dimensions de base
-- =========================

CREATE TABLE IF NOT EXISTS mart.dim_periode (
  sk_periode      BIGINT PRIMARY KEY,
  type_periode    VARCHAR NOT NULL,       -- ex: TRIMESTRE
  code_periode    VARCHAR NOT NULL,       -- ex: 2025T1
  lib_periode     VARCHAR,
  annee           INTEGER,
  trimestre       INTEGER,
  mois            INTEGER,
  -- champs calendaires (remplis plus tard par ETL)
  date_debut      DATE,
  date_fin        DATE,
  mois_iso        VARCHAR,                -- 'YYYY-MM'
  dat_maj_source  TIMESTAMP,
  UNIQUE (type_periode, code_periode)
);

CREATE TABLE IF NOT EXISTS mart.dim_territoire (
  sk_territoire     BIGINT PRIMARY KEY,
  type_territoire   VARCHAR NOT NULL,     -- ex: DEP/REG/FR
  code_territoire   VARCHAR NOT NULL,     -- ex: 44
  lib_territoire    VARCHAR,
  niveau            VARCHAR,              -- ex: departement/region/pays
  code_parent       VARCHAR,              -- ex: code région parent
  dat_maj_source    TIMESTAMP,
  UNIQUE (type_territoire, code_territoire)
);

-- Activité (ROME/NAF/…): SCD2 léger (géré par l’ETL)
CREATE TABLE IF NOT EXISTS mart.dim_activite (
  sk_activite     BIGINT PRIMARY KEY,
  type_activite   VARCHAR NOT NULL,       -- ex: ROME
  code_activite   VARCHAR NOT NULL,       -- ex: M1811
  lib_activite    VARCHAR,
  valid_from      DATE DEFAULT CURRENT_DATE,
  valid_to        DATE,
  is_current      BOOLEAN NOT NULL DEFAULT TRUE,
  dat_maj_source  TIMESTAMP,
  UNIQUE (type_activite, code_activite, is_current) -- une version courante à la fois
);

CREATE TABLE IF NOT EXISTS mart.dim_nomenclature (
  sk_nomenclature    BIGINT PRIMARY KEY,
  code_nomenclature  VARCHAR NOT NULL,
  lib_nomenclature   VARCHAR,
  dat_maj_source     TIMESTAMP,
  UNIQUE (code_nomenclature)
);

CREATE TABLE IF NOT EXISTS mart.dim_caracteristique (
  sk_caracteristique  BIGINT PRIMARY KEY,
  type_caract         VARCHAR NOT NULL,   -- ex: TYPECTR, GENRE, AGE, NIVQUAL...
  code_caract         VARCHAR NOT NULL,   -- ex: CDI, H, 25_34, CADRE...
  lib_caract          VARCHAR,
  dat_maj_source      TIMESTAMP,
  UNIQUE (type_caract, code_caract)
);

-- =========================
-- 2) Tables de faits
-- =========================

CREATE TABLE IF NOT EXISTS mart.fact_embauches (
  sk_periode       BIGINT NOT NULL,
  sk_territoire    BIGINT NOT NULL,
  sk_activite      BIGINT NOT NULL,
  sk_nomenclature  BIGINT NOT NULL,
  nb_embauches     DECIMAL(18,2) NOT NULL,
  pct_embauches    DECIMAL(9,6),
  est_masque       BOOLEAN NOT NULL DEFAULT FALSE,
  source           VARCHAR,
  batch_id         VARCHAR,
  load_ts          TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT pk_fact_embauches PRIMARY KEY
    (sk_periode, sk_territoire, sk_activite, sk_nomenclature)
);

CREATE TABLE IF NOT EXISTS mart.fact_embauches_caract (
  sk_periode          BIGINT NOT NULL,
  sk_territoire       BIGINT NOT NULL,
  sk_activite         BIGINT NOT NULL,
  sk_nomenclature     BIGINT NOT NULL,
  sk_caracteristique  BIGINT NOT NULL,
  nb                  DECIMAL(18,2) NOT NULL,
  pct                 DECIMAL(9,6),
  est_masque          BOOLEAN NOT NULL DEFAULT FALSE,
  source              VARCHAR,
  batch_id            VARCHAR,
  load_ts             TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT pk_fact_embauches_caract PRIMARY KEY
    (sk_periode, sk_territoire, sk_activite, sk_nomenclature, sk_caracteristique)
);

-- =========================
-- 3) Staging (landing zone)
-- =========================

CREATE TABLE IF NOT EXISTS mart.stg_embauches (
  type_territoire   VARCHAR,
  code_territoire   VARCHAR,
  lib_territoire    VARCHAR,
  type_activite     VARCHAR,
  code_activite     VARCHAR,
  lib_activite      VARCHAR,
  code_nomenclature VARCHAR,
  lib_nomenclature  VARCHAR,
  type_periode      VARCHAR,
  code_periode      VARCHAR,
  lib_periode       VARCHAR,
  valeur_nombre     DECIMAL(18,2),
  valeur_pct        DECIMAL(9,6),
  est_masque        BOOLEAN,
  dat_maj           TIMESTAMP,
  source            VARCHAR,
  batch_id          VARCHAR
);

CREATE TABLE IF NOT EXISTS mart.stg_embauches_caract (
  type_territoire   VARCHAR,
  code_territoire   VARCHAR,
  type_activite     VARCHAR,
  code_activite     VARCHAR,
  code_nomenclature VARCHAR,
  type_periode      VARCHAR,
  code_periode      VARCHAR,
  type_caract       VARCHAR,
  code_caract       VARCHAR,
  lib_caract        VARCHAR,
  nombre            DECIMAL(18,2),
  pourcentage       DECIMAL(9,6),
  est_masque        BOOLEAN,
  source            VARCHAR,
  batch_id          VARCHAR
);

-- =========================
-- 4) Référentiels ROME et bridges
-- =========================

CREATE TABLE IF NOT EXISTS mart.dim_appellation (
  appellation_id      VARCHAR PRIMARY KEY,
  appellation_libelle VARCHAR
);

CREATE TABLE IF NOT EXISTS mart.dim_competence (
  competence_id       VARCHAR PRIMARY KEY,
  competence_libelle  VARCHAR
);

-- Bridges métier ↔ appellation/compétence (factless)
CREATE TABLE IF NOT EXISTS mart.bridge_metier_appellation (
  code_rome      VARCHAR NOT NULL,
  appellation_id VARCHAR NOT NULL,
  PRIMARY KEY (code_rome, appellation_id)
);

CREATE TABLE IF NOT EXISTS mart.bridge_metier_competence (
  code_rome     VARCHAR NOT NULL,
  competence_id VARCHAR NOT NULL,
  PRIMARY KEY (code_rome, competence_id)
);

CREATE SEQUENCE IF NOT EXISTS mart.seq_dim_activite START 1;
CREATE SEQUENCE IF NOT EXISTS mart.seq_sk_territoire START 1;
CREATE SEQUENCE IF NOT EXISTS mart.seq_sk_periode START 1;
CREATE SEQUENCE IF NOT EXISTS mart.seq_sk_nomenclature START 1;
CREATE SEQUENCE IF NOT EXISTS mart.seq_caracteristique START 1;

ALTER TABLE mart.dim_periode       ALTER COLUMN sk_periode       SET DEFAULT nextval('mart.seq_sk_periode');
ALTER TABLE mart.dim_territoire    ALTER COLUMN sk_territoire    SET DEFAULT nextval('mart.seq_sk_territoire');
ALTER TABLE mart.dim_nomenclature  ALTER COLUMN sk_nomenclature  SET DEFAULT nextval('mart.seq_sk_nomenclature');
ALTER TABLE mart.dim_caracteristique ALTER COLUMN sk_caracteristique SET DEFAULT nextval('mart.seq_caracteristique');
ALTER TABLE mart.dim_activite      ALTER COLUMN sk_activite      SET DEFAULT nextval('mart.seq_dim_activite');