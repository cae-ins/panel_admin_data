# ============================================================
# PANEL ADMIN — ÉTAPE 3 : BRONZE → SILVER ICEBERG
# ============================================================
# Adapté de : 02_consolidation_complete_2015_2025.R
#              (partie enrichissement + transformation)
#
# Lit la table Bronze, applique toute la logique métier :
#   - Mapping des colonnes brutes → noms standardisés (mapper_colonnes)
#   - Normalisation et filtrage des situations administratives
#   - Enrichissement avec les codes ANSTAT (organisme, grade, emploi...)
#   - Cast des montants en numérique
#
# Table source  : nessie.bronze.panel_admin_solde_mensuel
# Table produite : nessie.silver.panel_admin_solde_mensuel
#
# Dépendances :
#   install.packages(c("sparklyr", "dplyr", "data.table", "stringi",
#                      "readxl", "aws.s3", "dotenv", "httr"))
# ============================================================

library(sparklyr)
library(dplyr)
library(data.table)
library(stringi)
library(readxl)
library(aws.s3)
library(dotenv)
library(httr)

httr::set_config(httr::config(ssl_verifypeer = FALSE, connecttimeout = 60, timeout = 600))

load_dot_env(file = ".env")

# --- CONFIGURATION ---

#LORSQU'ON TRAVAILLE DEPUIS SA MACHINE LOCAL
MINIO_ENDPOINT   <- "http://192.168.1.230:30137"
MINIO_ACCESS_KEY <- "datalab-team"
MINIO_SECRET_KEY <- "minio-datalabteam123"
NESSIE_URI       <- "http://192.168.1.230:30604/api/v1"

#LORSQU'ON TRAVAILLE SUR JHUB
# MINIO_ENDPOINT   <- "http://minio.mon-namespace.svc.cluster.local:80"
# MINIO_ACCESS_KEY <- "datalab-team"
# MINIO_SECRET_KEY <- "minio-datalabteam123"
# NESSIE_URI       <- "http://nessie.trino.svc.cluster.local:19120/api/v1"

#---------------------------------------------------------------------------------

BUCKET         <- "staging"
PREFIX_REFS    <- "panel_admin/references"
TABLE_BRONZE   <- "nessie.bronze.panel_admin_solde_mensuel"
TABLE_SILVER   <- "nessie.silver.panel_admin_solde_mensuel"

endpoint_propre <- sub("^https?://", "", MINIO_ENDPOINT)

Sys.setenv(
  AWS_ACCESS_KEY_ID     = MINIO_ACCESS_KEY,
  AWS_SECRET_ACCESS_KEY = MINIO_SECRET_KEY,
  AWS_DEFAULT_REGION    = "us-east-1"
)

# ============================================================
# FONCTIONS MÉTIER (extraites du script original — identiques)
# ============================================================

normaliser_pour_matching <- function(texte) {
  if (is.null(texte) || length(texte) == 0) return(NA_character_)
  texte <- as.character(texte)
  texte[is.na(texte)] <- ""
  texte <- trimws(texte)
  texte <- gsub("-", " ", texte, fixed = TRUE)
  texte <- gsub("\\.", " ", texte)
  texte <- gsub("'|'|'", " ", texte)
  texte <- gsub("[,;:/()\\[\\]]", " ", texte)
  texte <- toupper(texte)
  texte <- stri_trans_general(texte, "Latin-ASCII")
  texte <- gsub("\\s+", " ", texte)
  texte <- trimws(texte)
  texte[texte == ""] <- NA_character_
  return(texte)
}

normaliser_nom_colonne <- function(nom) {
  if (is.na(nom) || nom == "") return("colonne_vide")
  nom <- gsub("^[A-Z_]+[0-9]+\\.", "", nom)
  nom <- toupper(nom)
  nom <- gsub("[-/_ ]", "_", nom)
  nom <- gsub("\\.", "_", nom)
  nom <- stri_trans_general(nom, "Latin-ASCII")
  nom <- gsub("[^A-Z0-9_]", "", nom)
  nom <- gsub("_+", "_", nom)
  return(trimws(nom, whitespace = "_"))
}

mapper_colonnes <- function(noms_colonnes) {
  noms_normalises <- sapply(noms_colonnes, normaliser_nom_colonne)

  # Table de correspondance : nom_standard → patterns regex à matcher
  mapping <- list(
    cle_unique             = c("MATRICULE.*CODE.*ORGANISME", "MATRICULE.*\\|\\|.*CODE"),
    matricule              = c("^MATRICULE$", "^MATRIC$"),
    nom                    = c("^NOM$", "^NOM_PRENOM$", "^NOM_ET_PRENOM$"),
    date_naissance         = c("DATE.*NAISSANCE", "DATE_NAISS", "NAISSANCE"),
    sexe                   = c("^SEXE$"),
    situation_matrimoniale = c("SITUATION.*MATRIMONIALE", "STATUT.*MATRIMONIAL"),
    nombre_enfant          = c("NOMBRE.*ENFANT", "NBR.*ENFANT"),
    situation              = c("^SITUATION$", "SITUATION_ADMINISTRATIVE"),
    date_debut_situation   = c("DATE.*DEBUT.*SITUATION", "DATE_SITUATION"),
    montant_brut           = c("MONTANT_BRUT", "SALAIRE_BRUT", "REMUNERATION_BRUTE", "^BRUT$"),
    montant_net            = c("MONTANT_NET", "SALAIRE_NET", "REMUNERATION_NETTE", "^NET$"),
    retenue_pension        = c("RETENUE.*PENSION", "^PENSION$", "COTISATION.*PENSION"),
    impot                  = c("^IMPOT$", "^IGR$", "IMPOT.*REVENU"),
    charge_patronale       = c("CHARGE.*PATRONALE", "CHARGES.*PATRONALES"),
    organisme              = c("^ORGANISME$", "MINISTERE", "DIRECTION"),
    lieu_affectation       = c("LIEU.*AFFECTATION", "^AFFECTATION$"),
    service                = c("^SERVICE$"),
    emploi                 = c("^EMPLOI$", "^CORPS$", "CORPS.*EMPLOI"),
    fonction               = c("^FONCTION$"),
    grade                  = c("CLASSE.*ECHELON", "CLASSE_ECHELON"),
    statut_fonctionnaire   = c("^GRADE$"),
    poste                  = c("^POSTE$", "LIBELLE.*POSTE"),
    prise_service          = c("PRISE.*SERVICE", "DATE.*PRISE.*SERVICE"),
    date_retraite          = c("DATE.*RETRAITE"),
    age_retraite           = c("AGE.*RETRAITE"),
    mois_annee             = c("MOIS.*ANNEE", "^PERIODE$")
  )

  correspondance <- data.frame(
    nom_source   = noms_colonnes,
    nom_standard = NA_character_,
    stringsAsFactors = FALSE
  )

  for (i in 1:nrow(correspondance)) {
    nom_norm <- noms_normalises[i]
    for (std_name in names(mapping)) {
      for (pattern in mapping[[std_name]]) {
        if (grepl(pattern, nom_norm, ignore.case = TRUE)) {
          correspondance$nom_standard[i] <- std_name
          break
        }
      }
      if (!is.na(correspondance$nom_standard[i])) break
    }
    if (is.na(correspondance$nom_standard[i])) {
      correspondance$nom_standard[i] <- tolower(gsub("_", "_", nom_norm))
    }
  }

  return(correspondance)
}

normaliser_situation <- function(situation) {
  if (is.na(situation) || situation == "") return("autre")
  sit_norm <- toupper(trimws(as.character(situation)))
  sit_norm <- stri_trans_general(sit_norm, "Latin-ASCII")
  sit_norm <- gsub("\\s+", " ", sit_norm)

  situations_valides <- list(
    en_activite      = c("EN ACTIVITE", "ACTIVITE", "EN ACTIVITÉ", "ACTIVITÉ"),
    regul_indemnites = c("REGUL. INDEMNITES", "REGUL INDEMNITES", "REGULARISATION INDEMNITES"),
    demi_solde       = c("DEMI-SOLDE", "DEMI SOLDE", "DEMISOLDE", "1/2 SOLDE")
  )

  for (categorie in names(situations_valides)) {
    if (sit_norm %in% situations_valides[[categorie]]) return(categorie)
  }
  return("autre")
}

# ============================================================
# CHARGEMENT DES TABLES DE CODES ANSTAT (depuis staging)
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CHARGEMENT : Tables de codes ANSTAT (depuis staging)\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

objets_ref <- aws.s3::get_bucket_df(
  bucket    = BUCKET,
  prefix    = PREFIX_REFS,
  base_url  = endpoint_propre,
  region    = "",
  use_https = FALSE
)
fichier_anstat_key <- objets_ref$Key[grepl("ANSTAT_CODE", objets_ref$Key, ignore.case = TRUE)][1]

tmp_anstat <- tempfile(fileext = ".xlsx")
aws.s3::save_object(
  object    = fichier_anstat_key,
  bucket    = BUCKET,
  file      = tmp_anstat,
  base_url  = endpoint_propre,
  region    = "",
  use_https = FALSE
)
cat(sprintf("✓ Fichier ANSTAT téléchargé : %s\n\n", basename(fichier_anstat_key)))

charger_table_codes <- function(sheet_name, libelle_col_names, col_jointure) {
  dt <- as.data.table(read_excel(tmp_anstat, sheet = sheet_name))
  for (lcn in libelle_col_names) {
    if (lcn %in% names(dt)) { setnames(dt, lcn, "libelle"); break }
  }
  if (!"libelle" %in% names(dt)) stop(sprintf("Colonne libellé non trouvée dans %s", sheet_name))
  dt[, libelle_norm := normaliser_pour_matching(libelle)]
  code_col <- names(dt)[1]
  list(table = dt, col_code = code_col, col_jointure_source = col_jointure)
}

dictionnaires_codes <- list(
  CODE_AFFECTATION    = charger_table_codes("lieu affectation",
                          c("LIBELLÉ_LIEU_AFFECTATION", "LIBELLE_LIEU_AFFECTATION"),
                          "lieu_affectation"),
  CODE_ORGANISME      = charger_table_codes("ORGANISME_OK",
                          c("LIBELLÉ_ORGANISME", "LIBELLE_ORGANISME"),
                          "organisme"),
  CODE_POSITION_SOLDE = charger_table_codes("CODE_SITUATION_SOLDE",
                          c("LIBELLÉ_POSITION_SOLDE", "LIBELLE_SITUATION_SOLDE",
                            "LIBELLÉ_SITUATION_SOLDE"),
                          "situation"),
  CODE_EMPLOI         = charger_table_codes("HISTORIQUE_ECHELLES_CORPS",
                          c("LIBELLÉ_EMPLOI", "LIBELLE_EMPLOI"),
                          "emploi"),
  CODE_SERVICE        = charger_table_codes("SERVICE",
                          c("LIBELLÉ_SERVICE", "LIBELLE_SERVICE"),
                          "service"),
  CODE_FONCTION       = charger_table_codes("FONCTION",
                          c("LIBELLÉ_FONCTION", "LIBELLE_FONCTION"),
                          "fonction"),
  CODE_GRADE          = charger_table_codes("GRADE",
                          c("LIBELLÉ_GRADE", "LIBELLE_GRADE"),
                          "grade"),
  CODE_POSTE          = charger_table_codes("LIBELLE POSTE",
                          c("LIBELLÉ_POSTE", "LIBELLE_POSTE"),
                          "poste")
)

for (nom_code in names(dictionnaires_codes)) {
  cat(sprintf("  %-25s : %d codes\n", nom_code,
              nrow(dictionnaires_codes[[nom_code]]$table)))
}
unlink(tmp_anstat)

# ============================================================
# CONNEXION SPARK
# ============================================================

config <- spark_config()
config$spark.driver.memory <- "16g"
config$spark.jars.packages <- paste(
  "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
  "org.apache.hadoop:hadoop-aws:3.3.4",
  "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1",
  sep = ","
)
config$spark.sql.extensions <- paste(
  "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
  "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
  sep = ","
)
config$spark.sql.catalog.nessie                  <- "org.apache.iceberg.spark.SparkCatalog"
config$`spark.sql.catalog.nessie.catalog-impl`   <- "org.apache.iceberg.nessie.NessieCatalog"
config$spark.sql.catalog.nessie.uri              <- NESSIE_URI
config$spark.sql.catalog.nessie.ref              <- "main"
config$spark.sql.catalog.nessie.warehouse        <- "s3a://bronze/"
config$spark.hadoop.fs.s3a.endpoint                    <- MINIO_ENDPOINT
config$spark.hadoop.fs.s3a.access.key                  <- MINIO_ACCESS_KEY
config$spark.hadoop.fs.s3a.secret.key                  <- MINIO_SECRET_KEY
config$spark.hadoop.fs.s3a.path.style.access           <- "true"
config$spark.hadoop.fs.s3a.impl                        <- "org.apache.hadoop.fs.s3a.S3AFileSystem"
config$`spark.hadoop.fs.s3a.aws.credentials.provider`  <- "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

sc <- spark_connect(master = "local", config = config)

# ============================================================
# LECTURE BRONZE ET TRANSFORMATION
# ============================================================

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("TRANSFORMATION : Bronze → Silver\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# Lecture Bronze (collect en data.table pour réutiliser la logique R)
cat("Lecture de la table Bronze (collect)...\n")
dt_bronze <- as.data.table(collect(spark_read_table(sc, TABLE_BRONZE)))
cat(sprintf("✓ Bronze : %s lignes × %d colonnes\n\n",
            format(nrow(dt_bronze), big.mark = ","), ncol(dt_bronze)))

# --- MAPPING DES COLONNES ---
cat("Application de mapper_colonnes()...\n")
correspondance <- mapper_colonnes(names(dt_bronze))

# Gérer les doublons dans les noms standards
noms_std <- correspondance$nom_standard
doublons <- noms_std[duplicated(noms_std)]
if (length(doublons) > 0) {
  for (nd in unique(doublons)) {
    idx <- which(noms_std == nd)
    for (k in seq_along(idx)) {
      if (k > 1) noms_std[idx[k]] <- paste0(nd, "_dup", k)
    }
  }
  correspondance$nom_standard <- noms_std
}

setnames(dt_bronze,
         old = correspondance$nom_source,
         new = correspondance$nom_standard,
         skip_absent = TRUE)

cat(sprintf("✓ %d colonnes mappées\n\n", nrow(correspondance)))

# --- NORMALISATION DES SITUATIONS ---
cat("Normalisation des situations administratives...\n")

if ("situation" %in% names(dt_bronze)) {
  dt_bronze[, situation_brute      := situation]
  dt_bronze[, situation_normalisee := sapply(situation, normaliser_situation)]

  avant   <- nrow(dt_bronze)
  dt_bronze <- dt_bronze[situation_normalisee %in% c("en_activite", "regul_indemnites", "demi_solde")]
  apres   <- nrow(dt_bronze)
  cat(sprintf("✓ Filtrage : %s → %s lignes (%.1f%% conservé)\n\n",
              format(avant, big.mark = ","),
              format(apres, big.mark = ","),
              100 * apres / avant))
}

# --- ENRICHISSEMENT PAR CODES ANSTAT ---
cat("Enrichissement avec les codes ANSTAT...\n")

for (nom_code in names(dictionnaires_codes)) {
  config_code    <- dictionnaires_codes[[nom_code]]
  col_jointure   <- config_code$col_jointure_source
  table_codes    <- config_code$table
  col_code       <- config_code$col_code

  if (!col_jointure %in% names(dt_bronze)) {
    dt_bronze[, (nom_code) := NA_character_]
    cat(sprintf("  %-25s : colonne source absente\n", nom_code))
    next
  }

  dt_bronze[, col_norm_tmp := normaliser_pour_matching(get(col_jointure))]

  table_match <- table_codes[!is.na(libelle_norm),
                              .(libelle_norm, code = get(col_code))]
  table_match <- table_match[!duplicated(libelle_norm)]

  dt_bronze <- merge(dt_bronze, table_match,
                     by.x = "col_norm_tmp", by.y = "libelle_norm",
                     all.x = TRUE)
  setnames(dt_bronze, "code", nom_code)
  dt_bronze[, col_norm_tmp := NULL]

  taux <- 100 * sum(!is.na(dt_bronze[[nom_code]])) / nrow(dt_bronze)
  cat(sprintf("  %-25s : %.1f%% matchés\n", nom_code, taux))
}
cat("\n")

# --- CAST DES TYPES NUMÉRIQUES ---
cat("Cast des montants en numérique...\n")

cols_numeriques <- c("montant_brut", "montant_net", "retenue_pension",
                     "impot", "charge_patronale")
for (col in intersect(cols_numeriques, names(dt_bronze))) {
  dt_bronze[, (col) := as.numeric(get(col))]
}

# Codes numériques (colonnes F2 : codes à chiffres uniquement)
cols_codes_num <- grep("^[0-9]+", names(dt_bronze), value = TRUE)
for (col in cols_codes_num) {
  dt_bronze[, (col) := as.numeric(get(col))]
}
cat(sprintf("✓ %d colonnes numériques castées\n\n",
            length(intersect(cols_numeriques, names(dt_bronze))) + length(cols_codes_num)))

# ============================================================
# ÉCRITURE EN SILVER
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat(sprintf("ÉCRITURE : %s\n", TABLE_SILVER))
cat(paste(rep("=", 70), collapse = ""), "\n\n")

cat(sprintf("Silver : %s lignes × %d colonnes\n",
            format(nrow(dt_bronze), big.mark = ","), ncol(dt_bronze)))

df_silver_spark <- copy_to(sc, dt_bronze, overwrite = TRUE)

DBI::dbExecute(sc, "CREATE NAMESPACE IF NOT EXISTS nessie.silver")
spark_write_table(df_silver_spark, TABLE_SILVER,
                  mode   = "overwrite",
                  format = "iceberg")

cat(sprintf("\n✓ TABLE SILVER ÉCRITE : %s\n", TABLE_SILVER))
cat("Prochaine étape : exécuter 04_validation_silver.R\n")

spark_disconnect(sc)
