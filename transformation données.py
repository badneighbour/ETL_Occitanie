import polars as pl
import pathlib
import urllib.request
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

### !!!
### Pour exécuter ce script il est nécessaire d'installer polars : pip install polars
### !!!

departements_occitanie = ["9", "09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"]
regex_codes_occitanie = "^(" + "|".join(departements_occitanie) + ")[0-9]{3}$"
# regex_codes_occitanie = "^(9|09|11|12|30|31|32|34|46|48|65|66|81|82)[0-9]{3}$"

# Téléchargement des dernières données
RETELECHARGER = False
input_path = "input/"
fichier1 = input_path + "valeursfoncieres-2023.csv"
fichier2 = input_path + "carte_loyers_maison.csv"
fichier3 = input_path + "carte_loyers_appart_12.csv"
fichier4 = input_path + "carte_loyers_appart_3plus.csv"
fichier5 = input_path + "carte_loyers_appart.csv"
fichier6 = input_path + "codes_communes.parquet"
fichier7 = input_path + "statistiques_delinquance.parquet"
fichier8 = input_path + "epci_occitanie.parquet"
fichier9 = input_path + "valeursfoncieres-2022.csv"
fichier10 = input_path + "valeursfoncieres-2021.csv"
url1 = "http://www.data.gouv.fr/fr/datasets/r/78348f03-a11c-4a6b-b8db-2acf4fee81b1"
url2 = "http://www.data.gouv.fr/fr/datasets/r/34434cef-2f85-43b9-a601-c625ee426cb7"
url3 = "http://www.data.gouv.fr/fr/datasets/r/edadefbc-9707-45ef-a841-283608709e58"
url4 = "http://www.data.gouv.fr/fr/datasets/r/08871624-ccb5-457a-83d5-fb134cba60da"
url5 = "http://www.data.gouv.fr/fr/datasets/r/43618998-3b37-4a69-bb25-f321f1a93ed1"
url6 = ("https://data.laregion.fr/api/explore/v2.1/catalog/datasets/departements-d-occitanie/exports/parquet?lang=fr"
        "&timezone=Europe%2FBerlin")
url7 = "http://www.data.gouv.fr/fr/datasets/r/2902fa66-cafd-47f5-9a15-196853a3ba42"
url8 = ("http://data.laregion.fr/api/explore/v2.1/catalog/datasets/intercommunalite-occitanie-milesimees/exports"
        "/parquet?lang=fr&timezone=Europe%2FBerlin")
url9 = "http://www.data.gouv.fr/fr/datasets/r/87038926-fb31-4959-b2ae-7a24321c599a"
url10 = "http://www.data.gouv.fr/fr/datasets/r/817204ac-2202-4b4a-98e7-4184d154d98c"


def dowload_data(url, file):
    nom_dossier, nom_fichier = file.split("/")
    if pathlib.Path(file).is_file():
        if not RETELECHARGER:
            print("Le fichier '"
                  + nom_fichier
                  + "' existe déjà dans le dossier '"
                  + nom_dossier
                  + "'. Téléchargement annulé."
                  )
            return
        pathlib.Path(file).unlink()
    urllib.request.urlretrieve(url, file)
    print("Le fichier '" + nom_fichier + "' a été téléchargé avec succès dans le dossier '" + nom_dossier + "' !")


try:
    pathlib.Path(input_path).mkdir(exist_ok=True)

    dowload_data(url1, fichier1)
    dowload_data(url2, fichier2)
    dowload_data(url3, fichier3)
    dowload_data(url4, fichier4)
    dowload_data(url5, fichier5)
    dowload_data(url6, fichier6)
    dowload_data(url7, fichier7)
    dowload_data(url8, fichier8)
    dowload_data(url9, fichier9)
    dowload_data(url10, fichier10)

except IOError:
    print("Erreur dans le téléchargement des données")

# Chargement des données

VF = pl.concat(
    [pl.scan_csv(f, separator="|", infer_schema_length=False, decimal_comma=True, low_memory=True).select(pl.all().exclude("^*lot$"))
     for f in [fichier1, fichier9, fichier10]]
)
VLM = pl.read_csv(fichier2, separator=";", encoding="latin", infer_schema_length=100000, decimal_comma=True)
VLAPP12 = pl.read_csv(fichier3, separator=";", encoding="latin", infer_schema_length=100000, decimal_comma=True)
VLAPP3PLUS = pl.read_csv(fichier4, separator=";", encoding="latin", infer_schema_length=100000, decimal_comma=True)
VLAPP = pl.read_csv(fichier5, separator=";", encoding="latin", infer_schema_length=100000, decimal_comma=True)
CD = pl.read_parquet(fichier6)
DELINQUANCE = pl.read_parquet(fichier7)
EPCI = pl.read_parquet(fichier8)

# Selection des données en occitanie
VF = VF.filter(pl.col("Code departement").is_in(departements_occitanie))
VLM = VLM.filter(pl.col("DEP").is_in(departements_occitanie))
VLAPP12 = VLAPP12.filter(pl.col("DEP").is_in(departements_occitanie))
VLAPP3PLUS = VLAPP3PLUS.filter(pl.col("DEP").is_in(departements_occitanie))
VLAPP = VLAPP.filter(pl.col("DEP").is_in(departements_occitanie))
DELINQUANCE = DELINQUANCE.filter(pl.col("CODGEO_2024").str.contains(regex_codes_occitanie))

# On sauvegarde les données d'occitanie
SAUVEGARDER = True
output_path = "output/"
output1 = output_path + "valeursfoncieres.csv"
output2 = output_path + "carte_loyers.csv"
output3 = output_path + "carte_loyers_appart_12.csv"
output4 = output_path + "carte_loyers_appart_3plus.csv"
output5 = output_path + "carte_loyers_appart.csv"
output6 = output_path + "codes_communes.csv"
output7 = output_path + "statistiques_delinquance.csv"


def save_data(file_output, df):
    nom_dossier, nom_fichier = file_output.split("/")
    if pathlib.Path(file_output).is_file():
        pathlib.Path(file_output).unlink()
    df.write_csv(pathlib.Path(file_output), separator=";")
    print("Données enregistrées dans le dossier '" + nom_dossier + "' dans le fichier '" + nom_fichier + "'")


try:
    pathlib.Path(output_path).mkdir(exist_ok=True)
    if SAUVEGARDER:
        save_data(output1, VF.collect())
        save_data(output2, VLM)
        save_data(output3, VLAPP12)
        save_data(output4, VLAPP3PLUS)
        save_data(output5, VLAPP)
        save_data(output7, DELINQUANCE)
    else:
        print("Données non sauvegardées")

except IOError:
    print("Erreur dans la sauvegarde des données")

VF = pl.read_csv(output1, separator=";", infer_schema_length=1000000, decimal_comma=True)

# Suppression des lignes doubles à cause de différentes cultures
VF = VF.unique(["Code departement", "Valeur fonciere", "Date mutation", "Section"])

# Selection des colonnes pertinentes
VF = VF[
    [
        "Valeur fonciere",
        "Code departement",
        "Code commune",
        "Code type local",
        "Type local",
        "Surface reelle bati",
        "Nombre pieces principales",
        "Nature mutation"
    ]
]

EPCI = EPCI[
    [
        "epci_code",
        "epci_current_code",
        "epci_name",
        "year",
    ]
]

CD = CD[["nom_officiel_departement", "code_officiel_departement"]]
VLAPP = VLAPP.drop("TYPPRED", "nbobs_com", "nbobs_mail", "id_zone", "REG")
VLM = VLM.drop("TYPPRED", "nbobs_com", "nbobs_mail", "id_zone", "REG")
VLAPP12 = VLAPP12.drop("TYPPRED", "nbobs_com", "nbobs_mail", "id_zone", "REG")
VLAPP3PLUS = VLAPP3PLUS.drop("TYPPRED", "nbobs_com", "nbobs_mail", "id_zone", "REG")


# On enlève tous les locaux industriels et les dépendances
VF = VF.filter(pl.col("Code type local") < 3)

# On remplace le code departement et le code commune par le code insee qui nous sert pour les jointures
VF = VF.with_columns(
    pl.when(pl.col("Code departement").eq(9))
    .then(pl.lit("09"))
    .otherwise(pl.col("Code departement").cast(pl.String))
    .alias("Code departement")
).with_columns(
    pl.when(pl.col("Code commune") < 10)
    .then(pl.col("Code departement").cast(pl.String) + pl.lit("00") + pl.col("Code commune").cast(pl.String))
    .when(pl.col("Code commune") < 100)
    .then(pl.col("Code departement").cast(pl.String) + pl.lit("0") + pl.col("Code commune").cast(pl.String))
    .otherwise(pl.col("Code departement").cast(pl.String) + pl.col("Code commune").cast(pl.String))
    .alias("INSEE_C")
)

VF = VF.drop("Code commune", "Code departement")

# On rajoute une colonne pour le type de bien
VLAPP = VLAPP.with_columns(pl.lit("app").alias("Type_de_bien"))
VLAPP12 = VLAPP12.with_columns(pl.lit("app12").alias("Type_de_bien"))
VLAPP3PLUS = VLAPP3PLUS.with_columns(pl.lit("app3p").alias("Type_de_bien"))
VLM = VLM.with_columns(pl.lit("maison").alias("Type_de_bien"))
VF = VF.with_columns(
    pl.when(pl.col("Code type local").eq(1))
    .then(pl.lit("maison"))
    .when(pl.col("Code type local").eq(2) & (pl.col("Nombre pieces principales") < 3))
    .then(pl.lit("app12"))
    .when((pl.col("Code type local").eq(2)) & (pl.col("Nombre pieces principales") > 2))
    .then(pl.lit("app3p"))
    .otherwise(pl.lit("autre"))
    .alias("Type_de_bien")
).drop("Code type local", "Type local")

# On fait l'union de toutes les données sur la valeur locative dans un même Dataframe
VL = pl.concat([VLM, VLAPP, VLAPP12, VLAPP3PLUS])

# On ajoute un prix au metre carré
VF = VF.with_columns((pl.col("Valeur fonciere") / pl.col("Surface reelle bati")).alias("valeur_fonciere_m2"))

# On fait les moyennes selon le type de bien
VFMOY = VF.group_by(
    ["INSEE_C", "Type_de_bien"]
).agg(
    pl.col("Valeur fonciere").median(),
    pl.col("valeur_fonciere_m2").median(),
    pl.len().alias("pondération_type_de_bien")
)

VFMOYAPP = VF.filter(pl.col("Type_de_bien").str.contains("app")).group_by(
    ["INSEE_C"]
).agg(
    pl.lit("app").alias("Type_de_bien"),
    pl.col("Valeur fonciere").median(),
    pl.col("valeur_fonciere_m2").median(),
    pl.lit(0).cast(pl.UInt32).alias("pondération_type_de_bien")
    # on met une pondération nulle aux appartements pour ne pas les compter en double
)

VFMOY = pl.concat([VFMOY, VFMOYAPP])

# On concatène la VL et la VF et on concalcule le rendement locatif et on enlève les valeurs aberrantes
RL = (VL.join(VFMOY, on=["INSEE_C", "Type_de_bien"])
      .with_columns((pl.col("loypredm2") / pl.col("valeur_fonciere_m2") * pl.lit(12)).alias("rendement_locatif"))
      ).filter(pl.col("rendement_locatif") < 0.5)

# On sélectionne les données les plus récentes de la délinquance
DELINQUANCE = DELINQUANCE.sort("annee").group_by("CODGEO_2024", "classe").last()

# On remplace les null par la moyenne dans les stastistiques de délinquance et on enlève les colonnes inutiles
DELINQUANCE = DELINQUANCE.with_columns(
    pl.col("tauxpourmille").fill_null(pl.col("complementinfotaux")),
    pl.col("faits").fill_null(pl.col("complementinfotaux") * pl.col("POP") / 1000)
).drop("complementinfoval", "valeur.publiée", "complementinfotaux", "millPOP", "millLOG", "annee", "unité.de.compte")

# On pivote les classes de faits pour faire tenir sur une ligne par ville
DELINQUANCE_PIVOT = DELINQUANCE.pivot("classe", index=["CODGEO_2024", "POP", "LOG"], values=["faits", "tauxpourmille"],
                                      aggregate_function="first")

# On pivote les types de bien pour tenir sur une seule ligne et on calcule la moyenne pondérée de tous les types de bien
RL = RL.pivot(
    "Type_de_bien", index=["INSEE_C", "EPCI", "DEP", "LIBGEO"],
    values=["Valeur fonciere", "valeur_fonciere_m2", "pondération_type_de_bien", "rendement_locatif"]
).with_columns(
    pl.col("pondération_type_de_bien_maison").fill_null(0),
    pl.col("pondération_type_de_bien_app12").fill_null(0),
    pl.col("pondération_type_de_bien_app3p").fill_null(0),
).with_columns(
    ((pl.col("Valeur fonciere_maison").fill_null(0) * pl.col("pondération_type_de_bien_maison") +
      pl.col("Valeur fonciere_app12").fill_null(0) * pl.col("pondération_type_de_bien_app12") +
      pl.col("Valeur fonciere_app3p").fill_null(0) * pl.col("pondération_type_de_bien_app3p")) /
     (pl.col("pondération_type_de_bien_maison") +
      pl.col("pondération_type_de_bien_app12") +
      pl.col("pondération_type_de_bien_app3p"))
     ).alias("valeur_fonciere_moyenne"),
    ((pl.col("valeur_fonciere_m2_maison").fill_null(0) * pl.col("pondération_type_de_bien_maison") +
      pl.col("valeur_fonciere_m2_app12").fill_null(0) * pl.col("pondération_type_de_bien_app12") +
      pl.col("valeur_fonciere_m2_app3p").fill_null(0) * pl.col("pondération_type_de_bien_app3p")) /
     (pl.col("pondération_type_de_bien_maison") +
      pl.col("pondération_type_de_bien_app12") +
      pl.col("pondération_type_de_bien_app3p"))
     ).alias("valeur_fonciere_m2_moyenne"),
    ((pl.col("rendement_locatif_maison").fill_null(0) * pl.col("pondération_type_de_bien_maison") +
      pl.col("rendement_locatif_app12").fill_null(0) * pl.col("pondération_type_de_bien_app12") +
      pl.col("rendement_locatif_app3p").fill_null(0) * pl.col("pondération_type_de_bien_app3p")) /
     (pl.col("pondération_type_de_bien_maison") +
      pl.col("pondération_type_de_bien_app12") +
      pl.col("pondération_type_de_bien_app3p"))
     ).alias("rendement_locatif_moyenne")
)

# On rajoute les noms des epcis et dep en supprimant les doublons dans la table referentiel
EPCI = EPCI.sort("year").group_by(["epci_current_code"]).last().drop("epci_current_code", "year")
RL = RL.join(EPCI, left_on="EPCI", right_on="epci_code", how="left")
RL = RL.join(
    CD, left_on="DEP", right_on="code_officiel_departement"
)

# On joint les données de la délinquance dans les données de rendement locatif
RL = RL.join(DELINQUANCE_PIVOT, left_on="INSEE_C", right_on="CODGEO_2024", how="left")

with pl.Config(tbl_cols=-1) and pl.Config(tbl_width_chars=1600) and pl.Config(tbl_rows=40):
    # print(VLM.sort("INSEE_C"))
    # print(VLAPP12.sort("INSEE_C"))
    # print(VLAPP3PLUS.sort("INSEE_C"))
    # print(VLAPP.sort("INSEE_C"))
    print(VL.sort("INSEE_C"))
    print(VF.sort("INSEE_C"))
    print(VFMOY.sort("INSEE_C"))
    print(RL.columns)
    print(RL.sort("INSEE_C"))
    # print(RL.filter(pl.col("valeur_fonciere_m2").is_not_null()).sort("EPCI"))
    # print(CC.sort("INSEE_C"))
    print(DELINQUANCE.sort("CODGEO_2024"))
    print(DELINQUANCE_PIVOT)
    # print(EPCI)

# On sauvegarde le résultat final
output_jasper = output_path + "rendement_locatif.csv"

try:
    pathlib.Path(output_path).mkdir(exist_ok=True)
    if SAUVEGARDER:
        save_data(output_jasper, RL)
    else:
        print("Données non sauvegardées")

except IOError:
    print("Erreur dans la sauvegarde des données")
