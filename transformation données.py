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
regex_codes_occitanie_explicit = "^(9|09|11|12|30|31|32|34|46|48|65|66|81|82)[0-9]{3}$"

# Téléchargement des dernières données
TELECHARGER = False
input_path = "input/"
fichier1 = input_path + "valeursfoncieres-2023.csv"
fichier2 = input_path + "carte_loyers_maison.csv"
fichier3 = input_path + "carte_loyers_appart_12.csv"
fichier4 = input_path + "carte_loyers_appart_3plus.csv"
fichier5 = input_path + "carte_loyers_appart.csv"
fichier6 = input_path + "codes_communes.csv"
fichier7 = input_path + "statistiques_delinquance.parquet"
url1 = "http://www.data.gouv.fr/fr/datasets/r/78348f03-a11c-4a6b-b8db-2acf4fee81b1"
url2 = "http://www.data.gouv.fr/fr/datasets/r/34434cef-2f85-43b9-a601-c625ee426cb7"
url3 = "http://www.data.gouv.fr/fr/datasets/r/edadefbc-9707-45ef-a841-283608709e58"
url4 = "http://www.data.gouv.fr/fr/datasets/r/08871624-ccb5-457a-83d5-fb134cba60da"
url5 = "http://www.data.gouv.fr/fr/datasets/r/43618998-3b37-4a69-bb25-f321f1a93ed1"
url6 = "http://www.data.gouv.fr/fr/datasets/r/13d16a2f-4cbb-43d1-9057-535ad83354b8"
url7 = "http://www.data.gouv.fr/fr/datasets/r/2902fa66-cafd-47f5-9a15-196853a3ba42"


def dowload_data(url, file):
    nom_dossier, nom_fichier = file.split("/")
    if pathlib.Path(file).is_file():
        if not TELECHARGER:
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

except IOError:
    print("Erreur dans le téléchargement des données")

# Chargement des données

VF = pl.read_csv(fichier1, separator="|", infer_schema_length=10000000, decimal_comma=True)
VLM = pl.read_csv(fichier2, separator=";", encoding="latin", infer_schema_length=100000, decimal_comma=True)
VLAPP12 = pl.read_csv(fichier3, separator=";", encoding="latin", infer_schema_length=100000, decimal_comma=True)
VLAPP3PLUS = pl.read_csv(fichier4, separator=";", encoding="latin", infer_schema_length=100000, decimal_comma=True)
VLAPP = pl.read_csv(fichier5, separator=";", encoding="latin", infer_schema_length=100000, decimal_comma=True)
CC = pl.read_csv(fichier6, separator=";", encoding="latin", infer_schema_length=100000, decimal_comma=True)
DELINQUANCE = pl.read_parquet(fichier7)


# Selection des données en occitanie
VF = VF.filter(pl.col("Code departement").is_in(departements_occitanie))
VLM = VLM.filter(pl.col("DEP").is_in(departements_occitanie))
VLAPP12 = VLAPP12.filter(pl.col("DEP").is_in(departements_occitanie))
VLAPP3PLUS = VLAPP3PLUS.filter(pl.col("DEP").is_in(departements_occitanie))
VLAPP = VLAPP.filter(pl.col("DEP").is_in(departements_occitanie))
CC = CC.filter(pl.col("#Code_commune_INSEE").str.contains(regex_codes_occitanie))
DELINQUANCE = DELINQUANCE.filter(pl.col("CODGEO_2023").str.contains(regex_codes_occitanie))

# Selection des colonnes pertinentes
VF = VF[
    [
        "Valeur fonciere",
        "Code departement",
        "Code commune",
        "Code type local",
        "Type local",
        "Surface reelle bati",
        "Nombre pieces principales"
    ]
]

VLAPP = VLAPP.drop("TYPPRED", "nbobs_com", "nbobs_mail", "id_zone")
VLM = VLM.drop("TYPPRED", "nbobs_com", "nbobs_mail", "id_zone")
VLAPP12 = VLAPP12.drop("TYPPRED", "nbobs_com", "nbobs_mail", "id_zone")
VLAPP3PLUS = VLAPP3PLUS.drop("TYPPRED", "nbobs_com", "nbobs_mail", "id_zone")

# On renomme la colonne pour plus de clareté
CC = CC.rename({"#Code_commune_INSEE": "INSEE_C"})

# On enlève tous les locaux industriels et les dépendances
VF = VF.filter(pl.col("Code type local") < 3)

# On remplace le code departement et le code commune par le code insee qui nous sert pour les jointures
VF = VF.with_columns(
    pl.when(pl.col("Code commune") < 10)
    .then(pl.col("Code departement").cast(pl.String) + pl.lit("00") + pl.col("Code commune").cast(pl.String))
    .when(pl.col("Code commune") < 100)
    .then(pl.col("Code departement").cast(pl.String) + pl.lit("0") + pl.col("Code commune").cast(pl.String))
    .otherwise(pl.col("Code departement").cast(pl.String) + pl.col("Code commune").cast(pl.String))
    .alias("INSEE_C")
)

VF = VF.drop("Code commune", "Code departement")

# On rajoute une colonne pour le type de bien
VLAPP = VLAPP.with_columns(pl.lit("appartement").alias("Type_de_bien"))
VLAPP12 = VLAPP12.with_columns(pl.lit("appartement de 1 ou 2 pièces").alias("Type_de_bien"))
VLAPP3PLUS = VLAPP3PLUS.with_columns(pl.lit("appartement de 3 pièces et plus").alias("Type_de_bien"))
VLM = VLM.with_columns(pl.lit("maison").alias("Type_de_bien"))
VF = VF.with_columns(
    pl.when(pl.col("Code type local").eq(1))
    .then(pl.lit("maison"))
    .when(pl.col("Code type local").eq(2) & pl.col("Nombre pieces principales") < 3)
    .then(pl.lit("appartement de 1 ou 2 pièces"))
    .when(pl.col("Code type local").eq(2) & pl.col("Nombre pieces principales") > 2)
    .then(pl.lit("appartement de 3 pièces et plus"))
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
).agg(pl.col("valeur_fonciere_m2").mean())

VFMOYAPP = VF.filter(pl.col("Type_de_bien").str.contains("app")).group_by(
    ["INSEE_C"]
).agg(pl.lit("appartement").alias("Type_de_bien"), pl.col("valeur_fonciere_m2").mean())

VFMOY = pl.concat([VFMOY, VFMOYAPP])

# On concatène la VL et la VF et on concalcule le rendement locatif
RL = (VL.join(VFMOY, on=["INSEE_C", "Type_de_bien"], how="left")
      .with_columns(pl.col("valeur_fonciere_m2") / pl.col("loypredm2") * pl.lit(12 * 100)))

with pl.Config(tbl_cols=-1) and pl.Config(tbl_width_chars=1000) and pl.Config(tbl_rows=40):
    print(VLM.sort("INSEE_C"))
    print(VLAPP12.sort("INSEE_C"))
    print(VLAPP3PLUS.sort("INSEE_C"))
    print(VLAPP.sort("INSEE_C"))
    print(VL.sort("INSEE_C"))
    print(VF.sort("INSEE_C"))
    print(VFMOY.sort("INSEE_C"))
    print(VFMOYAPP.sort("INSEE_C"))
    print(RL.sort("INSEE_C"))
    print(CC.sort("INSEE_C"))
    print(DELINQUANCE.sort("CODGEO_2023"))

# On sauvegarde les données traitées
SAUVEGARDER = False
output_path = "output/"
output1 = output_path + "valeursfoncieres-2023.csv"
output2 = output_path + "carte_loyers_maison.csv"
output3 = output_path + "carte_loyers_appart_12.csv"
output4 = output_path + "carte_loyers_appart_3plus.csv"
output5 = output_path + "carte_loyers_appart.csv"
output6 = output_path + "codes_communes.csv"
output7 = output_path + "statistiques_delinquance.csv"


def save_data(file_output, df):
    nom_dossier, nom_fichier = file_output.split("/")
    if pathlib.Path(file_output).is_file():
        pathlib.Path(file_output).unlink()
    df.write_csv(pathlib.Path(file_output))
    print("Données enregistrées dans le dossier '" + nom_dossier + "' dans le fichier '" + nom_fichier + "'")


try:
    pathlib.Path(output_path).mkdir(exist_ok=True)
    if SAUVEGARDER:
        save_data(output1, VF)
        save_data(output2, VLM)
        save_data(output3, VLAPP12)
        save_data(output4, VLAPP3PLUS)
        save_data(output5, VLAPP)
        save_data(output6, CC)
        save_data(output7, DELINQUANCE)
    else:
        print("Données non sauvegardées")

except IOError:
    print("Erreur dans la sauvegarde des données")
