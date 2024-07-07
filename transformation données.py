import polars as pl
import pathlib
import urllib.request

### !!!
### Pour exécuter ce script il est nécessaire d'installer polars : pip install polars
### !!!

departements_occitanie = ["9", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"]
regex_codes_occitanie = "^(9|09|11|30|31|32|34|46|48|65|66|81|82)[0-9]{3}$"

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
url6 = "http://www.data.gouv.fr/fr/datasets/r/3062548d-f510-4ded-ba38-a64126a5331b"
url7 = "http://www.data.gouv.fr/fr/datasets/r/2902fa66-cafd-47f5-9a15-196853a3ba42"


def dowload_data(url, file):
    nom_dossier, nom_fichier = file.split("/")
    if pathlib.Path(file).is_file():
        if not TELECHARGER:
            print("Le fichier '" + nom_fichier + "' existe déjà dans le dossier '" + nom_dossier + "'. Téléchargement annulé.")
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
        "Nature mutation",
        "Valeur fonciere",
        "Commune",
        "Code departement",
        "Code commune",
        "Code type local",
        "Type local",
        "Surface reelle bati",
        "Nombre pieces principales",
        "Surface terrain"
    ]
]

# On enlève tous les locaux  industriels
VF = VF.filter(pl.col("Code type local") != 4)

# On sauvegarde les données traitées
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

    save_data(output1, VF)
    save_data(output2, VLM)
    save_data(output3, VLAPP12)
    save_data(output4, VLAPP3PLUS)
    save_data(output5, VLAPP)
    save_data(output6, CC)
    save_data(output7, DELINQUANCE)

except IOError:
    print("Erreur dans la sauvegarde des données")
