#########################
# Script Transform
#########################

"""
Ce script extrait importe les données extraites via le script extract.py, détermine la dernière date d'extraction, change les formats de colonnes, concate les nouvelles données avec
les anciennes et exporte le tout dans le dossier Data.
"""

#########################
# Bibliothèques
#########################

import os

import pandas as pd

# Gestion des chemins de fichiers :
script_dir = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(script_dir, "Data", "extracted_data.csv")

##########################################
# Fonctions de nettoyage du jeu de données
##########################################

# Importation des données extraites :
df = pd.read_csv(data_path)
print("Début du nettoyage des données.")


def clean_data(df):
    """
    Fonction permettant de nettoyer le jeu de données extrait par le script "extract.py".

    arguments : df - DataFrame brut extrait de la base de données.
    """
    # Changement des formats de colonnes :
    string_columns = [
        "Id Commande",
        "Id Client",
        "Id Magasin",
        "Statut Commande",
        "Id Produit",
        "Id Facture",
        "Statut Paiement Facture",
    ]

    for col in string_columns:
        df[col] = df[col].astype(str)

    # Changement des formats de dates :
    date_columns = ["Date Commande", "Date Comptable"]

    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        df[col] = df[col].dt.strftime("%Y-%m-%d")
    print("Nettoyage des données terminé.")

    # Exportation des données nettoyées :
    output_path = os.path.join(script_dir, "Data", "extracted_data.csv")
    df.to_csv(output_path, index=False)

    print("Données nettoyées exportées avec succès.")
    return df
