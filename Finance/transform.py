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
from datetime import datetime

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
    print("Nettoyage des données terminé.")

    # Mapping des status de paiement :
    df.loc[df["Statut Commande"] == "Annulée", "Statut Paiement Facture"] = (
        "En attente d'annulation"
    )
    df.loc[df["Statut Commande"] == "En cours", "Statut Paiement Facture"] = (
        "En attente de Livraison"
    )

    # Création des tranches de risques :
    today = datetime.now()
    jours = (today - df["Date Comptable"]).dt.days

    # Créer les tranches
    df["Tranche Comptable"] = pd.cut(
        jours,
        bins=[0, 30, 60, 90, float("inf")],
        labels=["0-30 jours", "31-60 jours", "61-90 jours", "+90 jours"],
    )

    for col in date_columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d")

    # Exportation des données nettoyées :
    output_path = os.path.join(script_dir, "Data", "extracted_data.csv")
    df.to_csv(output_path, index=False)

    print("Données nettoyées exportées avec succès.")
    return df
