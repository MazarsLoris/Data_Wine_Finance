#########################
# Script Extract
#########################

"""
Ce script extrait les données financières et de vente pour analyse.
"""

#########################
# Bibliothèques
#########################

import os

import pandas as pd
from sqlalchemy import create_engine

# Gestion des chemins de fichiers :
script_dir = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(script_dir, "Data", "extracted_data.csv")
output_path = os.path.join(script_dir, "Data", "extracted_data.csv")


# Création de la fonction extract_data :
def extract_data(password):
    """
    Fonction permettant d'extraire les données financières et de vente depuis la base de données.

    arguments : Aucun
    """
    # Importation
    try:
        last_extract = pd.read_csv(data_path)
        last_extract["Date Comptable"] = pd.to_datetime(
            last_extract["Date Comptable"], errors="coerce"
        )
        last_date = last_extract["Date Comptable"].max()
    except (FileNotFoundError, pd.errors.EmptyDataError):
        last_date = None

    # Sécurité : Si last_date est NaT, None ou vide, on met une date par défaut
    if pd.isnull(last_date):
        last_date = "2020-01-01"
    else:
        # On s'assure que c'est bien formaté en texte pour le SQL
        last_date = last_date.strftime("%Y-%m-%d")

    print(f"Dernière date utilisée pour le filtre : {last_date}")

    # Connexion à la base de données :
    engine = create_engine(f"mysql+pymysql://root:{password}@localhost/db_sales")
    print("Connexion à la base de données réussie.")

    # Requête SQL pour extraire les données :
    query = f"""
    SELECT 
    -- Table commandes
    lc.cmd_id AS `Id Commande`, lc.date_cmd AS `Date Commande`, lc.client_id AS `Id Client`, lc.store_id AS `Id Magasin`, 
    lc.statut AS `Statut Commande`, lc.total_ht AS `Total HT Commande`,

    -- Table commande_details
    cd.prod_id AS `Id Produit`, cd.quantite AS `Quantite`, cd.prix_unitaire AS `Prix Unitaire`, cd.remise_pourcentage AS `Taux Remise`,
    cd.sous_total AS `Sous-Total Produit`,

    -- Table factures
    lf.facture_id AS `Id Facture`, lf.date_emission `Date Comptable`, lf.montant_ttc AS `Montant Facture TTC`, 
    lf.statut_paiement AS `Statut Paiement Facture`
    
    FROM db_sales.commandes AS lc

    LEFT JOIN db_finance.factures AS lf 
    ON lc.cmd_id = lf.cmd_id

    LEFT JOIN db_sales.commande_details AS cd 
    ON lc.cmd_id = cd.cmd_id

    WHERE lc.date_cmd > '{last_date}';
    """
    print("Requête SQL prête.")

    # Exécution de la requête et chargement des données dans un DataFrame :
    df = pd.read_sql_query(query, con=engine)
    print("Données extraites avec succès.")

    # Création du dossier  Data de sortie si nécessaire :
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Exportation des données :
    df.to_csv(output_path, index=False)
    print(f"Données extraites et sauvegardées dans {output_path}")
    return df
