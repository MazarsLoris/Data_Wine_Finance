#########################
# Script Extract
#########################

"""
Ce script extrait les données des ressources humaines et de vente pour analyse.
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
    Fonction permettant d'extraire les données des ressources humaines et de vente depuis la base de données

    arguments : Aucun
    """
    last_date = None  # Sécurité pour éviter un NameError si le fichier n'existe pas

    # Importation :
    try:
        last_extract = pd.read_csv(data_path)
        last_extract["Date Comptable"] = pd.to_datetime(
            last_extract["Date Comptable"], errors="coerce"
        )
        last_date = last_extract["Date Comptable"].max()
    except (FileNotFoundError, pd.errors.EmptyDataError):
        last_extract = None

    # Sécurité : Si last_date est NaT, None ou vide, on met une date par défaut :
    if pd.isnull(last_date):
        last_date = "2020-01-01"
    else:
        # On s'assure que l'on a le bon format
        last_date = last_date.strftime("%Y-%m-%d")

    print(f"Dernière date utilisée pour le filtre : {last_date}")

    # Connexion à la base de données :
    engine = create_engine(f"mysql+pymysql://root:{password}@localhost/db_sales")
    print("Connexion à la base de données réussie.")

    # Requête SQL pour extraire les données :
    query = f"""
    SELECT 
    -- Table commandes
    cmd.cmd_id AS `Id Commande`, cmd.date_cmd AS `Date Commande`, cmd.client_id AS `Id Client`, cmd.store_id AS `Id Magasin Commande`, cmd.canal AS `Canal Vente`, cmd.statut AS `Statut Commande`,

    -- Table employes
    emp.emp_id AS `Id Employé`, emp.nom_complet AS `Nom Complet Employé`, emp.poste AS `Poste Employé`,

    -- Table magasins
    mag.nom_magasin AS `Nom Magasin`, mag.ville AS `Ville Magasin`, mag.type_magasin AS `Type Magasin`,

    -- Table commande_details
    cd.detail_id AS `Id Detail`, cd.quantite AS `Quantite`, cd.prix_unitaire AS `Prix Unitaire`, cd.remise_pourcentage AS `Taux Remise`, cd.sous_total AS `Sous-Total Produit`,

    -- Table produits
    p.prod_id AS `Id Produit`, p.nom_produit AS `Nom Produit`, p.prix_achat AS `Prix Achat Produit`,

    -- Table factures
    lf.facture_id AS `Id Facture`, lf.date_emission AS `Date Comptable`, lf.statut_paiement AS `Statut Paiement Facture`

    FROM db_sales.commandes AS cmd

    LEFT JOIN db_hr.employes AS emp 
    ON cmd.emp_id = emp.emp_id

    LEFT JOIN db_hr.magasins AS mag 
    ON cmd.store_id = mag.store_id

    LEFT JOIN db_sales.commande_details AS cd 
    ON cmd.cmd_id = cd.cmd_id

    LEFT JOIN db_sales.produits AS p 
    ON cd.prod_id = p.prod_id

    LEFT JOIN db_finance.factures AS lf 
    ON cmd.cmd_id = lf.cmd_id

    WHERE cmd.date_cmd >= '{last_date}';
    """

    print("Requête SQL prête.")

    # Exécution de la requête et chargement des données dans un DataFrame :
    df = pd.read_sql_query(query, con=engine)
    print("Données extraites avec succès.")

    # Création du dossier de sortie si nécessaire :
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Exportation des données :
    df.to_csv(output_path, index=False)
    print(f"Données extraites et sauvegardées dans {output_path}")

    return df
