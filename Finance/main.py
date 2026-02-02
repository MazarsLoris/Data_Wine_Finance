#########################
# Script Main
#########################

"""
Ce script principal exécute les scripts d'extraction, de transformation et de chargement des données financières.
Il orchestre le flux de données de la base de données vers le fichier final prêt pour l'analyse.
"""

#########################
# Importation
#########################

# Importation des bibliothèques :
import getpass

# Importation des scripts :
import extract
from transform import clean_data

print("Début du processus ETL.")

# Étape 1 : Extraction des données
## Récuperation du nom d'utilisateur pour la connexion à la base de données :
password = getpass.getpass("Entrez le mot de passe pour la base de données : ")

extracted_data = extract.extract_data(password)
print("Étape d'extraction terminée.")

# Étape 2 : Transformation des données
transformed_data = clean_data(extracted_data)
print("Étape de transformation terminée.")
