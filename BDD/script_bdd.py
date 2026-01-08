import os
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
fake = Faker("fr_FR")
DB_USER = "root"
DB_PASS = "LoMa1709!!"
DB_HOST = "localhost"
connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}"
engine_root = create_engine(connection_string)

print("RESET TOTAL DE L'ENVIRONNEMENT MAZARS")

# On recrée les bases proprement
with engine_root.connect() as conn:
    dbs = ["db_sales", "db_hr", "db_crm", "db_finance"]
    for db in dbs:
        conn.execute(text(f"DROP DATABASE IF EXISTS {db};"))
        conn.execute(text(f"CREATE DATABASE {db};"))
    conn.commit()

# ==============================================================================
# 2. STRUCTURE SQL
# ==============================================================================
print(" Création de la structure SQL...")

with engine_root.connect() as conn:
    # --- HR ---
    conn.execute(text("USE db_hr;"))
    conn.execute(
        text("""
        CREATE TABLE magasins (
            store_id VARCHAR(10) PRIMARY KEY,
            nom_magasin VARCHAR(100),
            ville VARCHAR(50),
            type_magasin VARCHAR(30)
        );
    """)
    )
    conn.execute(
        text("""
        CREATE TABLE employes (
            emp_id VARCHAR(10) PRIMARY KEY,
            nom_complet VARCHAR(100),
            poste VARCHAR(50),
            store_id VARCHAR(10),
            salaire_annuel INT,
            date_embauche DATE
        );
    """)
    )

    # --- CRM ---
    conn.execute(text("USE db_crm;"))
    conn.execute(
        text("""
        CREATE TABLE clients (
            client_id VARCHAR(10) PRIMARY KEY,
            nom_client VARCHAR(100),
            type_client VARCHAR(20),
            siret VARCHAR(20),
            email VARCHAR(100),
            telephone VARCHAR(50),
            ville VARCHAR(50)
        );
    """)
    )
    conn.execute(
        text("""
        CREATE TABLE avis_produits (
            avis_id INT AUTO_INCREMENT PRIMARY KEY,
            prod_id VARCHAR(10),
            note INT,
            commentaire TEXT,
            date_avis DATE
        );
    """)
    )

    # --- SALES ---
    conn.execute(text("USE db_sales;"))
    # Correction : On va remplir cette table cette fois
    conn.execute(
        text(
            "CREATE TABLE ref_cepages (code_cepage VARCHAR(3) PRIMARY KEY, nom_cepage VARCHAR(50));"
        )
    )

    conn.execute(
        text("""
        CREATE TABLE produits (
            prod_id VARCHAR(10) PRIMARY KEY, ean13 VARCHAR(13), nom_produit VARCHAR(150),
            region VARCHAR(50), appellation VARCHAR(50), type_vin VARCHAR(20),
            code_cepage VARCHAR(3), millesime INT, degre_alcool DECIMAL(4,1),
            prix_vente DECIMAL(10,2), prix_achat DECIMAL(10,2)
        );
    """)
    )

    conn.execute(
        text("""
        CREATE TABLE commandes (
            cmd_id VARCHAR(10) PRIMARY KEY, date_cmd DATETIME, client_id VARCHAR(10),
            store_id VARCHAR(10), emp_id VARCHAR(10), canal VARCHAR(20),
            statut VARCHAR(20), total_ht DECIMAL(10,2)
        );
    """)
    )

    conn.execute(
        text("""
        CREATE TABLE commande_details (
            detail_id INT AUTO_INCREMENT PRIMARY KEY, cmd_id VARCHAR(10), prod_id VARCHAR(10),
            quantite INT, prix_unitaire DECIMAL(10,2), remise_pourcentage DECIMAL(5,2),
            sous_total DECIMAL(10,2)
        );
    """)
    )

    # --- FINANCE ---
    conn.execute(text("USE db_finance;"))
    conn.execute(
        text("""
        CREATE TABLE factures (
            facture_id VARCHAR(10) PRIMARY KEY, cmd_id VARCHAR(10), date_emission DATE,
            statut_paiement VARCHAR(20), montant_ttc DECIMAL(10,2)
        );
    """)
    )
    conn.commit()

# ==============================================================================
# 3. GÉNÉRATION DES DONNÉES
# ==============================================================================
print(" Génération des données...")

# --- A. CEPAGES ---
cepages_list = [
    {"code_cepage": "MER", "nom_cepage": "Merlot"},
    {"code_cepage": "CAB", "nom_cepage": "Cabernet Sauvignon"},
    {"code_cepage": "CHA", "nom_cepage": "Chardonnay"},
    {"code_cepage": "SYR", "nom_cepage": "Syrah"},
    {"code_cepage": "XXX", "nom_cepage": "Inconnu"},  # Cas limite
]
pd.DataFrame(cepages_list).to_sql(
    "ref_cepages", engine_root, schema="db_sales", if_exists="append", index=False
)


# --- B. MAGASINS ---
CAVES = [
    ("Cave Paris - Madeleine", "Paris", "Prestige"),
    ("Cave Lyon - Bellecour", "Lyon", "Standard"),
    ("Cave Bordeaux", "Bordeau", "Flagship"),  # Faute de frappe intentionnelle
    ("Cave Marseille", "Marseille ", "Standard"),  # Espace en trop intentionnel
    ("Site Web", "Digital", "Web"),
]
magasins_data = [
    {
        "store_id": f"S{str(i + 1).zfill(3)}",
        "nom_magasin": n,
        "ville": v,
        "type_magasin": t,
    }
    for i, (n, v, t) in enumerate(CAVES)
]
pd.DataFrame(magasins_data).to_sql(
    "magasins", engine_root, schema="db_hr", if_exists="append", index=False
)


# --- C. EMPLOYES ---
employes_data = []
map_store_emp = {}
idx_emp = 1

for m in magasins_data:
    sid = m["store_id"]
    map_store_emp[sid] = []

    # Pas d'employés physiques pour le Web
    if m["type_magasin"] != "Web":
        for _ in range(random.randint(2, 5)):
            eid = f"E{str(idx_emp).zfill(4)}"
            employes_data.append(
                {
                    "emp_id": eid,
                    "nom_complet": fake.name(),
                    "poste": "Vendeur" if random.random() > 0.2 else "Responsable",
                    "store_id": sid,
                    "salaire_annuel": random.randint(22000, 45000),
                    "date_embauche": fake.date_between(
                        start_date="-5y", end_date="today"
                    ),
                }
            )
            map_store_emp[sid].append(eid)
            idx_emp += 1

employes_data.append(
    {
        "emp_id": "E9999",
        "nom_complet": "Michel Fantome",
        "poste": "Consultant",
        "store_id": None,
        "salaire_annuel": 0,
        "date_embauche": None,
    }
)

pd.DataFrame(employes_data).to_sql(
    "employes", engine_root, schema="db_hr", if_exists="append", index=False
)


# --- D. PRODUITS ---
produits_data = []
# Dictionnaire pour retrouver les prix rapidement lors des commandes
price_map = {}

for i in range(1, 151):
    prix = round(random.uniform(5.0, 150.0), 2)
    pid = f"P{str(i).zfill(5)}"

    cepage = random.choice(["MER", "CAB", "CHA", "SYR"])
    if i % 20 == 0:
        cepage = None

    p_data = {
        "prod_id": pid,
        "ean13": fake.ean13(),
        "nom_produit": f"Vin {fake.word().capitalize()} - Lot {i}",
        "region": random.choice(["Bordeaux", "Bourgogne", "Alsace", "Loire"]),
        "appellation": "AOC",
        "type_vin": random.choice(["Rouge", "Blanc", "Rosé"]),
        "code_cepage": cepage,
        "millesime": random.randint(2015, 2023),
        "degre_alcool": round(random.uniform(11.5, 15.0), 1),
        "prix_vente": prix,
        "prix_achat": round(prix * 0.55, 2),
    }
    produits_data.append(p_data)
    price_map[pid] = prix

pd.DataFrame(produits_data).to_sql(
    "produits", engine_root, schema="db_sales", if_exists="append", index=False
)


# --- E. CLIENTS ---
clients_data = []
for i in range(1, 1201):
    is_pro = random.random() < 0.15  # 15% de pros
    clients_data.append(
        {
            "client_id": f"C{str(i).zfill(5)}",
            "nom_client": fake.company() if is_pro else fake.name(),
            "type_client": "Professionnel" if is_pro else "Particulier",
            "siret": fake.siret() if is_pro else None,
            # Data Quality Issue: Des emails manquants
            "email": fake.email() if random.random() > 0.05 else None,
            "telephone": fake.phone_number(),
            "ville": fake.city(),
        }
    )

clients_data.append(clients_data[0].copy())
clients_data[-1]["client_id"] = "C99999"

pd.DataFrame(clients_data).to_sql(
    "clients", engine_root, schema="db_crm", if_exists="append", index=False
)


# --- F. TRANSACTIONS & FACTURES ---
cmd_buffer, det_buffer, fact_buffer = [], [], []
prod_ids = [p["prod_id"] for p in produits_data]
cli_ids = [c["client_id"] for c in clients_data]

start_date = datetime(2022, 1, 1)
end_date = datetime.now()
days_range = (end_date - start_date).days

print(" Génération de 5000 commandes réparties sur 3 ans...")

for i in range(1, 5001):
    cmd_id = f"CMD{str(i).zfill(6)}"

    # 1. Date aléatoire
    random_days = random.randint(0, days_range)
    date_c = start_date + timedelta(days=random_days)

    # 2. Choix Store et Vendeur
    store = random.choice(magasins_data)
    sid = store["store_id"]

    # Logique Canal
    canal = "Web" if sid == "S004" else "Boutique"

    emps = map_store_emp.get(sid, [])
    eid = random.choice(emps) if emps else None

    if canal == "Boutique" and random.random() < 0.02:
        eid = None

    client = random.choice(cli_ids)

    # 3. Détails de commande
    nb_items = random.randint(1, 5)
    total_cmd = 0

    for _ in range(nb_items):
        pid = random.choice(prod_ids)
        qty = random.randint(1, 12)
        px_unit = price_map.get(pid, 10.0)

        if random.random() < 0.001:
            px_unit = 0

        remise = 0.0
        if qty >= 6:
            remise = 0.10

        sub_total = qty * px_unit * (1 - remise)
        total_cmd += sub_total

        det_buffer.append(
            {
                "cmd_id": cmd_id,
                "prod_id": pid,
                "quantite": qty,
                "prix_unitaire": px_unit,
                "remise_pourcentage": remise * 100,
                "sous_total": round(sub_total, 2),
            }
        )

    # 4. Commande Header
    statut = random.choice(["Livrée", "En cours", "Annulée"])
    cmd_buffer.append(
        {
            "cmd_id": cmd_id,
            "date_cmd": date_c,
            "client_id": client,
            "store_id": sid,
            "emp_id": eid,
            "canal": canal,
            "statut": statut,
            "total_ht": round(total_cmd, 2),
        }
    )

    # 5. Génération Facture (Si livrée) - Correction : on peuple db_finance
    if statut == "Livrée":
        fact_buffer.append(
            {
                "facture_id": f"F{str(i).zfill(6)}",
                "cmd_id": cmd_id,
                "date_emission": date_c + timedelta(days=1),  # Facture le lendemain
                "statut_paiement": "Payée" if random.random() > 0.1 else "En attente",
                "montant_ttc": round(total_cmd * 1.20, 2),  # TVA 20%
            }
        )

# Insertion en masse
pd.DataFrame(cmd_buffer).to_sql(
    "commandes", engine_root, schema="db_sales", if_exists="append", index=False
)
pd.DataFrame(det_buffer).to_sql(
    "commande_details", engine_root, schema="db_sales", if_exists="append", index=False
)
pd.DataFrame(fact_buffer).to_sql(
    "factures", engine_root, schema="db_finance", if_exists="append", index=False
)

# ==============================================================================
# 4. GÉNÉRATION DE FICHIERS PLATS (EXCEL & CSV)
# ==============================================================================
print(" Génération des fichiers plats (CSV & Excel)...")

# Création du dossier de stockage
os.makedirs("data_files", exist_ok=True)

# --- A. FICHIER CSV : CAMPAGNES MARKETING (Sources externes) ---
# Scénario : Export d'un outil type Mailchimp ou Google Ads.
# Pièges : Séparateur point-virgule (classique en France), dates format texte.

marketing_data = []
channels = ["Email", "Facebook", "Google Ads", "Instagram", "Influenceur"]
dates_marketing = pd.date_range(start="2022-01-01", end=datetime.now(), freq="W")

for d in dates_marketing:
    chosen_channel = random.choice(channels)

    # Data Quality Issue : Incohérence de saisie (majuscules/minuscules)
    if random.random() < 0.1:
        chosen_channel = chosen_channel.lower()  # "email" au lieu de "Email"

    marketing_data.append(
        {
            "campagne_id": fake.bothify(text="CMP-####-??"),
            "date_lancement": d.strftime(
                "%d/%m/%Y"
            ),  # Format français TEXTE (piège pour Power BI)
            "canal": chosen_channel,
            "cout": round(random.uniform(500, 5000), 2),
            "clics": random.randint(100, 10000)
            if chosen_channel != "Email"
            else None,  # Pas de clics pour certains emails
            "impressions": random.randint(10000, 500000),
        }
    )

df_marketing = pd.DataFrame(marketing_data)

# Export en CSV avec séparateur point-virgule (Standard Excel FR)
df_marketing.to_csv(
    "data_files/marketing_campaigns.csv",
    sep=";",
    index=False,
    encoding="utf-8-sig",  # utf-8-sig pour gérer les accents sur Excel
)


# --- B. FICHIER EXCEL : OBJECTIFS DE VENTE (Budget) ---
# Scénario : Fichier envoyé par le contrôle de gestion.
# Pièges : Granularité mensuelle (alors que les ventes sont journalières), Magasins fermés inclus.

targets_data = []
years = [2022, 2023, 2024]
store_ids = [m["store_id"] for m in magasins_data]
store_ids.append("S999")  # Un magasin qui n'existe pas dans la base RH (Piège)

for y in years:
    for m in range(1, 13):
        for sid in store_ids:
            # Objectif un peu plus élevé en fin d'année
            base_target = 30000 if m < 10 else 50000

            targets_data.append(
                {
                    "Année": y,
                    "Mois": m,  # Juste le numéro du mois (Piège : il faudra reconstruire une date)
                    "Code_Magasin": sid,
                    "Objectif_CA": base_target * random.uniform(0.8, 1.2),
                }
            )

df_targets = pd.DataFrame(targets_data)

# Export Excel
df_targets.to_excel("data_files/objectifs_ventes.xlsx", index=False)

print(f"FICHIERS GÉNÉRÉS DANS LE DOSSIER : {os.path.abspath('data_files')}")
print("   - marketing_campaigns.csv (Attention au format de date et séparateur)")
print("   - objectifs_ventes.xlsx (Attention à la granularité Mois/Année)")

print("ENVIRONNEMENT PRÊT !")
