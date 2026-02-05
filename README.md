# Projet "Data Wine & Finance" : Du SI Complexe à la Décision Business

## Présentation du Projet
Ce projet simule l'écosystème data complet d'un négociant en vins. L'objectif est de démontrer ma capacité à évoluer dans un environnement **multi-sources** et **multi-bases**, en traitant des problématiques réelles de **qualité de données** et de **réconciliation financière**.

Plutôt que d'utiliser un dataset "propre", j'ai conçu un environnement volontairement dégradé (anomalies, doublons, erreurs de calcul) pour tester la robustesse de mes pipelines de nettoyage **Python** et la précision de mes analyses **SQL / Power BI**.

---

## Architecture Technique
L'environnement repose sur 4 bases de données relationnelles et des flux de données externes :

* **`db_sales`** : Cœur de l'activité (Produits, Commandes, Détails).
* **`db_finance`** : Gestion des factures et statuts de paiement.
* **`db_crm`** : Référentiel clients et avis produits.
* **`db_hr`** : Structure des magasins et performance des employés.
* **Flux Externes** : Objectifs de vente (Excel) et Campagnes Marketing (CSV).

---

## Les 4 Missions

Chaque scénario business nécessite entre **4 et 6 jointures SQL** et un traitement de données spécifique.

### 1. Mission "Audit & TVA" (Finance)
> **Question Manager :** *"On a un écart inexpliqué entre nos volumes de ventes et nos encaissements. Avons-nous des oublis de facturation ou des erreurs de calcul de TVA ?"*
* **Défi :** Réconcilier le flux : **Produit** → **Commande** → **Facture** → **Magasin**.
* **Expertise :** Identification des pertes de revenus (commandes livrées non facturées).

#### Organisation du projet :

Maison_Mazars_BI/  
├── Finance/  
│   ├── Data/  
│   │   └── extracted_data.csv  
│   ├── extract.py  
│   ├── main.py  
│   ├── transform.py  
│   ├── Rapport_Finance.pbip   
│   ├── requirements.txt  
├── .gitignore  
└── README.md  

#### Utilisation du projet :
Utilisation du Projet
Ce projet suit un flux de travail automatisé divisé en trois phases : installation, exécution du pipeline de données (ETL) et analyse visuelle.

1. Installation des dépendances
Avant de commencer, assurez-vous d'avoir Python installé. Installez ensuite les bibliothèques nécessaires (Pandas, SQLAlchemy, PyMySQL) via le terminal :

```Bash
pip install -r Finance/requirements.txt
```

2. Exécution du Pipeline ETL
Pour mettre à jour les données du rapport, lancez le script orchestrateur :

```Bash
python Finance/main.py
```
Le processus se déroule comme suit :

- Authentification : Le script demande le mot de passe de la base de données SQL de manière sécurisée.

- Extraction : extract.py récupère les nouvelles transactions depuis la base de données.

- Transformation : transform.py nettoie les données, harmonise les statuts et calcule les Tranches Comptables (0-30j, 31-60j, 61-90j, +90j).

- Chargement : Le résultat est exporté dans Finance/Data/extracted_data.csv.

3. Consultation des Rapports
Ouvrez le fichier Rapport_Finance.pbix dans Power BI Desktop. Cliquez sur "Actualiser" pour charger les dernières données traitées par Python.

**Onglet GLOBAL :** Vision d'ensemble sur le volume (1642 factures) et le montant total des anomalies (249,90K €).

**Onglet LISTING :** Détail par client et par commande avec l'affichage de la tranche de risque.

**Onglet ANALYSE :** Suivi de l'évolution du montant HT et distribution des dossiers à risques (focus sur les +90 jours représentant 0,24M €).

#### Résultats :
Ce projet démontre la capacité à transformer des données comptables brutes en leviers de décision stratégiques via un pipeline ETL automatisé. En isolant précisément **249,90K €** d'anomalies critiques, l'outil permet de passer d'une simple observation de données à un audit financier proactif et ciblé.

L'implémentation des tranches de vieillissement (de 0-30j à +90j) offre une vision claire du risque client et permet une gestion priorisée du cash-flow. Cette solution constitue une base solide pour le pilotage de la performance financière, garantissant une visibilité totale sur les encours et la réduction des délais de paiement.

![alt text](image-1.png)

### 2. Mission "Rentabilité" (Direction Commerciale)
> **Question Manager :** *"Quelle est la rentabilité réelle par vendeur une fois qu'on déduit les remises et le coût d'achat des produits ?"*
* **Défi :** Croiser l'**Employé** et son **Magasin** avec le détail des **Remises** par **Produit**.
* **Expertise :** Analyse de la marge nette réelle et détection des remises abusives.

### 3️. Mission "Fidélisation" (CRM / Marketing)
> **Question Manager :** *"Nos clients 'Pro' qui achètent nos vins les mieux notés sont-ils plus fidèles en termes de récurrence d'achat ?"*
* **Défi :** Lier le profil **Client** à son historique de **Commandes** et à la moyenne des **Avis** produits.
* **Expertise :** Segmentation client et corrélation Satisfaction/Fidélité.

### 4️. Mission "ROI Marketing" (Direction Générale)
> **Question Manager :** *"Quel est le ROI 'Cash' réel de nos campagnes (CA encaissé vs budget dépensé) ?"*
* **Défi :** Matcher le fichier **Marketing (CSV)** avec les **Factures Payées (SQL)** en gérant les hétérogénéités de données.
* **Expertise :** Automatisation du nettoyage de fichiers plats et calcul de performance financière.

---

## Demandes Externes

### 1. Audit de Conformité (Commissaire aux Comptes)
* **La Demande :** "Transmettre l'exhaustivité des factures avec le détail des magasins et des montants TTC pour notre audit annuel."
* **Cohérence SI :** Utilisation de la table `factures` avec ses identifiants préfixés (ex: `F000004`) jointe aux tables `commandes` et `magasins`.
* **Le défi Python :** **Intégrité du format texte.** Les identifiants "F000..." sont des chaînes de caractères sensibles. Python garantit que l'export CSV conserve ce typage strict, évitant que des logiciels tiers ne tronquent les zéros ou n'interprètent mal le préfixe lors du lettrage comptable.
* **Livrable :** `audit_facturation_exhaustif.csv`

### 2. Réconciliation "Ventes vs Objectifs" (Direction Commerciale)
* **La Demande :** "Comparer les ventes réelles extraites du SQL avec le fichier de nomenclature des objectifs Excel par région."
* **Cohérence SI :** Croisement entre la table `db_hr.magasins` et le fichier externe `objectifs_ventes.xlsx`.
* **Le défi Python :** **Gestion des ruptures de jointure.** Le fichier Excel contient un magasin fictif (`S999`) absent de la base SQL. Python permet de traiter cette exception proprement (via un merge/join spécifique) pour générer un rapport d'écart fiable sans perte de données.
* **Livrable :** `ecart_ventes_objectifs.xlsx`

### 3. Data-Sharing Partenaire (Agence Marketing "Digital Wine")
* **La Demande :** "Extraire les transactions générées par les campagnes digitales pour calculer nos commissions d'affiliation."
* **Cohérence SI :** Jointure entre le fichier plat `marketing_campaigns.csv` et la table `db_sales.commandes`.
* **Le défi Python :** **Normalisation de la casse et des dates.** Le fichier partenaire contient des variantes de saisie ("Email", "email", "FACEBOOK"). Python harmonise ces entrées en minuscules et convertit les dates textes du CSV au format standard ISO pour assurer une jointure parfaite.
* **Livrable :** `attribution_marketing_clean.csv`

### 4. Justification des Remises (Contrôle Interne DGCCRF)
* **La Demande :** "Justifier l'application de la remise 'Professionnel' de 10% sur les commandes de gros volumes (plus de 6 bouteilles)."
* **Cohérence SI :** Jointure entre `db_crm.clients` (type='Professionnel') et `db_sales.commande_details` (quantite > 6).
* **Le défi Python :** **Audit de calcul financier.** Recalculer via Python la remise théorique attendue et la confronter au `montant_ttc` réel stocké dans les factures `FXXXXXX`. Ce script permet d'identifier les anomalies d'arrondis ou les erreurs d'application de prix avant l'extraction finale.
* **Livrable :** `conformite_remises_export.csv`

---

## Stack Technique & Méthodologie

### 1. Exploration & Audit (Jupyter Notebook)
Chaque mission commence par une phase d'exploration pour **débusquer les anomalies** (doublons, valeurs nulles, incohérences). C'est ici que je documente ma démarche d'audit.

### 2. Automatisation & Industrialisation (Python)
Développement de scripts de nettoyage (`Cleaning & ETL`) pour :
* Normaliser les formats de dates et les encodages.
* Corriger les fautes de frappe et espaces superflus (Villes/Magasins).
* Générer des rapports d'erreurs automatisés.

### 3. Visualisation & Décision (Power BI)
Création de dashboards interactifs :
* **DAX** pour les mesures complexes (CA Net, % d'atteinte d'objectifs).
* **Modélisation en étoile** pour optimiser la performance des rapports.

---
**Auteur** : Loris MAZARS - Étudiant en BUT3 Science des Données