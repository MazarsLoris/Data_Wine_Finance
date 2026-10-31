-- Mission "Rentabilité"

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

WHERE cmd.date_cmd >= '2024-01-01';