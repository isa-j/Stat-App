import pandas as pd
import numpy as np
import statsmodels.api as sm
from linearmodels.panel import PanelOLS
import matplotlib.pyplot as plt

# ===============================
# 1. Charger données SECTORIELLES
# ===============================
# Nouveau fichier avec 2 secteurs : Céréales et Métaux
df = pd.read_csv("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App-1/Data_clean/df_sectoral_tariffs.csv")

df = df.drop(columns=["Unnamed: 0"], errors="ignore")

# ===============================
# 2. Format panel
# ===============================
# - Changement des noms de variables
# - Transformation log de certaines variables
# - sélection des pays

df = df[df["year"] >= 1995]
df = df[df["year"] <= 2022]
df = df.set_index(["Country Code", "year"])

#print(df.columns)
#raise Exception("Stop here")
A = ['Country Name',
       'Balance des transactions courantes en pourcentage du PIB',
       'Cours des actions', 'Emploi',
       'Importations de biens et services, volume', 'M3',
       'Prix à la consommation', 'Production volume', 'Produit intérieur brut, volume',
       'Taux de change nominal', 'Taux de chômage']
B = ['Country Name',
       'Balance_paiements',
       'Actions', 'Emploi',
       'Importations', 'M3',
       'IPC', 'Production', 'PIB',
       'Taux_change', 'Chômage']

mapping = dict(zip(A, B))

df = df.rename(columns=mapping)

df = df.drop(columns=['Actions', 'Importations','M3', 'Chômage'], errors='ignore')

country = ["AUT", "BEL", "DEU", "DNK", "ESP", "FIN", "FRA", "GBR",
           "GRC", "IRL", "ITA", "NLD", "PRT", "SWE"]  # 14 pays européens

# Filtrer aux pays disponibles
df = df.loc[df.index.get_level_values("Country Code").isin(country)]

print(f"Pays disponibles: {df.index.get_level_values('Country Code').unique()}")

# Nettoyer les colonnes inutiles du fichier précédent
df = df.drop(columns=['tariff_AR', 'tariff_FN', 'lag_AR', 'delta_AR', 'lag_FN', 'delta_FN'], errors='ignore')

# Transformer le PIB en log (données déjà en volume)
df["PIB"] = np.log(df["PIB"])
df["IPC"] = np.log(df["IPC"])

print("\nAperçu des données:")
print(df.head())
print(df)

# ===============================
# 3. Construire variables de base
# ===============================

#génère la base de données pour la régression SANS la variable y_{t+k} - y_{t-1}
# input : nombre de lags >0, nom de la variable d'intérêt 
def base_lagged(lag, name_y, tariff_type):
    df["y"] = df[name_y]

    # lagged series
    for i in range(1, lag +1):
        df[f"y_lag{i}"] = df.groupby(level=0)["y"].shift(i)
        df[f"tariff_lag{i}"] = df.groupby(level=0)[tariff_type].shift(i)

    #Variation des lagged series
    df["delta_y"] = df["y"] - df["y_lag1"]
    df["delta_tariff"] = df[tariff_type] - df["tariff_lag1"]
    for i in range(1, lag+1):
        df[f"delta_y_lag{i}"] = df.groupby(level=0)["delta_y"].shift(i)
        df[f"delta_tariff_lag{i}"] = df.groupby(level=0)["delta_tariff"].shift(i)

    cols = (
        ["delta_tariff"] +
 #      [f"tariff_lag{i}" for i in range(1, lag + 1)] +
        [f"delta_tariff_lag{i}" for i in range(1, lag + 1)] +
        [f"delta_y_lag{i}" for i in range(1, lag + 1)])

    data = df[cols].dropna()

    return data

# ===============================
# 3.1 Construire variables de base avec les controles (simplifiées)
# ===============================
# Variables de contrôle essentielles seulement
control_cols = ['IPC', 'Emploi', 'Production']

#génère la base de données avec les variables de controle simplifiées
def base_lagged_with_controls_simple(lag, df_input, tariff_type):
    df_temp = df_input.copy()

    for name in control_cols:
        if name in df_temp.columns:
            # lagged series
            for i in range(1, lag + 1):
                df_temp[f"{name}_lag{i}"] = df_temp.groupby(level=0)[name].shift(i)
            
            # Variation des lagged series
            df_temp[f"delta_{name}"] = df_temp[name] - df_temp[f"{name}_lag1"]
            for i in range(1, lag + 1):
                df_temp[f"delta_{name}_lag{i}"] = df_temp.groupby(level=0)[f"delta_{name}"].shift(i)

    # Lags de tarifs
    for i in range(1, lag + 1):
        df_temp[f"tariff_lag{i}"] = df_temp.groupby(level=0)[tariff_type].shift(i)
    
    # Variation de tarif
    df_temp["delta_tariff"] = df_temp[tariff_type] - df_temp["tariff_lag1"]
    for i in range(1, lag + 1):
        df_temp[f"delta_tariff_lag{i}"] = df_temp.groupby(level=0)["delta_tariff"].shift(i)

    cols = ["delta_tariff"] + [f"delta_tariff_lag{i}" for i in range(1, lag + 1)]
    
    for name in control_cols:
        if name in df_temp.columns:
            for i in range(1, lag + 1):
                if f"delta_{name}_lag{i}" in df_temp.columns:
                    cols.append(f"delta_{name}_lag{i}")

    data = df_temp[cols].dropna()
    return data


# ===============================
# 4. Local Projections
# ===============================

def projection_locale(H, lag, name_y, df, tariff_type):
    """
    Version ROBUSTE avec effets pays + contrôles macro
    """
    betas = []
    lower_ci95 = []
    upper_ci95 = []
    lower_ci90 = []
    upper_ci90 = []

    df["y_lag1"] = df.groupby(level=0)[name_y].shift(1)

    for k in range(H + 1):

        # y_{t+k}
        df[f"y_lead{k}"] = df.groupby(level=0)[name_y].shift(-k)

        # Variable dépendante : y_{t+k} - y_{t-1}
        df[f"dep_k{k}"] = df[f"y_lead{k}"] - df["y_lag1"]

        # Construire les variables avec lags de y comme contrôle
        df_temp = df.copy()
        
        # Lags du tarif et du PIB (pour absorber la dynamique macro)
        for i in range(1, lag + 1):
            df_temp[f"tariff_lag{i}"] = df_temp.groupby(level=0)[tariff_type].shift(i)
            df_temp[f"y_lag{i+1}"] = df_temp.groupby(level=0)[name_y].shift(i+1)
        
        df_temp["delta_tariff"] = df_temp[tariff_type] - df_temp["tariff_lag1"]
        for i in range(1, lag + 1):
            df_temp[f"delta_tariff_lag{i}"] = df_temp.groupby(level=0)["delta_tariff"].shift(i)

        # Inclure lags de y en tant que contrôle (absorbe la dynamique)
        cols = ["delta_tariff"] + [f"delta_tariff_lag{i}" for i in range(1, lag + 1)]
        cols += [f"y_lag{i+1}" for i in range(lag)]  # Ajouter les lags de y
        
        data = df_temp[cols].dropna()
        data[f"dep_k{k}"] = df_temp[f"dep_k{k}"]       
        data = data.dropna()

        y = data[f"dep_k{k}"]
        X = data.drop(columns=[f"dep_k{k}"])
        X = sm.add_constant(X)

        # Estimation panel avec FE pays SEULEMENT
        model = PanelOLS(
            y,
            X,
            entity_effects=True,
            time_effects=False
        )

        try:
            res = model.fit(cov_type="clustered", cluster_entity=True)
        except Exception as e:
            # Si erreur, essayer OLS régulier
            try:
                res = sm.OLS(y, X).fit()
            except:
                res = sm.OLS(y, X.iloc[:, :2]).fit()  # Juste tarif + const

        # Récupérer les résultats
        if hasattr(res, 'std_errors'):
            se = res.std_errors.get("delta_tariff", np.nan)
        else:
            se = res.bse.get("delta_tariff", np.nan) if hasattr(res.bse, 'get') else np.nan
            
        beta = res.params.get("delta_tariff", np.nan) if hasattr(res.params, 'get') else res.params["delta_tariff"]

        betas.append(beta)
        lower_ci95.append(beta - 1.96 * se)
        upper_ci95.append(beta + 1.96 * se)

        lower_ci90.append(beta - 1.65* se)
        upper_ci90.append(beta + 1.65 * se)

    return(betas, lower_ci95, upper_ci95, lower_ci90, upper_ci90, [], [])

# ===============================
# 5. Choose lag value
# ===============================
# variable_y= "PIB"
# tariff_type = 'tariff_AR'

# AIC=[]
# BIC=[]
# lag_value=range(1,10)

# for i in range(1,10):
#     betas, lower_ci95, upper_ci95, lower_ci90, upper_ci90, aic, bic= projection_locale(1, i,  variable_y, df, tariff_type )
#     AIC.append(aic[0])
#     BIC.append(bic[0])

# plt.figure()
# plt.plot(lag_value, AIC, label="AIC", linewidth=2)
# plt.plot(lag_value, BIC, label="BIC", linewidth=2)

# plt.legend()

# plt.show()

# ===============================
# 6. Robustesse
# ===============================
#Analyse spécifique des pays européens

# ===============================
# 7. Plot IRF - Deux secteurs
# ===============================
H = 5
lag = 1
variable_y = "PIB"

# Deux secteurs d'intérêt
sectors = [
    ('tariff_cereales', 'Céréales (Agriculture)', 'Agriculture Tariff Shock'),
    ('tariff_metaux', 'Métaux (Métallurgie)', 'Metals Tariff Shock')
]

print(f"\n{'='*70}")
print(f"ANALYSE JORDÀ - RÉPONSE DU PIB À DES CHOCS DE TARIFS")
print(f"{'='*70}")
print(f"Variable dépendante: {variable_y}")
print(f"Horizon: {H} ans")
print(f"Nombre de lags: {lag}")
print(f"Pays: {', '.join(df.index.get_level_values('Country Code').unique())}")

# Boucle sur les deux secteurs
for tariff_col, sector_name, shock_name in sectors:
    print(f"\n{'-'*70}")
    print(f"Secteur: {sector_name}")
    print(f"Colonne: {tariff_col}")
    print(f"{'-'*70}")
    
    # Vérifier que la colonne existe
    if tariff_col not in df.columns:
        print(f"⚠️  Colonne {tariff_col} non trouvée!")
        continue
    
    # Calculer les IRF
    betas, lower_ci95, upper_ci95, lower_ci90, upper_ci90, aic, bic = projection_locale(
        H, lag, variable_y, df.copy(), tariff_col
    )
    
    # Afficher les résultats
    print(f"\nRésultats des IRF pour {sector_name}:")
    for k in range(H + 1):
        print(f"  Horizon {k}: β={betas[k]:8.6f}, IC95%=[{lower_ci95[k]:8.6f}, {upper_ci95[k]:8.6f}]")
    
    # Plot IRF
    plt.figure(figsize=(12, 6))
    
    horizons = range(H + 1)
    
    # Courbe principale
    plt.plot(horizons, betas, label="Impulse response", linewidth=2, color='#2E86AB', marker='o')
    plt.plot(horizons, lower_ci90, label="90% confidence interval", linewidth=1, linestyle='--', color='orange')
    plt.plot(horizons, upper_ci90, linewidth=1, alpha=0.5, linestyle='--', color='orange')
    
    # Intervalle de confiance en zone ombrée
    plt.fill_between(horizons, lower_ci95, upper_ci95, alpha=0.3, color='#2E86AB', label="95% confidence interval")
    
    # Ligne zéro
    plt.axhline(0, color='black', linestyle='-', linewidth=0.8)
    
    plt.xlabel("Horizon (years)", fontsize=11)
    plt.ylabel(f"Response of log({variable_y})", fontsize=11)
    plt.title(f"Impulse Response Function - {shock_name}\n{sector_name} (EU14)", fontsize=13, fontweight='bold')
    plt.legend(loc='best', fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Sauvegarder le graphe
    output_path = f"/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App-1/Résultats/IRF_{sector_name.replace(' ', '_').replace('(', '').replace(')', '')}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\n✅ Graphe sauvegardé: {output_path}")
    
    plt.show()

print(f"\n{'='*70}")
print(f"✅ Analyse Jordà terminée!")
print(f"{'='*70}")

