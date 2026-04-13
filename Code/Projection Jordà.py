import pandas as pd
import numpy as np
import statsmodels.api as sm
from linearmodels.panel import PanelOLS
import matplotlib.pyplot as plt

# ===============================
# 1. Charger données
# ===============================
#df = pd.read_csv("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data_clean/df_long_indicators_vs_tarifs_imputed.csv")
df = pd.read_csv("C:/Users/lilic/Bureau/Dossiers/Dossiers non-triés/X/2025-09-4A/Projet Stat'App/Clone Git/Stat-App/Data_clean/df_long_indicators_vs_tarifs_31mars.csv")
#df = pd.read_csv("C:/Users/lilic/Bureau/Dossiers/Dossiers non-triés/X/2025-09-4A/Projet Stat'App/Clone Git/Stat-App/Data_clean/df_long_indicators_vs_tarifs_imputed.csv")

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

print(df.columns)
#raise Exception("Stop here")
A = ['Country Name',
       'Balance des transactions courantes en pourcentage du PIB',
       'Cours des actions', 'Emploi',
       'Importations de biens et services, volume', 'M3',
       'Prix à la consommation', 'Produit intérieur brut, volume',
       'Taux de change nominal', 'Taux de chômage', 'tariff_AR', 'tariff_FN', 'tariff_selected',
       'tariff_lag1',
       'delta_tariff']
B = ['Country Name',
       'Balance_paiements',
       'Actions', 'Emploi',
       'Importations', 'M3',
       'IPC', 'PIB',
       'Taux_change', 'Chômage', 'tariff_AR', 'tariff_FN', 'tariff_selected',
       'tariff_lag1',
       'delta_tariff']

mapping = dict(zip(A, B))

df = df.rename(columns=mapping)

df = df.drop(columns=['Actions', 'Importations','M3', 'lag_AR','delta_AR','lag_FN','delta_FN', "Chômage"])

country = ["ESP", "GRC", "LUX", "CZE", "FRA", "JPN", "MEX", "NLD", "USA", "DEU","AUS", "AUT", "CAN", "KOR", "FIN", "ITA", "NOR", "NZL", "GBR", "SWE"] #IND less availaible in the current years
PIB_1995 =[803136997375,167488456016,30604417413, 118281953160, 1749300344479, 3785636263361, 724231953299, 524315389158,11052644207040,2614281254585, 709528553282, 267089386116, 955122057479, 627888532992, 152674521262, 1671695507214, 254491639971, 102929582187, 1935631814547, 309511442391   ]
df = df.loc[df.index.get_level_values("Country Code").isin(country)]
gdp_series = pd.Series(PIB_1995, index=country)
print(gdp_series)

for c in country:
    df.loc[(c, 1995), "PIB_new"] = gdp_series.loc[c]
    for i in range(1996, 2023):
        df.loc[(c, i), "PIB_new"] = df.loc[(c, i-1), "PIB_new"]* (1 + df.loc[(c, i), "PIB"]/100)
        print(df.loc[(c, i), "PIB"])



df["PIB"] = np.log(df["PIB_new"])
df["IPC"] = np.log(df["IPC"])
#df = df.drop("PIB_new")
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
# 3.1 Construire variables de base avec les controles
# ===============================
col = list(df.columns)
filtered_cols = [c for c in col if c != 'Country Name' and 'tariff' not in c]

#génère la base de données avec les variables de controle pour la régression SANS la variable y_{t+k} - y_{t-1}
# input : nombre de lags >0, base

def base_lagged_with_controls(lag, df, tariff_type):

    for name in filtered_cols:

        # lagged series
        for i in range(1, lag +1):
            df[f"{name}_lag{i}"] = df.groupby(level=0)[name].shift(i)
            df[f"tariff_lag{i}"] = df.groupby(level=0)[tariff_type].shift(i)

        #Variation des lagged series
        df[f"delta_{name}"] = df[name] - df[f"{name}_lag1"]
        df["delta_tariff"] = df[tariff_type] - df["tariff_lag1"]
        for i in range(1, lag+1):
            df[f"delta_{name}_lag{i}"] = df.groupby(level=0)[f"delta_{name}"].shift(i)
            df[f"delta_tariff_lag{i}"] = df.groupby(level=0)["delta_tariff"].shift(i)

    cols = (
        ["delta_tariff"] +
        [f"delta_tariff_lag{i}" for i in range(1, lag + 1)])
    
    for name in filtered_cols :
        for i in range(1, lag + 1):
           cols.append(f"delta_{name}_lag{i}")

    data = df[cols].dropna()

    return data


# ===============================
# 4. Local Projections
# ===============================

def projection_locale(H, lag, name_y, df, tariff_type):
    betas = []
    lower_ci95 = []
    upper_ci95 = []
    lower_ci90 = []
    upper_ci90 = []
    aic=[]
    bic=[]

    df["y_lag1"] = df.groupby(level=0)[name_y].shift(1)

    for k in range(H + 1):

        # y_{t+k}
        df[f"y_lead{k}"] = df.groupby(level=0)[name_y].shift(-k)

        # Variable dépendante : y_{t+k} - y_{t-1}
        df[f"dep_k{k}"] = df[f"y_lead{k}"] - df["y_lag1"]

        data = base_lagged_with_controls(lag, df, tariff_type)
        data[f"dep_k{k}"] = df[f"dep_k{k}"]       
        data = data.dropna()

        y = data[f"dep_k{k}"]
        X = data.drop(columns=[f"dep_k{k}"])
        X = sm.add_constant(X)

        # Estimation panel avec FE pays + année
        model = PanelOLS(
            y,
            X,
            entity_effects=True,
            time_effects=True
        )

        res = model.fit(cov_type="clustered", cluster_entity=True)

        beta = res.params["delta_tariff"]
        se = res.std_errors["delta_tariff"]

        betas.append(beta)
        lower_ci95.append(beta - 1.96 * se)
        upper_ci95.append(beta + 1.96 * se)

        lower_ci90.append(beta - 1.65* se)
        upper_ci90.append(beta + 1.65 * se)

        llf = res.loglik  # log-likelihood
        k = res.params.shape[0]  # number of estimated coefficients
        aic.append(-2 * llf + 2 * k)
        bic.append(-2 * llf + k * np.log(res.nobs))

    return(betas, lower_ci95, upper_ci95, lower_ci90, upper_ci90, aic, bic)

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

print(df.index.get_level_values("Country Code").unique())


europe = ["ESP", "LUX", "FRA" , "NLD", "DEU", "AUT",  "FIN", "ITA", "NOR",  "SWE", "CZE" , "GRC"]  #IND less availaible in the current years
autre = ["JPN", "MEX", "USA", "AUS", "CAN", "KOR", "NZL", "GBR"]

pos_balance = []
neg_balance = []

for c in df.index.get_level_values("Country Code").unique():
    df_sub = df.loc[df.index.get_level_values("Country Code") == c]
    mean = df_sub["Balance_paiements"].mean()
    if mean>0 :
        pos_balance.append(c)
    else :
        neg_balance.append(c)
    

df_europe = df.loc[df.index.get_level_values("Country Code").isin(europe)].copy()
df_autre = df.loc[df.index.get_level_values("Country Code").isin(autre)].copy()
df_pos = df.loc[df.index.get_level_values("Country Code").isin(pos_balance)].copy()
df_neg = df.loc[df.index.get_level_values("Country Code").isin(neg_balance)].copy()

# ===============================
# 7. Plot IRF
# ===============================
H=5
lag = 1
tariff_type = 'tariff_FN'
variable_y = "IPC"

betas, lower_ci95, upper_ci95, lower_ci90, upper_ci90, aic, bic= projection_locale(H, lag, variable_y, df_neg, tariff_type)

plt.figure()

horizons = range(H + 1)

# Courbe principale
plt.plot(horizons, betas, label="Impulse response", linewidth=2)
plt.plot(horizons, lower_ci90, label="90% confidence interval", linewidth=1, linestyle ='--',  color='orange')
plt.plot(horizons, upper_ci90, linewidth=1,alpha=0.5, linestyle ='--',  color='orange')

# Intervalle de confiance en zone ombrée
plt.fill_between(horizons, lower_ci95, upper_ci95, alpha=0.3, label="95% confidence interval")

# Ligne zéro
plt.axhline(0)

plt.xlabel("Horizon (years)")
plt.ylabel("Response of consummer price index (in log)")
plt.title("Impulse response to a tariff shock (most favoured nation rate)")
plt.legend()

plt.show()





