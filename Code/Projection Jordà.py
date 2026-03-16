import pandas as pd
import numpy as np
import statsmodels.api as sm
from linearmodels.panel import PanelOLS
import matplotlib.pyplot as plt

# ===============================
# 1. Charger données
# ===============================
df = pd.read_csv("C:/Users/lilic/Bureau/Dossiers/Dossiers non-triés/X/2025-09-4A/Projet Stat'App/Clone Git/Stat-App/Data_clean/df_long_indicators vs tarifs.csv")


##Attention Je mets juste mon chemin perso pour tester ce que ça donne
df = pd.read_csv("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App-1/Data_clean/df_long_indicators vs tarifs.csv")



df = df.drop(columns=["Unnamed: 0"], errors="ignore")



# ===============================
# 2. Format panel
# ===============================
df = df.set_index(["Country Code", "year"])
print(df.columns)
# ===============================
# 3. Construire variables de base
# ===============================

#génère la base de données pour la régression SANS la variable y_{t+k} - y_{t-1}
# input : nombre de lags >0, nom de la variable d'intérêt 
def base_lagged(lag, name_y):
    df["y"] = df[name_y]

    # lagged series
    for i in range(1, lag +1):
        df[f"y_lag{i}"] = df.groupby(level=0)["y"].shift(i)
        df[f"tariff_lag{i}"] = df.groupby(level=0)["tariff"].shift(i)

    #Variation des lagged series
    df["delta_y"] = df["y"] - df["y_lag1"]
    df["delta_tariff"] = df["tariff"] - df["tariff_lag1"]
    for i in range(1, lag+1):
        df[f"delta_y_lag{i}"] = df.groupby(level=0)["delta_y"].shift(i)
        df[f"delta_tariff_lag{i}"] = df.groupby(level=0)["delta_tariff"].shift(i)

    cols = (
        ["delta_tariff"] +
 #       [f"tariff_lag{i}" for i in range(1, lag + 1)] +
       [f"delta_tariff_lag{i}" for i in range(1, lag + 1)] +
        [f"delta_y_lag{i}" for i in range(1, lag + 1)])

    data = df[cols].dropna()

    return data


# ===============================
# 4. Local Projections
# ===============================

def projection_locale(H, lag, name_y):
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

        data = base_lagged(lag, name_y)
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
variable_y= "Prix à la consommation"

AIC=[]
BIC=[]
lag_value=range(1,15)

for i in range(1,15):
    betas, lower_ci95, upper_ci95, lower_ci90, upper_ci90, aic, bic= projection_locale(1, i, variable_y )
    AIC.append(aic[0])
    BIC.append(bic[0])

plt.figure()
plt.plot(lag_value, AIC, label="AIC", linewidth=2)
plt.plot(lag_value, BIC, label="BIC", linewidth=2)

plt.legend()

plt.show()


# ===============================
# 6. Plot IRF
# ===============================
H=5
lag =2

betas, lower_ci95, upper_ci95, lower_ci90, upper_ci90, aic, bic= projection_locale(H, lag, variable_y )

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
plt.ylabel(f"Response of {variable_y}")
plt.title("Impulse Response to a Tariff Shock")
plt.legend()

plt.show()



