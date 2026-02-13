import pandas as pd
import numpy as np
import statsmodels.api as sm
from linearmodels.panel import PanelOLS
import matplotlib.pyplot as plt

# ===============================
# 1. Charger données
# ===============================
df = pd.read_csv("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data_clean/df_long_indicators vs tarifs.csv")
df = df.drop(columns=["Unnamed: 0"], errors="ignore")

# ===============================
# 2. Format panel
# ===============================
df = df.set_index(["Country Code", "year"])

# ===============================
# 3. Construire variables de base
# ===============================

# Log PIB
df["log_gdp"] = np.log(df["Produit intérieur brut, volume"])
df["unemployment"] = df["Taux de chômage"]

# Lags variable d'intérêt (remplacer "unemployment" ou "unemp" par le Y choisi)
df["unemp_lag1"] = df.groupby(level=0)["unemployment"].shift(1)
df["unemp_lag2"] = df.groupby(level=0)["unemployment"].shift(2)

# Variation PIB
df["delta_unemp"] = df["unemployment"] - df["unemp_lag1"]
df["delta_unemp_lag1"] = df.groupby(level=0)["delta_unemp"].shift(1)
df["delta_unemp_lag2"] = df.groupby(level=0)["delta_unemp"].shift(2)

# Lags tarifs
df["tariff_lag1"] = df.groupby(level=0)["tariff"].shift(1)
df["tariff_lag2"] = df.groupby(level=0)["tariff"].shift(2)

# ===============================
# 4. Local Projections
# ===============================

H = 5   # nombre d'horizons

betas = []
lower_ci = []
upper_ci = []

for k in range(H + 1):

    # y_{t+k}
    df[f"unemp_lead{k}"] = df.groupby(level=0)["unemployment"].shift(-k)

    # Variable dépendante : y_{t+k} - y_{t-1}
    df[f"dep_k{k}"] = df[f"unemp_lead{k}"] - df["unemp_lag1"]

    # Construire dataset propre
    data = df[[
        f"dep_k{k}",
        "delta_tariff",
        "delta_unemp_lag1",
        "delta_unemp_lag2",
        "tariff_lag1",
        "tariff_lag2"
    ]].dropna()

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
    lower_ci.append(beta - 1.96 * se)
    upper_ci.append(beta + 1.96 * se)

# ===============================
# 5. Plot IRF
# ===============================

plt.figure()

horizons = range(H + 1)

# Courbe principale
plt.plot(horizons, betas, label="Impulse response", linewidth=2)

# Intervalle de confiance en zone ombrée
plt.fill_between(horizons, lower_ci, upper_ci, alpha=0.3, label="95% confidence interval")

# Ligne zéro
plt.axhline(0)

plt.xlabel("Horizon (years)")
plt.ylabel("Response of unemployment rate")
plt.title("Impulse Response to a Tariff Shock")
plt.legend()

plt.show()


