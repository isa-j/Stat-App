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





# ===============================
# 6. Local Projections (Jordà 2005 spec)
# ===============================

# Controls & transformations for Jordà (2005)
# - log exchange rate (avoid log(0) / negative values)
# - current account balance
# - lags for all necessary variables

df["log_exchange"] = np.log(df["Taux de change nominal"].replace({0: np.nan}))

df["log_gdp_lag1"] = df.groupby(level=0)["log_gdp"].shift(1)
df["log_gdp_lag2"] = df.groupby(level=0)["log_gdp"].shift(2)

df["delta_log_gdp"] = df["log_gdp"] - df["log_gdp_lag1"]
df["delta_log_gdp_lag1"] = df.groupby(level=0)["delta_log_gdp"].shift(1)
df["delta_log_gdp_lag2"] = df.groupby(level=0)["delta_log_gdp"].shift(2)

# Exchange rate dynamics
df["exchange_lag1"] = df.groupby(level=0)["log_exchange"].shift(1)
df["exchange_lag2"] = df.groupby(level=0)["log_exchange"].shift(2)
df["delta_exchange"] = df["log_exchange"] - df["exchange_lag1"]
df["delta_exchange_lag1"] = df.groupby(level=0)["delta_exchange"].shift(1)
df["delta_exchange_lag2"] = df.groupby(level=0)["delta_exchange"].shift(2)

# Current account balance lags
df["ca"] = df["Balance des transactions courantes en pourcentage du PIB"]
df["ca_lag1"] = df.groupby(level=0)["ca"].shift(1)
df["ca_lag2"] = df.groupby(level=0)["ca"].shift(2)


def local_projection(y_var, label, H=5, ci=0.90):
    """Estimate local projection IRF for y_var to delta_tariff using Jordà (2005) specification.

    Args:
        y_var: name of the dependent variable in df (e.g., "log_gdp" or "unemployment").
        label: label used for naming intermediate columns and plots.
        H: horizon in years.
        ci: confidence level for bands (0.90 or 0.95).

    Returns:
        betas: list of coefficients for each horizon.
        lower_ci: list of lower bounds.
        upper_ci: list of upper bounds.
        n_obs: list of number of observations for each horizon.
    """

    betas, lower_ci, upper_ci, n_obs = [], [], [], []

    # Compute z-score for CI (use standard values to avoid scipy dependency)
    z_map = {0.90: 1.645, 0.95: 1.96}
    z = z_map.get(ci, 1.645)

    # Ensure we have the required lags for y_var
    df[f"{y_var}_lag1"] = df.groupby(level=0)[y_var].shift(1)
    df[f"{y_var}_lag2"] = df.groupby(level=0)[y_var].shift(2)

    for k in range(H + 1):
        # Build dependent variable: y_{t+k} - y_{t-1}
        df[f"{y_var}_lead{k}"] = df.groupby(level=0)[y_var].shift(-k)
        df[f"dep_{label}_{k}"] = df[f"{y_var}_lead{k}"] - df[f"{y_var}_lag1"]

        delta_lag1 = f"delta_{y_var}_lag1"
        delta_lag2 = f"delta_{y_var}_lag2"

        if delta_lag1 not in df.columns or delta_lag2 not in df.columns:
            df[f"delta_{y_var}"] = df[y_var] - df[f"{y_var}_lag1"]
            df[delta_lag1] = df.groupby(level=0)[f"delta_{y_var}"].shift(1)
            df[delta_lag2] = df.groupby(level=0)[f"delta_{y_var}"].shift(2)

        data = df[[
            f"dep_{label}_{k}",
            "delta_tariff",
            delta_lag1,
            delta_lag2,
            "tariff_lag1",
            "tariff_lag2",
            "delta_exchange_lag1",
            "delta_exchange_lag2",
            "ca_lag1",
            "ca_lag2",
        ]].dropna()

        if data.empty:
            betas.append(np.nan)
            lower_ci.append(np.nan)
            upper_ci.append(np.nan)
            n_obs.append(0)
            continue

        y = data[f"dep_{label}_{k}"]
        X = sm.add_constant(data.drop(columns=[f"dep_{label}_{k}"]))

        model = PanelOLS(y, X, entity_effects=True, time_effects=True)
        res = model.fit(cov_type="clustered", cluster_entity=True)

        beta = res.params["delta_tariff"]
        se = res.std_errors["delta_tariff"]

        betas.append(beta)
        lower_ci.append(beta - z * se)
        upper_ci.append(beta + z * se)
        n_obs.append(len(data))

    return betas, lower_ci, upper_ci, n_obs


# ---- IRF pour log GDP (Jordà 2005)
H = 6
betas_gdp, lower_gdp, upper_gdp, n_obs_gdp = local_projection("log_gdp", "log_gdp", H=H, ci=0.90)

print(f"Nombre d'observations par horizon pour log GDP: {n_obs_gdp}")

plt.figure()
horizons = range(H + 1)
plt.plot(horizons, betas_gdp, label="IRF (log GDP)", linewidth=2)
plt.fill_between(horizons, lower_gdp, upper_gdp, alpha=0.3, label="90% CI")
plt.axhline(0, color="black", linewidth=0.8)
plt.xlabel("Horizon (years)")
plt.ylabel("Response of log GDP")
plt.title("IRF of log GDP to tariff shock (Jordà 2005 spec)")
plt.legend()
plt.show()


# ---- IRF pour chômage (même spécification)
H = 6
betas_unemp, lower_unemp, upper_unemp, n_obs_unemp = local_projection("unemployment", "unemployment", H=H, ci=0.90)

print(f"Nombre d'observations par horizon pour chômage: {n_obs_unemp}")

plt.figure()
plt.plot(horizons, betas_unemp, label="IRF (unemployment)", linewidth=2)
plt.fill_between(horizons, lower_unemp, upper_unemp, alpha=0.3, label="90% CI")
plt.axhline(0, color="black", linewidth=0.8)
plt.xlabel("Horizon (years)")
plt.ylabel("Response of unemployment rate")
plt.title("IRF of unemployment to tariff shock (Jordà 2005 spec)")
plt.legend()
plt.show()


# ---- IRF combinée (log GDP + chômage)
plt.figure(figsize=(10, 6))
plt.plot(horizons, betas_gdp, label="IRF (log GDP)", linewidth=2, color="#1f77b4")
plt.fill_between(horizons, lower_gdp, upper_gdp, alpha=0.25, color="#1f77b4")

plt.plot(horizons, betas_unemp, label="IRF (unemployment)", linewidth=2, color="#ff7f0e")
plt.fill_between(horizons, lower_unemp, upper_unemp, alpha=0.25, color="#ff7f0e")

plt.axhline(0, color="black", linewidth=0.8)
plt.xlabel("Horizon (years)")
plt.ylabel("Response")
plt.title("IRF to tariff shock (Jordà 2005 spec) — log GDP vs unemployment")
plt.legend()
plt.tight_layout()
plt.show()


