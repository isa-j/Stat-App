import pandas as pd

df_indic = pd.read_parquet("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data_clean/Indicateurs.parquet")

df_tarifs = pd.read_csv("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data_clean/Tarifs douaniers (MFN vs Applied).csv")

df_tarifs.isna().sum().sum()
#On calque les pays étudiés des deux df
values_to_drop = ["OECD", "EA19", "EU27_2020", "G20", "OECDE", "G7", "EA20"]

df_indic = df_indic[~df_indic["REF_AREA"].isin(values_to_drop)]

df_tarifs = df_tarifs[df_tarifs["Country Code"].isin(df_indic["REF_AREA"].unique())]

df_tarifs["Country Code"].unique()

len(df_indic["Mesure"].unique())


#On va transfo les tarifs en df long pour préparer la fusion
df_tarifs = df_tarifs.loc[:, ~df_tarifs.columns.str.contains("^Unnamed")]
 
df_tariffs_long = df_tarifs.melt(
    id_vars=["Country Code", "Country Name", "Indicator Name"],
    var_name="year_type",
    value_name="tariff"
)


#Maintenant on fait apparaître l'année et le type de tarif
df_tariffs_long[["year", "tariff_type"]] = (
    df_tariffs_long["year_type"].str.split("_", expand=True)
)

#Mesures de précaution ; on se débarasse de year_type
df_tariffs_long["year"] = df_tariffs_long["year"].astype(int)
df_tariffs_long.drop(columns="year_type", inplace=True)
#On vérifie que ça a bien fonctionné
df_tariffs_long

#On nomme bien le code pays partout pareil
df_indic = df_indic.rename(columns = {"REF_AREA" : "Country Code"})

#Jointure finale
df_final = df_indic.merge(
    df_tariffs_long,
    on=["Country Code", "year"],
    how="left"
)

#df_final.to_parquet("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data_clean/Indicators and tarifs.parquet")
df_final = df_final[df_final["Fréquence d'observation"] == "Annuelle"]
df_final




#Il y a des lignes identiques qui servent à rien dans le df, on va vérifier qu'elles sont bien identiques partout
# On filtre les lignes correspondant à chaque transformation
df_période = df_final[df_final['Transformation'] == 'Taux de croissance, période sur période']
df_sur_an = df_final[df_final['Transformation'] == 'Taux de croissance, sur un an']

# On s'assure qu'elles sont bien alignées par exemple par une colonne 'Année' ou 'Date'
df_merged = df_période.merge(df_sur_an, on='year', suffixes=('_période', '_sur_an'))

# On compare les valeurs
toutes_identiques = (df_merged['OBS_VALUE_période'].values == df_merged['OBS_VALUE_sur_an'].values).all()

print(toutes_identiques) #C'est bon on avait raison c identique



df_final = df_final[df_final['Transformation'] != 'Taux de croissance, période sur période']
df_final.to_parquet("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data_clean/Indicators and tarifs V2.parquet")


df_france = df_final[(df_final["Country Code"] == "FRA") & (df_final["tariff_type"] == "AR")]
df_france = df_france[ (df_france["Mesure"] == "Production volume") & (df_france["Activité économique"] == "Industrie (sauf construction)") &(df_france["Unité de mesure"] == "Taux de croissance")]


####Graphique de corrélation (PB: mieux vaudrait utiliser le lag des tarifs pour capter l'effet des politiques protectionnistes)

#Version "naïve"

import matplotlib.pyplot as plt
import seaborn as sns

# Scatter plot simple avec seaborn
plt.figure(figsize=(8,5))
sns.scatterplot(data=df_france, x='tariff', y='OBS_VALUE')

# Ajouter une droite de régression pour visualiser la tendance
sns.regplot(data=df_france, x='tariff', y='OBS_VALUE', scatter=False, color='red')

plt.title("Corrélation entre tarifs douaniers et taux de croissance de la production (France, 1988-2022)")
plt.xlabel("Tarif douanier (%)")
plt.ylabel("Taux de croissance de la production (%)")
plt.show()



####On va essayer avec le lag

df_tariff = (
    df_final[['Country Name', 'year', 'tariff', 'tariff_type']]
    .drop_duplicates()
    .sort_values(['Country Name', 'year'])
)

df_tariff = df_tariff.sort_values(['Country Name', 'tariff_type', 'year'])

df_tariff['tariff_lag1'] = (
    df_tariff
    .groupby(['Country Name', 'tariff_type'])['tariff']
    .shift(1)
)

df_tariff['delta_tariff'] = (
    df_tariff['tariff'] - df_tariff['tariff_lag1']
)


df_final = df_final.merge(
    df_tariff,
    on=['Country Name', 'year', 'tariff_type', 'tariff'],
    how='left'
)

#df_final.to_csv("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data_clean/Indicators and tarifs V3.csv")

# Crée le tarif retardé d'un an puis la diff de tarif
#df_final['tariff_lag1'] = (
#    df_final
#    .groupby('Country Name')['tariff']
#    .shift(1)
#)

#df_final['delta_tariff'] = df_final['tariff'] - df_final['tariff_lag1']

df_france = df_final[(df_final["Country Code"] == "FRA") & (df_final["tariff_type"] == "AR")]
df_france = df_france[ (df_france["Mesure"] == "Production volume") & (df_france["Activité économique"] == "Industrie (sauf construction)") &(df_france["Unité de mesure"] == "Taux de croissance")]

plt.scatter(df_france['tariff_lag1'], df_france['OBS_VALUE'])
plt.xlabel("Tarifs douaniers en % (t-1)")
plt.ylabel("Taux de croissance de la production en % (t)")
plt.title("Production vs tarifs douaniers retardés d'un an (France, 1988-2022)")
plt.show()




#Avec le delta des tarifs ?
plt.scatter(df_france['delta_tariff'], df_france['OBS_VALUE'])
plt.xlabel("Evolution des Tarifs douaniers en points de pourcentage (t-1 à t)")
plt.ylabel("Taux de croissance de la production en % (t)")
plt.title("Production vs delta tarifs douaniers retardés d'un an (France, 1988-2022)")
sns.regplot(data=df_france, x='delta_tariff', y='OBS_VALUE', scatter=False, color='red')
plt.show()






#Peu concluant, on va passer à des régressions contrôlées avec un peu de taff de preprocessing avant
import pandas as pd
df_final = pd.read_csv("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data_clean/Indicators and tarifs V3.csv")


print(f"Après chargement V3.csv: {len(df_final)}")

print(len(df_final[(df_final["Mesure"] == "Prix à la consommation") & (df_final["Unité de mesure"] == "Taux de croissance")]))


####Réorganisation du df

vars_utiles = [
    'Production volume',
    'Produit intérieur brut, volume',
    'Taux de chômage', 
    'Balance des transactions courantes en pourcentage du PIB',
    'Taux de change nominal', 
    'Prix à la consommation', 
    'Cours des actions', 
    'Importations de biens et services, volume', 
    'M3',
    'Emploi'
]

df_sub = df_final[df_final['Mesure'].isin(vars_utiles)]
print(f"Après sélection vars_utiles: {len(df_sub)}")

#for col in df_sub.columns : 
   # print (df_sub[col].unique())

df_sub = df_sub.drop(columns=['TIME_PERIOD', 'BASE_PER'])
print(f"Après drop colonnes: {len(df_sub)}")

# ----------- Relaxer les filtres pour maximiser le panel -----------
# - garder plusieurs statuts d'observation (pas seulement "Normal value")
# - garder toutes les catégories de tariff_type (AR + FN)
# - conserver l'indicateur tarifaire le plus cohérent (ici le seul présent)
keep_statuts = [
    "Normal value",
    "Provisional value",
    "Estimated value",
    "Time series break",
    "Definition differs",
]
df_sub = df_sub[df_sub["Statut d'observation"].isin(keep_statuts)]
# On garde l'indicateur tarifaire principal (unique dans le jeu de données)
df_sub = df_sub[df_sub["Indicator Name"] == 'Weighted mean tariff rate (MFN vs Applied)']
# Garder les deux types de tarifs (AR et FN) pour tracer/contrôler ensuite
df_sub = df_sub[df_sub["tariff_type"].isin(["AR", "FN"])]
print(f"Après filtres relaxés (statut + indicator + tariff_type AR/FN): {len(df_sub)}")

# Vérifier les doublons avant pivot
duplicates = df_sub.groupby(['Country Code', 'year', 'Mesure']).size()
print(f"Nombre de groupes avec >1 occurrence: {(duplicates > 1).sum()}")
if (duplicates > 1).any():
    print("Exemples de doublons:")
    print(duplicates[duplicates > 1].head())


#Petit checkup de ce que ça a donné
len(df_sub)
df_sub["Activité économique"].value_counts()
df_sub["Mesure"].unique()
df_sub["Transformation"].unique()


df_wide = df_sub.pivot_table(
    index= ['Country Name', 'Country Code', 'year'],   # 1 ligne = 1 pays × année
    columns='Mesure',         # chaque variable devient une colonne
    values='OBS_VALUE'
).reset_index()
print(f"Après pivot_table: {len(df_wide)}")



# Conserver les tarifs AR et FN séparés, pour identifier le type exact
tariffs = (
    df_sub[['Country Name', 'Country Code', 'year', 'tariff_type', 'tariff']]
    .drop_duplicates()
    .pivot_table(index=['Country Name', 'Country Code', 'year'], columns='tariff_type', values='tariff')
    .reset_index()
)
tariffs.columns.name = None
if 'AR' in tariffs.columns:
    tariffs = tariffs.rename(columns={'AR': 'tariff_AR'})
if 'FN' in tariffs.columns:
    tariffs = tariffs.rename(columns={'FN': 'tariff_FN'})
# Pas de colonne tarif sélectionné : on conserve deux séries distinctes AR et FN
tariffs = tariffs.drop(columns=['tariff'], errors='ignore')
print(f"Tariffs lines (AR+FN): {len(tariffs)}")

df_reg = df_wide.merge(tariffs, on=['Country Name', 'year', 'Country Code'], how='left')
print(f"Après merge tarifs: {len(df_reg)}")

# Calcul lag + delta par type de tarif (sans imputation)
df_reg = df_reg.sort_values(["Country Code", "year"])

df_reg['lag_AR'] = df_reg.groupby('Country Code')['tariff_AR'].shift(1)
df_reg['delta_AR'] = df_reg['tariff_AR'] - df_reg['lag_AR']

df_reg['lag_FN'] = df_reg.groupby('Country Code')['tariff_FN'].shift(1)
df_reg['delta_FN'] = df_reg['tariff_FN'] - df_reg['lag_FN']

# On ne garde pas l'ancienne colonne 'tariff' si elle existe
df_reg = df_reg.drop(columns=['tariff'], errors='ignore')

print(f"Après calcul lag/delta AR/FN (pas d'imputation) : {len(df_reg)} lignes")



df_reg.to_csv("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App-1/Data_clean/df_long_indicators_vs_tarifs_31mars.csv", index=False)
print(f"Fichier 31 mars (avec Emploi et tariff_type FN) sauvegardé avec {len(df_reg)} lignes.")








import statsmodels.formula.api as smf

formula = """
Q("Produit intérieur brut, volume") 
~ delta_FN
+ Q("Taux de chômage")
+ Q("Balance des transactions courantes en pourcentage du PIB")
+ Q("Taux de change nominal")
+ Q("Prix à la consommation")
+ Q("Cours des actions")
"""

model = smf.ols(formula, data=df_reg).fit(cov_type='HC1')
print(model.summary())

#with open("regression_results.txt", "w") as f:
    #f.write(model.summary().as_text())



####Piste d'amélioration de la régression : 
# faire sur le delta d'avant, 
# pour représenter le délai avant que les effets
#du protectionnisme se fassent sentir

#On crée le lag du delta_FN
df_reg = df_reg.sort_values(["Country Code", "year"])

df_reg["delta_FN_lag1"] = (
    df_reg
    .groupby("Country Code")["delta_FN"]
    .shift(1)
)
#Petit checkup
df_reg[["Country Name", "year", "delta_FN", "delta_FN_lag1"]].head(15)


len(df_reg)
#Puis régression longue
import statsmodels.formula.api as smf

formula = """
Q("Produit intérieur brut, volume") ~
delta_FN_lag1 +
Q("Taux de chômage") +
Q("Prix à la consommation") +
Q("M3") 
"""




model = smf.ols(formula, data=df_reg).fit(cov_type="HC1")
print(model.summary())

#with open("regression2_delta_tarifs_lag.txt", "w") as f:
    #f.write(model.summary().as_text())


df_reg[ "Produit intérieur brut, volume"].unique()



