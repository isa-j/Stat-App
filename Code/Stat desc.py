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
    'M3'
]

df_sub = df_final[df_final['Mesure'].isin(vars_utiles)]

#for col in df_sub.columns : 
   # print (df_sub[col].unique())

df_sub = df_sub.drop(columns=['TIME_PERIOD', 'BASE_PER'])

df_sub = df_sub[(df_sub["Statut d'observation"] == "Normal value") 
                & (df_sub["Activité économique"].isin(["Non applicable", "Total - ensemble des activités"]))
                & (df_sub["Ajustement"].isin(["Corrigé des variations saisonnières et des effets de calendrier", "N'est pas applicable"]))
                & (df_sub["Indicator Name"] == 'Weighted mean tariff rate (MFN vs Applied)')
                & (df_sub["tariff_type"] == "AR")
                ]


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

tariffs = df_sub[['Country Name', 'Country Code', 'year', 'tariff', 'tariff_lag1', 'delta_tariff']].drop_duplicates()
df_reg = df_wide.merge(tariffs, on=['Country Name', 'year'], how='left')

df_reg.head(100)


len(df_reg)








import statsmodels.formula.api as smf

formula = """
Q("Produit intérieur brut, volume") 
~ delta_tariff
+ Q("Taux de chômage")
+ Q("Balance des transactions courantes en pourcentage du PIB")
+ Q("Taux de change nominal")
+ Q("Prix à la consommation")
+ Q("Cours des actions")
"""

model = smf.ols(formula, data=df_reg).fit(cov_type='HC1')
print(model.summary())

with open("regression_results.txt", "w") as f:
    f.write(model.summary().as_text())




