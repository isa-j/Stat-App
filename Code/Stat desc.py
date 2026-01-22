import pandas as pd

df_indic = pd.read_parquet("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data_clean/Indicateurs.parquet")

df_tarifs = pd.read_csv("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data_clean/Tarifs douaniers (MFN vs Applied).csv")


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
df_final.to_parquet("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data_clean/Indicators and tarifs V2")


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
