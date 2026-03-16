import pandas as pd
df_final = pd.read_csv("C:/Users/lilic/Bureau/Dossiers/Dossiers non-triés/X/2025-09-4A/Projet Stat'App/Clone Git/Stat-App/Data_clean/Indicators and tarifs V3.csv")

df_final[df_final["Mesure"] == "Produit intérieur brut, volume"]["Unité de mesure"].unique()
#print(df_final["Mesure"].values )

####Réorganisation du df

vars_utiles = [
    'Production volume',
    'Produit intérieur brut, volume',
    'Emploi',
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
#    print (df_sub[col].unique())

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

df_sub = df_sub[~(
    (df_sub["Mesure"] == "Prix à la consommation") &
    (df_sub["Unité de mesure"] == "Taux de croissance")
)]


df_wide = df_sub.pivot_table(
    index= ['Country Name', 'Country Code', 'year'],   # 1 ligne = 1 pays × année
    columns='Mesure',         # chaque variable devient une colonne
    values='OBS_VALUE'
).reset_index()



tariffs = df_sub[['Country Name', 'Country Code', 'year', 'tariff', 'tariff_lag1', 'delta_tariff']].drop_duplicates()

df_reg = df_wide.merge(tariffs, on=['Country Name', 'year', 'Country Code'], how='left')

df_reg.head(100)

df_reg.to_csv("df_long_indicators vs tarifs.csv")

len(df_reg)
