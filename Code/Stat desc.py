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


