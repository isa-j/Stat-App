#Tariffs
import pandas as pd


path_AR = "/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data/API_TM.TAX.MRCH.WM.AR.ZS_DS2_en_excel_v2_129989.xls"
df_AR = pd.read_excel(path_AR, header = 3)


path_FN = "/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data/API_TM.TAX.MRCH.WM.FN.ZS_DS2_en_excel_v2_128528.xls"
df_FN = pd.read_excel(path_FN, header = 3)

print(df_FN.columns)
print(df_AR.columns)




# Colonnes d'identité (à adapter si besoin)
id_cols = [
    "Country Name",
    "Country Code",
    "Indicator Name",
    "Indicator Code"
]

merge_col = [ "Country Name" , "Country Code"]

# Colonnes années = tout le reste
year_cols_fn = [c for c in df_FN.columns if c not in id_cols]
year_cols_ar = [c for c in df_AR.columns if c not in id_cols]

# Renommage
df_fn = df_FN.rename(columns={c: f"{c}_FN" for c in year_cols_fn})

df_ar = df_AR.rename(columns={c: f"{c}_AR" for c in year_cols_ar})


df_merged = df_fn.merge(
    df_ar,
    on=merge_col,
    how="inner"
)


df_merged["Indicator Name_x"] = "Weighted mean tariff rate (MFN vs Applied)"


df_merged = df_merged.drop(columns = {"Indicator Code_x", "Indicator Name_y", "Indicator Code_y"})
df_merged = df_merged.rename(columns = {"Indicator Name_x" : "Indicator Name"})



#### Avant de sauvegarder on se débarasse des années où on a aucune données

# Colonnes d'identification
id_cols = ["Country Name", "Country Code", "Indicator Name"]

# Colonnes années
year_cols = [c for c in df_merged.columns if c not in id_cols]

# Extraire les années uniques (sans suffixe)
years = sorted({c.split("_")[0] for c in year_cols})

# Liste des colonnes à supprimer
cols_to_drop = []

for y in years:
    fn_col = f"{y}_FN"
    ar_col = f"{y}_AR"
    
    # Vérifier si toutes les lignes sont NaN pour les deux colonnes
    if df_merged[[fn_col, ar_col]].isna().all().all():
        cols_to_drop.extend([fn_col, ar_col])
        print(f"Year {y} is completely empty → will drop")

# Supprimer les colonnes vides
df_merged = df_merged.drop(columns=cols_to_drop)



df_merged.to_csv("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App/Data_clean/Tarifs douaniers (MFN vs Applied).csv")