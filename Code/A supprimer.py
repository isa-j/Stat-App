import pandas as pd

df = pd.read_csv("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App-1/Data_clean/Indicators and tarifs V3.csv")
df["Mesure"].unique()
df_PIB = df[df["Mesure"] == "Production volume"]
df_PIB.columns
print(df_PIB.tail(50))