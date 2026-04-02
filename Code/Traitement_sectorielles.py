import pandas as pd
import numpy as np
import os
import glob
from pathlib import Path

# ============================================================================
# PREPROCESSING DES DONNÉES SECTORIELLES DE TARIFS DOUANIERS
# ============================================================================

# Chemin racine des données
DATA_PATH = "/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App-1/Data/2662006_A69F6B0B-8"

def load_and_clean_mfn_data(data_path):
    """
    Charge tous les fichiers CSV annuels de tarifs MFN et les combine.
    
    Parameters:
    -----------
    data_path : str
        Chemin vers le dossier contenant les sous-dossiers MFN_*
    
    Returns:
    --------
    pd.DataFrame
        Dataframe consolidé avec tous les données nettoyées
    """
    
    # Trouver tous les dossiers MFN_*
    mfn_folders = sorted(glob.glob(os.path.join(data_path, "MFN_*")))
    print(f"✓ {len(mfn_folders)} dossiers MFN trouvés\n")
    
    dataframes = []
    
    # Charger chaque fichier CSV
    for folder in mfn_folders:
        csv_files = glob.glob(os.path.join(folder, "*.CSV"))
        if csv_files:
            csv_file = csv_files[0]  # Il y a un seul CSV par dossier
            try:
                # Charger le CSV
                df = pd.read_csv(csv_file)
                dataframes.append(df)
                folder_name = os.path.basename(folder)
                print(f"  ✓ Chargé: {folder_name} ({len(df)} lignes)")
            except Exception as e:
                print(f"  ✗ Erreur lors du chargement de {csv_file}: {e}")
    
    # Combiner tous les dataframes
    print(f"\n✓ Combinaison des {len(dataframes)} fichiers...")
    df_combined = pd.concat(dataframes, ignore_index=True)
    print(f"✓ Dataframe combiné: {df_combined.shape[0]} lignes, {df_combined.shape[1]} colonnes\n")
    
    return df_combined


def clean_data(df):
    """
    Nettoie les données (types, valeurs manquantes, etc.)
    
    Parameters:
    -----------
    df : pd.DataFrame
        Dataframe brut à nettoyer
    
    Returns:
    --------
    pd.DataFrame
        Dataframe nettoyé
    """
    
    print("=" * 70)
    print("NETTOYAGE DES DONNÉES")
    print("=" * 70)
    
    # Créer une copie
    df = df.copy()
    
    # Afficher les types et info avant
    print("\n État avant nettoyage:")
    print(f"  Shape: {df.shape}")
    print(f"  Colonnes: {list(df.columns)}")
    
    # Les colonnes de tarifs peuvent avoir des espaces (données manquantes)
    tariff_columns = ['Sum_Of_Rates', 'Min_Rate', 'Max_Rate', 'SimpleAverage']
    
    print(f"\n Conversion des colonnes numériques...")
    for col in tariff_columns:
        if col in df.columns:
            # Convertir en float, les valeurs inválides deviennent NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            missing_count = df[col].isna().sum()
            print(f"  {col}: {missing_count} valeurs manquantes ({missing_count/len(df)*100:.1f}%)")
    
    # Colonnes qui doivent être des entiers
    int_columns = ['Nbr_NA_Lines', 'Nbr_Free_Lines', 'Nbr_AVE_Lines', 
                   'Nbr_Dutiable_Lines', 'TotalNoOfValidLines', 'TotalNoOfLines']
    
    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    # ProductCode en string
    if 'ProductCode' in df.columns:
        df['ProductCode'] = df['ProductCode'].astype(str)
    
    # Year en integer
    if 'Year' in df.columns:
        df['Year'] = df['Year'].astype('int32')
    
    # Reporter_ISO_N en integer
    if 'Reporter_ISO_N' in df.columns:
        df['Reporter_ISO_N'] = pd.to_numeric(df['Reporter_ISO_N'], errors='coerce').astype('Int32')
    
    print(f"\n Nettoyage complété")
    print(f"  Shape finale: {df.shape}")
    
    return df


def descriptive_analysis(df):
    """
    Fait une analyse descriptive rapide des données.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Dataframe à analyser
    """
    
    print("\n" + "=" * 70)
    print("ANALYSE DESCRIPTIVE DES DONNÉES")
    print("=" * 70)
    
    # Nombre de produits uniques
    num_products = df['ProductCode'].nunique()
    print(f"\n PRODUITS")
    print(f"  Nombre de produits différents: {num_products}")
    
    # Années couvertes
    years = sorted(df['Year'].unique())
    print(f"\n PÉRIODE")
    print(f"  Années couvertes: {years[0]} à {years[-1]} ({len(years)} années)")
    print(f"  {len(years)} fichiers annuels")
    
    # Reporters (pays)
    num_reporters = df['Reporter_ISO_N'].nunique()
    print(f"\n REPORTERS (PAYS)")
    print(f"  Nombre de reporters: {num_reporters}")
    
    # Statistiques sur les tarifs
    print(f"\n STATISTIQUES SUR LES TARIFS")
    tariff_cols = ['Sum_Of_Rates', 'Min_Rate', 'Max_Rate', 'SimpleAverage']
    
    for col in tariff_cols:
        if col in df.columns:
            print(f"\n  {col}:")
            print(f"    Non-manquant: {df[col].notna().sum()} ({df[col].notna().sum()/len(df)*100:.1f}%)")
            print(f"    Min: {df[col].min():.2f}")
            print(f"    Max: {df[col].max():.2f}")
            print(f"    Moyenne: {df[col].mean():.2f}")
            print(f"    Médiane: {df[col].median():.2f}")
    
    # Lignes de données
    print(f"\n COMPOSITION DES LIGNES")
    print(f"  TotalNoOfLines (moyenne par produit-année): {df['TotalNoOfLines'].mean():.1f}")
    print(f"  TotalNoOfValidLines (moyenne): {df['TotalNoOfValidLines'].mean():.1f}")
    print(f"  Nbr_Free_Lines (moyenne): {df['Nbr_Free_Lines'].mean():.1f}")
    print(f"  Nbr_Dutiable_Lines (moyenne): {df['Nbr_Dutiable_Lines'].mean():.1f}")
    
    # Données manquantes globales
    print(f"\n  VALEURS MANQUANTES")
    missing = df.isnull().sum()
    if missing.sum() > 0:
        print(missing[missing > 0])
    else:
        print("  Aucune valeur manquante")
    
    # Distribution des données par année
    print(f"\n DISTRIBUTION PAR ANNÉE")
    distribution = df.groupby('Year').size()
    print(f"  Nombre d'observations par année:")
    print(f"    Min: {distribution.min()} ({distribution.idxmin()})")
    print(f"    Max: {distribution.max()} ({distribution.idxmax()})")
    print(f"    Moyenne: {distribution.mean():.0f}")
    
    print("\n" + "=" * 70)
    
    return num_products


# ============================================================================
# EXÉCUTION
# ============================================================================

if __name__ == "__main__":
    # Charger et combiner tous les fichiers
    df_tarifs = load_and_clean_mfn_data(DATA_PATH)
    
    # Nettoyer les données
    df_tarifs = clean_data(df_tarifs)
    
    # Analyse descriptive
    num_produits = descriptive_analysis(df_tarifs)
    
    # Afficher les premières lignes
    print("\n APERÇU DES DONNÉES (10 premières lignes):")
    print(df_tarifs.head(10))
    
    print("\n✅ Preprocessing terminé!")
    print(f"DataFrame final: {df_tarifs.shape[0]} lignes × {df_tarifs.shape[1]} colonnes")
    print(f"Prêt pour l'analyse (variable: df_tarifs)")

df_tarifs