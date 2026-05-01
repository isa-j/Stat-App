import pandas as pd
import numpy as np
import os
import glob
from pathlib import Path
import itertools

# ============================================================================
# PREPROCESSING DES DONNÉES SECTORIELLES DE TARIFS DOUANIERS
# ============================================================================

# Chemin racine des données
DATA_PATH = "/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App-1/Data/2662006_A69F6B0B-8"
MACRO_DATA_PATH = "/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App-1/Data_clean/df_long_indicators_vs_tarifs_31mars.csv"

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


def filter_raw_materials(df_tarifs):
    """
    Filtre les données pour ne garder que les matières premières.
    
    Parameters:
    -----------
    df_tarifs : pd.DataFrame
        Dataframe des tarifs sectoriels nettoyés
    
    Returns:
    --------
    pd.DataFrame
        Dataframe filtré sur les matières premières
    """
    
    print("\n" + "=" * 70)
    print("FILTRAGE MATIÈRES PREMIÈRES")
    print("=" * 70)
    
    # Créer colonne chapitre HS
    df_tarifs = df_tarifs.copy()
    df_tarifs['HS_Chapter'] = df_tarifs['ProductCode'].astype(str).str[:2]
    
    # Chapitres HS pour matières premières
    raw_materials_chapters = [
        '10',  # Céréales
        '12',  # Graines et fruits oléagineux
        '25',  # Sel, soufre, terres et pierres (inclut ciment)
        '26',  # Minerais, scories et cendres
        '27',  # Combustibles minéraux (pétrole)
        '72',  # Fer et acier
        '73',  # Articles en fer ou acier
        '74',  # Cuivre et articles en cuivre
        '75',  # Nickel et articles en nickel
        '76',  # Aluminium et articles en aluminium
        '78',  # Plomb et articles en plomb
        '79',  # Zinc et articles en zinc
        '80',  # Étain et articles en étain
        '81'   # Autres métaux communs
    ]
    
    # Filtrer
    df_filtered = df_tarifs[df_tarifs['HS_Chapter'].isin(raw_materials_chapters)].copy()
    
    print(f"✓ Filtrage complété:")
    print(f"  Avant: {len(df_tarifs)} lignes")
    print(f"  Après: {len(df_filtered)} lignes")
    print(f"  Ratio: {len(df_filtered)/len(df_tarifs)*100:.1f}%")
    
    # Statistiques par chapitre
    chapter_stats = df_filtered.groupby('HS_Chapter').agg({
        'ProductCode': 'nunique',
        'Year': ['min', 'max', lambda x: len(x)]
    }).round(0)
    chapter_stats.columns = ['Produits_uniques', 'Année_min', 'Année_max', 'Observations']
    
    # Noms des chapitres
    chapter_names = {
        '10': 'Céréales', '12': 'Graines oléagineuses', '25': 'Sel/Soufre/Terres/Pierres',
        '26': 'Minerais', '27': 'Combustibles (Pétrole)', '72': 'Fer/Acier',
        '73': 'Articles fer/acier', '74': 'Cuivre', '75': 'Nickel', '76': 'Aluminium',
        '78': 'Plomb', '79': 'Zinc', '80': 'Étain', '81': 'Autres métaux'
    }
    
    print(f"\n📋 Statistiques par secteur:")
    for chapter in sorted(raw_materials_chapters):
        if chapter in chapter_stats.index:
            stats = chapter_stats.loc[chapter]
            name = chapter_names.get(chapter, f'Chapitre {chapter}')
            print(f"  {chapter}: {name}")
            print(f"    Produits: {int(stats['Produits_uniques'])}, Obs: {int(stats['Observations'])}, Période: {int(stats['Année_min'])}-{int(stats['Année_max'])}")
    
    return df_filtered


def create_long_format_macro_with_products(df_macro_eu, df_sectoral_filtered):
    """
    Crée un dataframe long avec pays-année-produit.
    Stratégie simple : créer le produit cartésien puis merger avec les données sectorielles.
    """
    
    print("\n" + "=" * 70)
    print("CRÉATION DATAFRAME LONG (PAYS-ANNÉE-PRODUIT)")
    print("=" * 70)
    
    # D'abord, agréger les données sectorielles par Year et ProductCode
    # (moyenne si plusieurs reporteurs/pays pour le même produit-année)
    sectoral_agg = df_sectoral_filtered.groupby(['Year', 'ProductCode']).agg({
        'SimpleAverage': 'mean',
        'Sum_Of_Rates': 'mean',
        'Min_Rate': 'mean',
        'Max_Rate': 'mean',
        'Nbr_Free_Lines': 'sum',
        'Nbr_Dutiable_Lines': 'sum',
        'TotalNoOfLines': 'sum'
    }).reset_index()
    
    countries = df_macro_eu['Country Code'].unique()
    products = sectoral_agg['ProductCode'].unique()
    years = df_macro_eu['year'].unique()
    
    print(f"📊 Dimensions:")
    print(f"  Pays: {len(countries)}")
    print(f"  Années: {len(years)}")
    print(f"  Produits matières premières: {len(products)}")
    print(f"  Total lignes attendu: {len(countries) * len(years) * len(products):,}")
    
    # Créer le produit cartésien : pays × année × produit
    combinations = list(itertools.product(countries, years, products))
    df_long = pd.DataFrame(combinations, columns=['Country Code', 'year', 'ProductCode'])
    
    # Ajouter les noms de pays
    country_names = df_macro_eu[['Country Code', 'Country Name']].drop_duplicates().set_index('Country Code')
    df_long['Country Name'] = df_long['Country Code'].map(country_names['Country Name'])
    
    # Merger avec les données macro
    macro_cols_to_keep = [col for col in df_macro_eu.columns 
                          if col not in ['Country Code', 'Country Name', 'Unnamed: 0', 'year']]
    df_long = df_long.merge(df_macro_eu[['Country Code', 'year'] + macro_cols_to_keep], 
                            on=['Country Code', 'year'], 
                            how='left')
    
    # Merger avec les données sectorielles agrégées
    df_long = df_long.merge(sectoral_agg, 
                           left_on=['year', 'ProductCode'], 
                           right_on=['Year', 'ProductCode'], 
                           how='left')
    
    # Nettoyer les colonnes
    df_long = df_long.drop('Year', axis=1)
    
    # Ajouter les colonnes sectorielles
    df_long['HS_Chapter'] = df_long['ProductCode'].astype(str).str[:2]
    sector_names = {
        '10': 'Céréales', '12': 'Graines oléagineuses', '25': 'Matériaux construction',
        '26': 'Minerais', '27': 'Énergie (Pétrole)', '72': 'Fer & Acier',
        '73': 'Produits fer/acier', '74': 'Cuivre', '75': 'Nickel', '76': 'Aluminium',
        '78': 'Plomb', '79': 'Zinc', '80': 'Étain', '81': 'Autres métaux'
    }
    df_long['Sector'] = df_long['HS_Chapter'].map(sector_names)
    
    print(f"✓ Dataframe long créé: {df_long.shape[0]:,} lignes × {df_long.shape[1]} colonnes")
    
    # Statistiques de complétude
    missing_tariffs = df_long['SimpleAverage'].isna().sum()
    total_obs = len(df_long)
    print(f"  Données tarifaires manquantes: {missing_tariffs:,} ({missing_tariffs/total_obs*100:.1f}%)")
    
    # Statistiques par secteur
    if 'SimpleAverage' in df_long.columns:
        sector_stats = df_long.groupby('Sector')['SimpleAverage'].agg(['count', 'mean', 'std']).round(3)
        print(f"\n📊 Statistiques par secteur:")
        for sector, stats in sector_stats.iterrows():
            print(f"  {sector}: {int(stats['count'])} obs, tarif moyen {stats['mean']:.2f}%, écart {stats['std']:.2f}%")
    
    return df_long


# ============================================================================
# EXÉCUTION
# ============================================================================

def create_sectoral_aggregated_df(df_long, df_macro_eu):
    """
    Crée un dataframe agrégé par secteur avec tarifs moyens.
    Restreint à 2 secteurs clés : Céréales et Métaux (regroupés)
    
    Parameters:
    -----------
    df_long : pd.DataFrame
        Dataframe long produit-année-pays
    df_macro_eu : pd.DataFrame
        Dataframe macro européen
    
    Returns:
    --------
    pd.DataFrame
        Dataframe avec index (Country Code, year) et colonnes tarifaires par secteur
    """
    
    print("\n" + "=" * 70)
    print("AGRÉGATION PAR SECTEUR (2 secteurs clés)")
    print("=" * 70)
    
    # Créer une colonne "secteur_agrégé" : Céréales vs Métaux
    df_long_agg = df_long.copy()
    
    # Définir les secteurs métallurgiques
    metal_sectors = ['Fer & Acier', 'Produits fer/acier', 'Cuivre', 'Nickel', 
                     'Aluminium', 'Plomb', 'Zinc', 'Étain', 'Autres métaux']
    
    # Créer colonne secteur agrégé
    df_long_agg['Secteur_Agregé'] = 'Autres'
    df_long_agg.loc[df_long_agg['Sector'] == 'Céréales', 'Secteur_Agregé'] = 'Céréales'
    df_long_agg.loc[df_long_agg['Sector'].isin(metal_sectors), 'Secteur_Agregé'] = 'Métaux'
    df_long_agg.loc[df_long_agg['Sector'] == 'Minerais', 'Secteur_Agregé'] = 'Minerais'
    
    # Grouper par Country Code, year, Secteur_Agregé et calculer tarif moyen
    sectoral_tariffs = df_long_agg.groupby(['Country Code', 'year', 'Secteur_Agregé']).agg({
        'SimpleAverage': 'mean',
        'Sum_Of_Rates': 'mean',
        'Nbr_Free_Lines': 'sum',
        'Nbr_Dutiable_Lines': 'sum'
    }).reset_index()
    
    # Garder seulement Céréales et Métaux
    sectoral_tariffs = sectoral_tariffs[sectoral_tariffs['Secteur_Agregé'].isin(['Céréales', 'Métaux'])]
    
    # Pivot pour avoir les secteurs en colonnes
    pivot_tariffs = sectoral_tariffs.pivot_table(
        index=['Country Code', 'year'],
        columns='Secteur_Agregé',
        values='SimpleAverage',
        aggfunc='mean'
    ).reset_index()
    
    # Renommer les colonnes de secteur
    pivot_tariffs.columns.name = None
    pivot_tariffs.columns = ['Country Code', 'year', 'tariff_cereales', 'tariff_metaux']
    
    print(f"✓ Tarifs sectoriels créés:")
    print(f"  Secteurs: Céréales et Métaux (regroupés)")
    
    # Merger avec les données macro
    macro_cols = [col for col in df_macro_eu.columns if col not in ['Unnamed: 0']]
    df_macro_clean = df_macro_eu[macro_cols].copy()
    
    # Merger
    df_sectoral = pivot_tariffs.merge(
        df_macro_clean,
        on=['Country Code', 'year'],
        how='left'
    )
    
    print(f"\n✓ Fusion avec données macro complétée")
    print(f"  Shape finale: {df_sectoral.shape[0]} lignes × {df_sectoral.shape[1]} colonnes")
    print(f"  Colonnes tarifaires: tariff_cereales, tariff_metaux")
    
    # Nettoyer les colonnes inutiles du fichier précédent
    cols_to_drop = ['tariff_AR', 'tariff_FN', 'lag_AR', 'delta_AR', 'lag_FN', 'delta_FN']
    df_sectoral = df_sectoral.drop(columns=cols_to_drop, errors='ignore')
    
    print(f"  ✓ Colonnes inutiles supprimées")
    print(f"  Shape après nettoyage: {df_sectoral.shape[0]} lignes × {df_sectoral.shape[1]} colonnes")
    
    return df_sectoral


if __name__ == "__main__":
    # Charger et combiner tous les fichiers
    df_tarifs = load_and_clean_mfn_data(DATA_PATH)
    
    # Nettoyer les données
    df_tarifs = clean_data(df_tarifs)
    
    # FILTRER LES MATIÈRES PREMIÈRES
    df_raw_materials = filter_raw_materials(df_tarifs)
    
    # Charger les données macro
    df_macro = pd.read_csv(MACRO_DATA_PATH)
    
    # Liste des pays européens
    european_countries = [
        'AUT', 'BEL', 'DEU', 'DNK', 'ESP', 'FIN', 'FRA', 'GBR',
        'GRC', 'IRL', 'ITA', 'NLD', 'PRT', 'SWE'
    ]
    
    # Filtrer les données macro sur pays européens
    df_macro_eu = df_macro[df_macro['Country Code'].isin(european_countries)].copy()
    print(f"\n✓ Données macro européennes: {df_macro_eu.shape[0]} lignes ({len(european_countries)} pays)")
    
    # CRÉER LE DATAFRAME LONG (produit-année-pays)
    df_long = create_long_format_macro_with_products(df_macro_eu, df_raw_materials)
    
    # CRÉER LE DATAFRAME SECTORIEL AGRÉGÉ
    df_sectoral = create_sectoral_aggregated_df(df_long, df_macro_eu)
    
    # Analyse descriptive
    num_produits = descriptive_analysis(df_tarifs)
    
    # Afficher les résultats
    print("\n📌 APERÇU DU DATAFRAME LONG FINAL:")
    print(f"Shape: {df_long.shape}")
    print(f"Colonnes: {list(df_long.columns)}")
    print("\nPremières lignes:")
    print(df_long.head())
    
    print("\n📌 APERÇU DU DATAFRAME SECTORIEL AGRÉGÉ:")
    print(f"Shape: {df_sectoral.shape}")
    print(f"Colonnes: {list(df_sectoral.columns)}")
    print("\nPremières lignes:")
    print(df_sectoral.head())
    
    print("\n✅ Preprocessing terminé!")
    print(f"DataFrame long: {df_long.shape[0]} lignes × {df_long.shape[1]} colonnes")
    print(f"DataFrame sectoriel: {df_sectoral.shape[0]} lignes × {df_sectoral.shape[1]} colonnes")
    print(f"Prêt pour l'analyse")
    
    # Sauvegarde optionnelle
    save_path_long = "/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App-1/Data_clean/df_long_macro_with_raw_materials.csv"
    df_long.to_csv(save_path_long, index=False)
    print(f"\n💾 DataFrame long sauvegardé: {save_path_long}")
    
    save_path_sectoral = "/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App-1/Data_clean/df_sectoral_tariffs.csv"
    df_sectoral.to_csv(save_path_sectoral, index=False)
    print(f"💾 DataFrame sectoriel sauvegardé: {save_path_sectoral}")