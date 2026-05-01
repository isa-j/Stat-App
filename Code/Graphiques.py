

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Configuration du style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

#Graphique 1 : comparaison AR vs FN

def plot_tariff_evolution_japan():
    
    
    
    
    # Data
    df = pd.read_csv("/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App-1/Data_clean/df_long_indicators_vs_tarifs_31mars.csv")
    
    # Filtrer pour le Japon
    df_japan = df[df['Country Code'] == 'JPN'].sort_values('year').copy()
    df_japan = df_japan.dropna(subset=['tariff_AR', 'tariff_FN'])
    
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # Trace les deux séries
    ax.plot(df_japan['year'], df_japan['tariff_AR'], 
            marker='o', linewidth=2.5, markersize=6, 
            color='#2E86AB', label='Taux agrégé appliqué (AR)', alpha=0.8)
    
    ax.plot(df_japan['year'], df_japan['tariff_FN'], 
            marker='s', linewidth=2.5, markersize=6, 
            color='#A23B72', label='Taux Most Favored Nation (FN)', alpha=0.8)
    
    # Zone d'écart entre AR et FN
    ax.fill_between(df_japan['year'], 
                     df_japan['tariff_AR'], 
                     df_japan['tariff_FN'],
                     alpha=0.15, color='gray', label='Écart AR-FN')
    
    # Pour le Style du graphe
    ax.set_xlabel('Année', fontsize=12, fontweight='bold')
    ax.set_ylabel('Taux de tarif (%)', fontsize=12, fontweight='bold')
    ax.set_title('Évolution comparative des tarifs FN et AR pour le Japon (1988-2022)', 
                 fontsize=14, fontweight='bold', pad=20)
    
    # Grille et légende
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.legend(loc='best', fontsize=11, framealpha=0.95)
    
    # Ajoute des annotations pour les points clés
    # Min et max de AR
    min_ar_idx = df_japan['tariff_AR'].idxmin()
    max_ar_idx = df_japan['tariff_AR'].idxmax()
    
    ax.annotate(f"{df_japan.loc[min_ar_idx, 'tariff_AR']:.2f}%",
                xy=(df_japan.loc[min_ar_idx, 'year'], df_japan.loc[min_ar_idx, 'tariff_AR']),
                xytext=(10, -15), textcoords='offset points',
                fontsize=9, bbox=dict(boxstyle='round,pad=0.5', facecolor='#2E86AB', alpha=0.3),
                arrowprops=dict(arrowstyle='->', color='#2E86AB', lw=1))
    
    ax.annotate(f"{df_japan.loc[max_ar_idx, 'tariff_AR']:.2f}%",
                xy=(df_japan.loc[max_ar_idx, 'year'], df_japan.loc[max_ar_idx, 'tariff_AR']),
                xytext=(10, 15), textcoords='offset points',
                fontsize=9, bbox=dict(boxstyle='round,pad=0.5', facecolor='#2E86AB', alpha=0.3),
                arrowprops=dict(arrowstyle='->', color='#2E86AB', lw=1))
    
    # format axes
    ax.set_xlim(df_japan['year'].min() - 0.5, df_japan['year'].max() + 0.5)
    ax.set_ylim(df_japan['tariff_AR'].min() - 0.5, df_japan['tariff_FN'].max() + 0.5)
    
    # X-axis tous les 2-3 ans
    ax.set_xticks(np.arange(df_japan['year'].min(), df_japan['year'].max() + 1, 3))
    ax.tick_params(axis='both', labelsize=10)
    
    #  zone pour les crises (ça fait plus pro)
    ax.axvspan(2007, 2009, alpha=0.1, color='red', label='_nolegend_')
    ax.text(2008, ax.get_ylim()[1] * 0.95, 'Crise 2008', 
            ha='center', fontsize=9, style='italic', color='red', alpha=0.6)
    
    
    plt.tight_layout()
    
    # Save
    output_path = "/Users/roland/Desktop/ENSAE 2A/Statapp/Github/Stat-App-1/Résultats/Tariff_Evolution_Japan.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Graphique sauvegardé: {output_path}")
    
    # Afficher des statistiques en plus
    
    print("STATISTIQUES DESCRIPTIVES - TARIFS JAPON")
    
    print(f"\nTarif AR (Taux appliqué):")
    print(f"  Moyenne: {df_japan['tariff_AR'].mean():.3f}%")
    print(f"  Médiane: {df_japan['tariff_AR'].median():.3f}%")
    print(f"  Min: {df_japan['tariff_AR'].min():.3f}% (en {df_japan.loc[df_japan['tariff_AR'].idxmin(), 'year']:.0f})")
    print(f"  Max: {df_japan['tariff_AR'].max():.3f}% (en {df_japan.loc[df_japan['tariff_AR'].idxmax(), 'year']:.0f})")
    print(f"  Écart-type: {df_japan['tariff_AR'].std():.3f}%")
    
    print(f"\nTarif FN (Taux final/lié):")
    print(f"  Moyenne: {df_japan['tariff_FN'].mean():.3f}%")
    print(f"  Médiane: {df_japan['tariff_FN'].median():.3f}%")
    print(f"  Min: {df_japan['tariff_FN'].min():.3f}% (en {df_japan.loc[df_japan['tariff_FN'].idxmin(), 'year']:.0f})")
    print(f"  Max: {df_japan['tariff_FN'].max():.3f}% (en {df_japan.loc[df_japan['tariff_FN'].idxmax(), 'year']:.0f})")
    print(f"  Écart-type: {df_japan['tariff_FN'].std():.3f}%")
    
    print(f"\nÉcart AR-FN:")
    gap = df_japan['tariff_FN'] - df_japan['tariff_AR']
    print(f"  Moyenne: {gap.mean():.3f}%")
    print(f"  Min: {gap.min():.3f}%")
    print(f"  Max: {gap.max():.3f}%")
    
    
    
    plt.show()


#Exec

if __name__ == "__main__":
    
    
    plot_tariff_evolution_japan()
