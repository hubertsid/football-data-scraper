import os
import json
import shutil
import pandas as pd
import time
import random
from kaggle.api.kaggle_api_extended import KaggleApi

# 🔹 Konfiguracja Kaggle Dataset
DATASET_NAME = "hubertsidorowicz/football-players-stats-2024-2025"
UPLOAD_FOLDER = "kaggle_upload"

# 🔹 URL-e do scrapowania
URLS = {
    'https://fbref.com/en/comps/Big5/stats/players/Big-5-European-Leagues-Stats': 'stats_standard',
    'https://fbref.com/en/comps/Big5/shooting/players/Big-5-European-Leagues-Stats': 'stats_shooting',
    'https://fbref.com/en/comps/Big5/passing/players/Big-5-European-Leagues-Stats': 'stats_passing',
    'https://fbref.com/en/comps/Big5/passing_types/players/Big-5-European-Leagues-Stats': 'stats_passing_types',
    'https://fbref.com/en/comps/Big5/gca/players/Big-5-European-Leagues-Stats': 'stats_gca',
    'https://fbref.com/en/comps/Big5/defense/players/Big-5-European-Leagues-Stats': 'stats_defense',
    'https://fbref.com/en/comps/Big5/possession/players/Big-5-European-Leagues-Stats': 'stats_possession',
    'https://fbref.com/en/comps/Big5/playingtime/players/Big-5-European-Leagues-Stats': 'stats_playing_time',
    'https://fbref.com/en/comps/Big5/misc/players/Big-5-European-Leagues-Stats': 'stats_misc',
    'https://fbref.com/en/comps/Big5/keepers/players/Big-5-European-Leagues-Stats': 'stats_keeper',
    'https://fbref.com/en/comps/Big5/keepersadv/players/Big-5-European-Leagues-Stats': 'stats_keeper_adv'
}

def authenticate_kaggle():
    """ Autoryzuje API Kaggle """

    print("✅ Kaggle API authentication successful!")

def scrape_table(url, table_id):
    """ Pobiera tabelę z podanego URL """
    try:
        df = pd.read_html(url, attrs={"id": table_id})[0]
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(0)
        df = df.loc[:, ~df.columns.duplicated()]
        if 'Player' in df.columns:
            df = df[df['Player'] != 'Player']
        print(f"✅ Pobrano: {table_id}")
        return df
    except Exception as e:
        print(f"❌ Błąd pobierania {table_id}: {e}")
        return None

def scrape_all_tables():
    """ Pobiera wszystkie tabele """
    dfs = {}
    for url, table_id in URLS.items():
        df = scrape_table(url, table_id)
        if df is not None:
            dfs[table_id] = df
        time.sleep(random.uniform(1, 2))
    return dfs

def merge_dataframes(dfs):
    """ Łączy pobrane tabele """
    if 'stats_standard' not in dfs:
        raise ValueError("❌ Brak głównej tabeli 'stats_standard'!")
    merged_df = dfs['stats_standard']
    for name, df in dfs.items():
        if name != 'stats_standard':
            merged_df = merged_df.merge(df, on=['Player', 'Squad'], how='left', suffixes=('', f'_{name}'))
    return merged_df

def remove_unwanted_columns(df):
    """ Usuwa kolumny zawierające 'matches' """
    return df.drop(columns=[col for col in df.columns if "matches" in col.lower()], errors='ignore')

def upload_dataset(df_full, df_light):
    """ Wysyła dane do Kaggle API """
    api = KaggleApi()
    api.authenticate()
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    full_path = os.path.join(UPLOAD_FOLDER, "players_data-2024_2025.csv")
    light_path = os.path.join(UPLOAD_FOLDER, "players_data_light-2024_2025.csv")
    df_full.to_csv(full_path, index=False)
    df_light.to_csv(light_path, index=False)
    metadata = {
        "title": "Football Players Stats 2024-2025",
        "id": DATASET_NAME,
        "licenses": [{"name": "CC0-1.0"}]
    }
    metadata_path = os.path.join(UPLOAD_FOLDER, "dataset-metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f)
    print("⬆️ Wysyłanie nowej wersji datasetu...")
    api.dataset_create_version(
        UPLOAD_FOLDER,
        version_notes="Zaktualizowane statystyki na sezon 2024/2025",
        delete_old_versions=False
    )
    print("✅ Nowa wersja datasetu została opublikowana!")

def run_pipeline():
    """ Uruchamia cały pipeline """
    print("🚀 Rozpoczynam scrapowanie danych...")
    dfs = scrape_all_tables()
    merged_df = merge_dataframes(dfs)
    df_cleaned = remove_unwanted_columns(merged_df)
    keep_columns = [
    'Rk', 'Player', 'Nation', 'Pos', 'Squad', 'Comp', 'Age', 'Born', 'MP', 'Starts', 'Min', '90s','Gls', 'Ast', 'G+A', 'G-PK', 'PK', 'PKatt','CrdY', 'CrdR','xG', 'npxG', 'xAG', 'npxG+xAG', 'G+A-PK', 'xG+xAG','PrgC', 'PrgP', 'PrgR','Sh', 'SoT', 'SoT%', 'Sh/90', 'SoT/90', 'G/Sh', 'G/SoT', 'Dist', 'FK','PK_stats_shooting', 'PKatt_stats_shooting', 'xG_stats_shooting', 'npxG_stats_shooting', 'npxG/Sh', 'G-xG', 'np:G-xG','Cmp', 'Att', 'Cmp%', 'TotDist', 'PrgDist', 'Ast_stats_passing', 'xAG_stats_passing', 'xA', 'A-xAG','KP', '1/3', 'PPA', 'CrsPA', 'PrgP_stats_passing','Live', 'Dead', 'FK_stats_passing_types', 'TB', 'Sw', 'Crs', 'TI', 'CK', 'In', 'Out', 'Str', 'Cmp_stats_passing_types','Tkl', 'TklW', 'Def 3rd', 'Mid 3rd', 'Att 3rd', 'Att_stats_defense', 'Tkl%', 'Lost','Blocks_stats_defense', 'Sh_stats_defense', 'Pass', 'Int', 'Tkl+Int', 'Clr', 'Err','SCA', 'SCA90', 'PassLive', 'PassDead', 'TO', 'Sh_stats_gca', 'Fld', 'Def', 'GCA', 'GCA90','Touches', 'Def Pen', 'Def 3rd_stats_possession', 'Mid 3rd_stats_possession', 'Att 3rd_stats_possession', 'Att Pen','Live_stats_possession', 'Att_stats_possession', 'Succ', 'Succ%', 'Tkld', 'Tkld%', 'Carries','TotDist_stats_possession', 'PrgDist_stats_possession', 'PrgC_stats_possession', '1/3_stats_possession', 'CPA','Mis', 'Dis', 'Rec', 'PrgR_stats_possession','CrdY_stats_misc', 'CrdR_stats_misc', '2CrdY', 'Fls', 'Fld_stats_misc', 'Off_stats_misc', 'Crs_stats_misc','Int_stats_misc', 'TklW_stats_misc', 'PKwon', 'PKcon', 'OG', 'Recov', 'Won', 'Lost_stats_misc', 'Won%','GA', 'GA90', 'SoTA', 'Saves', 'Save%', 'W', 'D', 'L', 'CS', 'CS%', 'PKatt_stats_keeper', 'PKA', 'PKsv', 'PKm','PSxG', 'PSxG/SoT', 'PSxG+/-', '/90', 'Cmp_stats_keeper_adv', 'Att_stats_keeper_adv', 'Cmp%_stats_keeper_adv','Att (GK)', 'Thr', 'Launch%', 'AvgLen', 'Opp', 'Stp', 'Stp%', '#OPA', '#OPA/90', 'AvgDist'
    ]
    df_light = df_cleaned[keep_columns]
    upload_dataset(df_cleaned, df_light)

#authenticate_kaggle()
run_pipeline()
