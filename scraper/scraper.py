import os
import json
import shutil
import pandas as pd
import time
import random
from kaggle.api.kaggle_api_extended import KaggleApi

# Kaggle Credentials
DATASET_NAME = "hubertsidorowicz/football-players-stats-2025-2026"
UPLOAD_FOLDER = "kaggle_upload"

# URL-e for scraping
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
    """ Authenticates Kaggle API """
    kaggle_username = os.getenv("KAGGLE_USERNAME")
    kaggle_key = os.getenv("KAGGLE_KEY")

    if not kaggle_username or not kaggle_key:
        raise ValueError("Missing KAGGLE_USERNAME or KAGGLE_KEY in environment variables!")

    os.environ["KAGGLE_USERNAME"] = kaggle_username
    os.environ["KAGGLE_KEY"] = kaggle_key
    print("Kaggle API authentication successful!")


def scrape_table(url, table_id):
    """ Retrieves a table from the given URL """
    try:
        df = pd.read_html(url, attrs={"id": table_id})[0]
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(0)
        df = df.loc[:, ~df.columns.duplicated()]
        if 'Player' in df.columns:
            df = df[df['Player'] != 'Player']
        print(f"Retrieved: {table_id}")
        return df
    except Exception as e:
        print(f"Error retrieving {table_id}: {e}")
        return None


def scrape_all_tables():
    """ Retrieves all tables """
    dfs = {}
    for url, table_id in URLS.items():
        df = scrape_table(url, table_id)
        if df is not None:
            dfs[table_id] = df
        time.sleep(random.uniform(1, 2))
    return dfs


def merge_dataframes(dfs):
    """ Merges retrieved tables """
    if 'stats_standard' not in dfs:
        raise ValueError("Missing main table 'stats_standard'!")
    merged_df = dfs['stats_standard']
    for name, df in dfs.items():
        if name != 'stats_standard':
            merged_df = merged_df.merge(df, on=['Player', 'Squad'], how='left', suffixes=('', f'_{name}'))
    return merged_df


def remove_unwanted_columns(df):
    """ Removes columns containing 'matches' """
    return df.drop(columns=[col for col in df.columns if "matches" in col.lower()], errors='ignore')


def fix_age_format(df):
    """
    Converts the 'Age' column from 'yy-ddd' format to 'yy' (years only).
    
    Example:
    - '22-150' -> '22'
    - '19-032' -> '19'
    """
    if 'Age' in df.columns:
        df['Age'] = df['Age'].astype(str).str.split('-').str[0]
        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
    return df


def upload_dataset(df_full, df_light):
    """ Uploads data to Kaggle API with column descriptions directly in metadata. """
    api = KaggleApi()
    api.authenticate()
    
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

    full_path = os.path.join(UPLOAD_FOLDER, "players_data-2025_2026.csv")
    light_path = os.path.join(UPLOAD_FOLDER, "players_data_light-2025_2026.csv")

    df_full.to_csv(full_path, index=False)
    df_light.to_csv(light_path, index=False)

    # Column descriptions for Kaggle UI
    COLUMN_DESCRIPTIONS = {
        "Rk": "Ranking of the player",
        "Player": "Name of the player",
        "Nation": "Nationality of the player",
        "Pos": "Position on the field",
        "Squad": "Team name",
        "Comp": "League competition",
        "Age": "Player's age in years",
        "Born": "Year of birth",
        "MP": "Matches played",
        "Starts": "Number of matches started",
        "Min": "Total minutes played",
        "90s": "Minutes played divided by 90 (full match equivalent)",
        "Gls": "Total goals scored",
        "Ast": "Total assists",
        "G+A": "Total goals and assists",
        "G-PK": "Goals excluding penalties",
        "PK": "Penalty kicks scored",
        "PKatt": "Penalty kick attempts",
        "CrdY": "Yellow cards received",
        "CrdR": "Red cards received",
        "xG": "Expected goals",
        "npxG": "Non-penalty expected goals",
        "xAG": "Expected assists",
        "npxG+xAG": "Sum of non-penalty xG and xAG",
        "PrgC": "Progressive carries",
        "PrgP": "Progressive passes",
        "PrgR": "Progressive runs",
        "Sh": "Total shots attempted",
        "SoT": "Shots on target",
        "SoT%": "Percentage of shots on target",
        "Sh/90": "Shots per 90 minutes",
        "SoT/90": "Shots on target per 90 minutes",
        "G/Sh": "Goals per shot",
        "G/SoT": "Goals per shot on target",
        "Dist": "Average shot distance (yards)",
        "FK": "Free kicks taken"
    }

    # Convert column descriptions to Kaggle format
    column_metadata = [{"name": col, "description": desc} for col, desc in COLUMN_DESCRIPTIONS.items()]

    # Creating metadata with column descriptions
    metadata = {
        "title": "Football Players Stats 2025-2026",
        "id": DATASET_NAME,
        "licenses": [{"name": "CC0-1.0"}],
        "columns": column_metadata,  # Column descriptions for Kaggle UI
        "files": [
            {
                "name": "players_data-2025_2026.csv",
                "description": "Complete dataset with all player statistics for the 2025-2026 season."
            },
            {
                "name": "players_data_light-2025_2026.csv",
                "description": "Lighter version of the dataset containing only key statistics."
            }
        ]
    }

    metadata_path = os.path.join(UPLOAD_FOLDER, "dataset-metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=4)

    print("Uploading new dataset version with column descriptions...")
    api.dataset_create_version(
        UPLOAD_FOLDER,
        version_notes="Updated statistics for the 2025/2026 season with column descriptions.",
        delete_old_versions=False
    )
    print("New dataset version with column descriptions has been published!")


def run_pipeline():
    """ Runs the entire pipeline """
    print("Starting data scraping...")
    dfs = scrape_all_tables()
    merged_df = merge_dataframes(dfs)
    df_cleaned = remove_unwanted_columns(merged_df)
    df_cleaned_fixed_age = fix_age_format(df_cleaned)

    keep_columns = [
    'Rk', 'Player', 'Nation', 'Pos', 'Squad', 'Comp', 'Age', 'Born', 'MP', 'Starts', 'Min', '90s','Gls', 'Ast', 'G+A', 'G-PK', 'PK', 'PKatt','CrdY', 'CrdR','xG', 'npxG', 'xAG', 'npxG+xAG', 'G+A-PK', 'xG+xAG','PrgC', 'PrgP', 'PrgR','Sh', 'SoT', 'SoT%', 'Sh/90', 'SoT/90', 'G/Sh', 'G/SoT', 'Dist', 'FK','PK_stats_shooting', 'PKatt_stats_shooting', 'xG_stats_shooting', 'npxG_stats_shooting', 'npxG/Sh', 'G-xG', 'np:G-xG','Cmp', 'Att', 'Cmp%', 'TotDist', 'PrgDist', 'Ast_stats_passing', 'xAG_stats_passing', 'xA', 'A-xAG','KP', '1/3', 'PPA', 'CrsPA', 'PrgP_stats_passing','Live', 'Dead', 'FK_stats_passing_types', 'TB', 'Sw', 'Crs', 'TI', 'CK', 'In', 'Out', 'Str', 'Cmp_stats_passing_types','Tkl', 'TklW', 'Def 3rd', 'Mid 3rd', 'Att 3rd', 'Att_stats_defense', 'Tkl%', 'Lost','Blocks_stats_defense', 'Sh_stats_defense', 'Pass', 'Int', 'Tkl+Int', 'Clr', 'Err','SCA', 'SCA90', 'PassLive', 'PassDead', 'TO', 'Sh_stats_gca', 'Fld', 'Def', 'GCA', 'GCA90','Touches', 'Def Pen', 'Def 3rd_stats_possession', 'Mid 3rd_stats_possession', 'Att 3rd_stats_possession', 'Att Pen','Live_stats_possession', 'Att_stats_possession', 'Succ', 'Succ%', 'Tkld', 'Tkld%', 'Carries','TotDist_stats_possession', 'PrgDist_stats_possession', 'PrgC_stats_possession', '1/3_stats_possession', 'CPA','Mis', 'Dis', 'Rec', 'PrgR_stats_possession','CrdY_stats_misc', 'CrdR_stats_misc', '2CrdY', 'Fls', 'Fld_stats_misc', 'Off_stats_misc', 'Crs_stats_misc','Int_stats_misc', 'TklW_stats_misc', 'PKwon', 'PKcon', 'OG', 'Recov', 'Won', 'Lost_stats_misc', 'Won%','GA', 'GA90', 'SoTA', 'Saves', 'Save%', 'W', 'D', 'L', 'CS', 'CS%', 'PKatt_stats_keeper', 'PKA', 'PKsv', 'PKm','PSxG', 'PSxG/SoT', 'PSxG+/-', '/90', 'Cmp_stats_keeper_adv', 'Att_stats_keeper_adv', 'Cmp%_stats_keeper_adv','Att (GK)', 'Thr', 'Launch%', 'AvgLen', 'Opp', 'Stp', 'Stp%', '#OPA', '#OPA/90', 'AvgDist'
    ]
    df_light = df_cleaned_fixed_age[keep_columns]
    upload_dataset(df_cleaned_fixed_age, df_light)


authenticate_kaggle()
run_pipeline()
