
"""
Get Data

SRP: Get Data From Kaggle
"""

from pathlib import Path

import kagglehub
import pandas as pd


# ==================================================
# Fetch Data
# ==================================================

def fetch_data(data_utils_config: dict) -> dict[str, pd.DataFrame]:

    # Config
    raw_data_path = Path(data_utils_config['raw_data_path'])
    kaggle_dataset = data_utils_config['kaggle_dataset']

    raw_data_path.mkdir(parents=True, exist_ok=True)

    # Check if data already exists
    existing_files = list(raw_data_path.glob('*.csv'))

    if not existing_files:
        # Download Data from Kaggle if not exists
        print("Downloading data from Kaggle...")
        dataset = kagglehub.dataset_download(kaggle_dataset)

        # Save to CSV
        for file in Path(dataset).glob('*.csv'):
            df = pd.read_csv(file)
            file_path = raw_data_path / file.name
            df.to_csv(file_path, index=False)
            print(f"Saved: {file_path}")

        existing_files = list(raw_data_path.glob('*.csv'))
    else:
        print(f"Data already exists in {raw_data_path}")

    # Load all CSV files into dictionary
    dataframes = {}
    for file_path in existing_files:
        dataset_name = file_path.stem
        df = pd.read_csv(file_path)
        dataframes[dataset_name] = df
        print(f"Loaded: {dataset_name} with {len(df)} rows")

    return dataframes



