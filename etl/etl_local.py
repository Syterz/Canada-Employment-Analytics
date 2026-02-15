import pandas as pd
from pathlib import Path
import glob

RAW_NAICS_PATH = Path("data/Canada NAICS/NAICS Canada Data.csv")
RAW_MONTHLY_PATH = Path ("data/Monthly Vac.csv")
QUARTERLY_VAC_PATH = Path("data/Canada Quarterly")

OUTPUT_PATH_NAICS = Path("data/Cleaned Pivots/processed_NAICS.parquet")
OUTPUT_PATH_Monthly = Path("data/Cleaned Pivots/processed_Monthly Vac.parquet")
OUTPUT_PATH_Quarterly = Path("data/Cleaned Pivots/processed_Quarterly Vac.parquet")


def extract():
    """Load raw dataset"""
    df_NAICS = pd.read_csv(RAW_NAICS_PATH, low_memory=False)
    df_Monthly_Vac = pd.read_csv(RAW_MONTHLY_PATH)

    df_list=[]
    files = sorted(QUARTERLY_VAC_PATH.glob("*.csv"))

    # Extracting all the files in that one folder (Contains data on each pronvince, Territories and Canada as a total)
    for file in files:
        df = pd.read_csv(file)
        
        df_list.append(df)
    
    df_Quarterly_Vac = pd.concat(df_list, ignore_index=True)

    return df_NAICS, df_Monthly_Vac, df_Quarterly_Vac


def transform_NAICS(df):
    """Cleaning for NAICS"""

    Kept_columns = [
        'REF_DATE', 'GEO','North American Industry Classification System (NAICS)', 'VALUE'
    ]

    # Standardize type of employment
    df_only_all_employee = df[df['Type of employee'] == "All employees"]

    # Keeping only after 2015
    df_2015_2025 = df_only_all_employee[df_only_all_employee['REF_DATE'] >= "2015-01"]

    df_cleaned_NAICS = df_2015_2025[Kept_columns]

    pivot_NAICS = df_cleaned_NAICS.pivot_table(
    index=['GEO', 'North American Industry Classification System (NAICS)'],
    columns='REF_DATE',
    values='VALUE'
    )

    return pivot_NAICS


def transform_Monthly_Vac(df):
    """Cleaning for Mothly Vacancy Dataset"""

    # Taking only the needed columns
    Kept_columns = [
        'REF_DATE','GEO','Statistics','VALUE'
    ]

    df_cleaned_column = df[Kept_columns]

    pivot_Monthly_Vac = df_cleaned_column.pivot_table(
    index=['GEO', 'Statistics'],
    columns='REF_DATE',
    values='VALUE'
    )

    return pivot_Monthly_Vac


def transform_Quarterly_Vac(df):
    """Cleaning for Quarterly Vacancy Dataset which includes NAICS data"""
    Kept_columns = [
        'REF_DATE','GEO','North American Industry Classification System (NAICS)',
        'Statistics','VALUE'
    ]

    df_cleaned_column = df[Kept_columns]

    pivot_Quarterly_Vac = df_cleaned_column.pivot_table(
    index=['GEO', 'North American Industry Classification System (NAICS)','Statistics'],
    columns='REF_DATE',
    values='VALUE'
    )

    return pivot_Quarterly_Vac



def load(pivot_NAICS, pivot_Monthly_Vac, pivot_Quarterly_Vac):
    """Write analytics-ready dataset"""
    pivot_NAICS.to_parquet(OUTPUT_PATH_NAICS, index=False)
    pivot_Monthly_Vac.to_parquet(OUTPUT_PATH_Monthly, index=False)
    pivot_Quarterly_Vac.to_parquet(OUTPUT_PATH_Quarterly, index=False)


def main():
    df_NAICS, df_Monthly_Vac, df_Quarterly_Vac = extract()
    pivot_NAICS = transform_NAICS(df_NAICS)
    pivot_Monthly_Vac = transform_Monthly_Vac(df_Monthly_Vac)
    pivot_Quarterly_Vac = transform_Quarterly_Vac(df_Quarterly_Vac)
    load(pivot_NAICS, pivot_Monthly_Vac, pivot_Quarterly_Vac)
    print("ETL completed → processed_NAICS/Monthly Vac/Quarterly Vac.parquet")


if __name__ == "__main__":
    main()

