import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# Read all three CSV files
df_cnbc = pd.read_csv('data_news/cnbc_headlines.csv')
df_guardian = pd.read_csv('data_news/guardian_headlines.csv')
df_reuters = pd.read_csv('data_news/reuters_headlines.csv')

# Function to process CNBC dataframe
def process_cnbc(df):
    df = df.copy()
    df.dropna(subset=['Time', 'Headlines'], inplace=True)
    df['date'] = pd.to_datetime(df['Time'], errors='coerce')
    # Drop rows with invalid dates
    df = df.dropna(subset=['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['source'] = 'CNBC'
    return df[['year', 'month', 'day', 'Headlines', 'source']]

# Function to process Guardian dataframe
def process_guardian(df):
    df = df.copy()
    df.dropna(subset=['Time', 'Headlines'], inplace=True)
    # Fix the 2-digit year issue by adding '20' prefix
    df['date'] = pd.to_datetime(df['Time'], format='%d-%b-%y', errors='coerce')
    # Drop rows with invalid dates
    df = df.dropna(subset=['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['source'] = 'Guardian'
    return df[['year', 'month', 'day', 'Headlines', 'source']]

# Function to process Reuters dataframe
def process_reuters(df):
    df = df.copy()
    df.dropna(subset=['Time', 'Headlines'], inplace=True)
    df['date'] = pd.to_datetime(df['Time'], errors='coerce')
    # Drop rows with invalid dates
    df = df.dropna(subset=['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['source'] = 'Reuters'
    return df[['year', 'month', 'day', 'Headlines', 'source']]

# Process each dataframe with its specific function
df_cnbc = process_cnbc(df_cnbc)
df_guardian = process_guardian(df_guardian)
df_reuters = process_reuters(df_reuters)

# Merge all dataframes
df_combined = pd.concat([df_cnbc, df_guardian, df_reuters], ignore_index=True)

# Convert numeric columns to int for consistency
df_combined['year'] = df_combined['year'].astype(int)
df_combined['month'] = df_combined['month'].astype(int)
df_combined['day'] = df_combined['day'].astype(int)

# Sort by date
df_combined = df_combined.sort_values(by=['year', 'month', 'day'])

print("Combined dataset head:")
print(df_combined.head())

print("\nDataset shape:", df_combined.shape)
print("\nSample counts by source:")
print(df_combined['source'].value_counts())

# Save the combined dataset
df_combined.to_csv('combined_headlines.csv', index=False)
