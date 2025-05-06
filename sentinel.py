import pandas as pd
import matplotlib.pyplot as plt
from db import get_db_engine

def fetch_daily_load(client_name: str, date: str) -> pd.DataFrame:
    """Fetch daily load profile for a specific client and date."""
    engine = get_db_engine()
    query = f"""
    SELECT datetime, kw_t, meterid
    FROM v3.five_minutes
    WHERE meterid = '{client_name}'
      AND datetime >= '{date} 00:00:00'
      AND datetime <= '{date} 23:59:59'
    ORDER BY datetime
    """
    df = pd.read_sql(query, engine)
    return df

def calculate_thresholds(df: pd.DataFrame, window_minutes: int = 60) -> pd.DataFrame:
    """Calculate rolling mean and threshold bounds."""
    if len(df) < 2:
        raise ValueError("Not enough data points to calculate interval.")
    
    interval_minutes = (df['datetime'].iloc[1] - df['datetime'].iloc[0]).seconds / 60
    window_size = max(1, int(window_minutes / interval_minutes))

    df['rolling_mean'] = df['kw_t'].rolling(window=window_size, min_periods=1).mean()
    df['rolling_std'] = df['kw_t'].rolling(window=window_size, min_periods=1).std()
    df['upper_threshold'] = df['rolling_mean'] + 2 * df['rolling_std']
    df['lower_threshold'] = df['rolling_mean'] - 2 * df['rolling_std']
    return df

def detect_deviations(df: pd.DataFrame) -> pd.DataFrame:
    """Detect when actual load is outside the thresholds."""
    df['deviation_flag'] = 'Normal'
    df.loc[df['kw_t'] > df['upper_threshold'], 'deviation_flag'] = 'High Deviation'
    df.loc[df['kw_t'] < df['lower_threshold'], 'deviation_flag'] = 'Low Deviation'
    return df

def plot_actual_vs_expected(df: pd.DataFrame, client_name: str, date: str):
    """Plot actual load vs expected (rolling mean as expected)."""
    plt.figure(figsize=(14, 6))
    
    plt.plot(df['datetime'], df['kw_t'], label='Actual Load (kW)', color='black', linewidth=1.5)
    plt.plot(df['datetime'], df['rolling_mean'], label='Expected Load (kW)', color='blue', linestyle='--')

    plt.title(f"Actual vs Expected Load\n{client_name} on {date}", fontsize=14, fontweight='bold')
    plt.xlabel("Datetime", fontsize=12)
    plt.ylabel("Load (kW)", fontsize=12)
    plt.xticks(fontsize=10)
    plt.yticks(fontsize=10)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.show()


def process_client_daily_load(client_name: str, date: str) -> pd.DataFrame:
    """Main function to process one client's daily load."""
    df = fetch_daily_load(client_name, date)
    if df.empty:
        raise ValueError(f"No data found for {client_name} on {date}.")
    
    # Make sure datetime column is parsed properly
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    df = calculate_thresholds(df)
    df = detect_deviations(df)
    return df

if __name__ == "__main__":
    # ✅ Set your dynamic input here
    client_name = "lolooboys-tenantmeter" 
    date = "2025-05-01"

    try:
        df_result = process_client_daily_load(client_name, date)
        print(df_result[['datetime', 'kw_t', 'rolling_mean', 'upper_threshold', 'lower_threshold', 'deviation_flag']])

        # ✅ PLOT
        plot_actual_vs_expected(df_result, client_name, date)

    except Exception as e:
        print(f"Error occurred: {e}")
