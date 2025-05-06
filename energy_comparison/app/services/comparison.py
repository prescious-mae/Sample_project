import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text

def fetch_daily_kwh(db: Session, client_name: str, year_month: str) -> pd.DataFrame:
    start_date = pd.to_datetime(f"{year_month}-01")
    end_date = (start_date + pd.offsets.MonthEnd(0)).date()

    query = text("""
        SELECT 
            DATE_TRUNC('day', datetime) AS day,
            SUM(kwh) AS total_kwh
        FROM hourly_data
        WHERE client_name = :client_name
          AND datetime >= :start_date
          AND datetime < :end_date
        GROUP BY day
        ORDER BY day
    """)
    result = db.execute(query, {
        "client_name": client_name,
        "start_date": start_date,
        "end_date": end_date
    })

    df = pd.DataFrame(result.fetchall(), columns=["day", f"kwh_{year_month}"])
    df["day"] = df["day"].dt.day
    return df

def compare_monthly_consumption(db: Session, client_name: str, month1: str, month2: str):
    df1 = fetch_daily_kwh(db, client_name, month1)
    df2 = fetch_daily_kwh(db, client_name, month2)

    df = pd.merge(df1, df2, on="day", how="outer").fillna(0)
    df["difference"] = df[f"kwh_{month1}"] - df[f"kwh_{month2}"]

    total1 = df[f"kwh_{month1}"].sum()
    total2 = df[f"kwh_{month2}"].sum()

    return {
        "summary": {
            month1: round(total1, 2),
            month2: round(total2, 2),
            "difference": round(total1 - total2, 2)
        },
        "daily_comparison": df.to_dict(orient="records")
    }
