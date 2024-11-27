import pandas as pd
from decimal import Decimal


def dataframe(header, data):
    if not header or not data:
        print("No Data.... Returning and empty DataFrame")
        return pd.DataFrame()
    
    try:
        df = pd.DataFrame(data, columns=header).replace('', pd.NA)

        stringColumns = ['Mutual Fund Category', 'Mutual Fund Name', 'Scheme Name', 'ISIN Div Payout/ISIN Growth', 'ISIN Div Reinvestment']
        decimalColumns = ['Net Asset Value', 'Repurchase Price', 'Sale Price']

        df['Scheme Code'] = pd.to_numeric(df['Scheme Code'], errors='coerce')
        df[stringColumns] = df[stringColumns].fillna('NA')
        pd.set_option('future.no_silent_downcasting', True)
        df[decimalColumns] = df[decimalColumns].fillna(-1).astype('float')
        df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%Y', errors='coerce').fillna(pd.to_datetime('01-Jan-1970')).dt.strftime('%Y-%m-%d')

        return df
    
    except Exception as e:
        print(f'There is an error in transformation --> {e}')
        return pd.DataFrame()