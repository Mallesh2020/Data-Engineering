from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta


def connect_source():
    prev_day = datetime.today() - timedelta(days = 1)
    from_date = prev_day.strftime("%d-%b-%Y")
    #to_date = datetime.today()
    source = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={from_date}&todt={from_date}"
    print(f"Generated source url: {source}")
    
    try:
        response = requests.get(source)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print("Request Error: {e}")


def prepare_data(response):
    if not response:
        print("No response from source")

    raw_text = BeautifulSoup(response.text, 'html.parser')
    records, mf_category, mf_name = [], None, None 

    for line in raw_text.text.splitlines():
        if not line:
            continue
        if ';' not in line:
            if "Schemes" in line:
                mf_category = line
            elif "Mutual Fund" in line:
                mf_name = line
        else:
            records.append(f"{mf_category};{mf_name};{line}" if mf_category and mf_name else line)
        
    if not records:
        print("No records are available to load")
        return None, None
    else:
        header = ['Mutual Fund Category', 'Mutual Fund Name'] + records[0].split(';')
        data = [record.split(';') for record in records[1:]]
        return header, data