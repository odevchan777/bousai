import pandas as pd
import sys
import requests
import time
from datetime import datetime

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

infile = sys.argv[1]
outfile = sys.argv[2]

def clean_text(text):
    return str(text).replace('　', '').replace(' ', '').strip()

def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# セッションとリトライ構成
session = requests.Session()
retries = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)

df = pd.read_csv(infile, encoding="utf-8")
df["住所"] = df["都道府県漢字名"].apply(clean_text) + df["市区郡漢字名"].apply(clean_text) + df["町村字通称漢字名"].apply(clean_text)
df["取得緯度"] = None
df["取得経度"] = None

total = len(df)
print(f"[{now()}] Start geocoding: {total} rows", flush=True)

for idx, row in df.iterrows():
    query = row["住所"]
    url = f"https://msearch.gsi.go.jp/address-search/AddressSearch?q={query}"
    try:
        res = session.get(url, timeout=10)  # timeout長め
        data = res.json()
        if data:
            lon, lat = data[0]["geometry"]["coordinates"]
            df.at[idx, "取得緯度"] = lat
            df.at[idx, "取得経度"] = lon
    except Exception as e:
        print(f"[{now()}] [ERROR] {query}: {e}", flush=True)

    if (idx + 1) % 100 == 0 or (idx + 1) == total:
        print(f"[{now()}] [INFO] Processed {idx + 1} / {total} rows", flush=True)

    time.sleep(1.0)  # ←ここ重要、アクセス間隔をあける

df.to_csv(outfile, index=False, encoding="utf-8")
print(f"[{now()}] [INFO] Saved result to {outfile}", flush=True)
