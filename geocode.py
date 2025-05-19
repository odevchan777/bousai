import pandas as pd
import sys
import requests
import time
from datetime import datetime

infile = sys.argv[1]
outfile = sys.argv[2]

def clean_text(text):
    return str(text).replace('　', '').replace(' ', '').strip()

def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# CSV読み込み（UTF-8）
df = pd.read_csv(infile, encoding="utf-8")

# 住所整形
df["住所"] = df["都道府県漢字名"].apply(clean_text) + df["市区郡漢字名"].apply(clean_text) + df["町村字通称漢字名"].apply(clean_text)

# 緯度経度カラム初期化
df["取得緯度"] = None
df["取得経度"] = None

total = len(df)
print(f"[{now()}] Start geocoding: {total} rows")

for idx, row in df.iterrows():
    query = row["住所"]
    url = f"https://msearch.gsi.go.jp/address-search/AddressSearch?q={query}"
    try:
        res = requests.get(url, timeout=5)
        data = res.json()
        if data:
            lon, lat = data[0]["geometry"]["coordinates"]
            df.at[idx, "取得緯度"] = lat
            df.at[idx, "取得経度"] = lon
    except Exception as e:
        print(f"[{now()}] [ERROR] {query}: {e}")

    # ⏳ 進行状況（100件ごと＋最終）を時刻付きで表示
    if (idx + 1) % 100 == 0 or (idx + 1) == total:
        print(f"[{now()}] [INFO] Processed {idx + 1} / {total} rows")

    time.sleep(0.5)

# CSV出力（UTF-8）
df.to_csv(outfile, index=False, encoding="utf-8")
print(f"[{now()}] [INFO] Saved result to {outfile}")
