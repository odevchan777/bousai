import pandas as pd
import requests
import time

def clean_text(text):
    return str(text).replace('　', '').replace(' ', '').strip()

df = pd.read_csv("master200.csv")
df["住所"] = df["都道府県漢字名"].apply(clean_text) + df["市区郡漢字名"].apply(clean_text) + df["町村字通称漢字名"].apply(clean_text)
df["取得緯度"] = None
df["取得経度"] = None

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
        print(f"[ERROR] {query}: {e}")
    time.sleep(0.5)

df.to_csv("master_with_coords.csv", index=False)
