import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

def clean_text(text):
    return str(text).replace('　', '').replace(' ', '').strip()

df = pd.read_csv("master200.csv", encoding="utf-8-sig")
df["住所"] = df["都道府県漢字名"].apply(clean_text) + df["市区郡漢字名"].apply(clean_text) + df["町村字通称漢字名"].apply(clean_text)
df["取得緯度"] = None
df["取得経度"] = None

def fetch_latlon(index, query):
    url = f"https://msearch.gsi.go.jp/address-search/AddressSearch?q={query}"
    try:
        res = requests.get(url, timeout=5)
        data = res.json()
        if data:
            lon, lat = data[0]["geometry"]["coordinates"]
            return index, lat, lon
    except Exception as e:
        print(f"[ERROR] {query}: {e}")
    return index, None, None

with ThreadPoolExecutor(max_workers=20) as executor:
    futures = [
        executor.submit(fetch_latlon, idx, row["住所"])
        for idx, row in df.iterrows()
    ]
    for future in as_completed(futures):
        idx, lat, lon = future.result()
        df.at[idx, "取得緯度"] = lat
        df.at[idx, "取得経度"] = lon

df.to_csv("master_with_coords.csv", index=False, encoding="utf-8-sig")
