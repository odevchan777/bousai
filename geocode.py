import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------
# 文字整形：全角スペースや空白を削除
# -----------------------
def clean_text(text):
    return str(text).replace('　', '').replace(' ', '').strip()

# -----------------------
# CSV読み込み（文字化け対策に utf-8-sig）
# -----------------------
df = pd.read_csv("master001.csv", encoding="utf-8-sig")

# 住所列作成（都道府県 + 市区郡 + 町村字）
df["住所"] = df["都道府県漢字名"].apply(clean_text) + df["市区郡漢字名"].apply(clean_text) + df["町村字通称漢字名"].apply(clean_text)

# 緯度経度カラムの初期化
df["取得緯度"] = None
df["取得経度"] = None

# -----------------------
# API呼び出し関数（タイムアウト＆リトライ付き）
# -----------------------
def fetch_latlon(index, query, retries=3):
    url = f"https://msearch.gsi.go.jp/address-search/AddressSearch?q={query}"
    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=15)
            data = res.json()
            if data:
                lon, lat = data[0]["geometry"]["coordinates"]
                return index, lat, lon
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)  # リトライ待機
            else:
                print(f"[FAILED] {query}: {e}")
    return index, None, None

# -----------------------
# 並列実行（15スレッド）
# -----------------------
with ThreadPoolExecutor(max_workers=15) as executor:
    futures = [
        executor.submit(fetch_latlon, idx, row["住所"])
        for idx, row in df.iterrows()
    ]
    for future in as_completed(futures):
        idx, lat, lon = future.result()
        df.at[idx, "取得緯度"] = lat
        df.at[idx, "取得経度"] = lon

# -----------------------
# 結果を書き出し（文字化け対策に utf-8-sig）
# -----------------------
df.to_csv("master_with_coords.csv", index=False, encoding="utf-8-sig")
