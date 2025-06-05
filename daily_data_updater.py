#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import io
import requests
import urllib3
import pandas as pd
from datetime import datetime, timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

RAW_DIR = r"C:\05model\raw"
CLEANED_DIR = r"C:\05model\cleaned"
MAX_LOOKBACK = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
}

REFERER = {
    "t86": "https://www.twse.com.tw/exchangeReport/TWT86U",
    "twt44u": "https://www.twse.com.tw/fund/TWT44U",
    "twt38u": "https://www.twse.com.tw/fund/TWT38U",
    "mi_margn": "https://www.twse.com.tw/exchangeReport/MI_MARGN",
    "mi_index": "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
}

URLS = {
    "t86": lambda d, tw: f"https://www.twse.com.tw/rwd/zh/fund/T86?response=csv&date={d}&selectType=ALLBUT0999",
    "twt44u": lambda d, tw: f"https://www.twse.com.tw/fund/TWT44U?response=csv&date={d}&selectType=ALL",
    "twt38u": lambda d, tw: f"https://www.twse.com.tw/fund/TWT38U?response=csv&date={d}&selectType=ALL",
    "mi_margn": lambda d, tw: f"https://www.twse.com.tw/exchangeReport/MI_MARGN?response=csv&date={d}&selectType=ALL",
    "mi_index": lambda d, tw: f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=csv&date={d}&type=ALL"
}

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def is_html_bytes(b: bytes) -> bool:
    text = b.decode("utf-8", errors="ignore").lower()
    return any(tag in text[:500] for tag in ("<html", "<!doctype", "<head", "<script"))

def download_one(session, name, url_func):
    today = datetime.today()
    for i in range(MAX_LOOKBACK):
        t = today - timedelta(days=i)
        d = t.strftime("%Y%m%d")
        tw = f"{t.year-1911}/{t.month:02}/{t.day:02}"
        r = session.get(
            url_func(d, tw),
            headers={**HEADERS, "Referer": REFERER[name]},
            verify=False,
            timeout=10
        )
        if r.status_code == 200 and len(r.content) > 500:
            if name == "t86" or not is_html_bytes(r.content):
                ensure_dir(RAW_DIR)
                fn = os.path.join(RAW_DIR, f"{d}_{name}.csv")
                with open(fn, "wb") as f:
                    f.write(r.content)
                print(f"[✅] {name} raw → {fn}")
                return True
    print(f"[❌] {name} raw 無法下載 (超過 {MAX_LOOKBACK} 天)")
    return False

def download_all():
    sess = requests.Session()
    for name, func in URLS.items():
        download_one(sess, name, func)

def clean_numeric(val):
    s = str(val).replace(",", "").strip()
    if s in ("", "-", "NA") or all(ch == "#" for ch in s):
        return 0.0
    try:
        return float(s)
    except:
        return 0.0

def read_csv_auto(path, **kwargs):
    for enc in ("cp950", "utf-8"):
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except:
            pass
    return pd.read_csv(path, encoding="cp950", errors="ignore", **kwargs)

def latest_raw(prefix):
    fs = [f for f in os.listdir(RAW_DIR) if f.lower().endswith(f"_{prefix}.csv")]
    if not fs:
        raise FileNotFoundError(prefix)
    fs.sort(key=lambda x: int(re.search(r"(\d{8})", x).group(1)), reverse=True)
    return os.path.join(RAW_DIR, fs[0])

def process_t86():
    ensure_dir(CLEANED_DIR)
    p = latest_raw("t86")
    df = read_csv_auto(p, skiprows=1, dtype=str)
    df.columns = df.columns.str.strip()
    df = df.rename(columns={
        "證券代號": "stock_id",
        "外陸資買賣超股數(不含外資自營商)": "foreign_buy",
        "三大法人買賣超股數": "insti_net"
    })[["stock_id", "foreign_buy", "insti_net"]]
    df = df[df["stock_id"].str.match(r"^\d{4}$", na=False)]
    df["foreign_buy"] = df["foreign_buy"].apply(clean_numeric)
    df["insti_net"] = df["insti_net"].apply(clean_numeric)
    out = os.path.join(CLEANED_DIR, "cleaned_t86.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[✅] t86 cleaned → {out}")

def process_twt44u():
    ensure_dir(CLEANED_DIR)
    p = latest_raw("twt44u")
    df = read_csv_auto(p, skiprows=1, dtype=str)
    df.columns = df.columns.str.strip()
    df.iloc[:, 1] = df.iloc[:, 1].str.replace("=", "").str.strip()
    df = df.iloc[:, [1, 3, 4, 5]].copy()
    df.columns = ["stock_id", "trust_buy", "trust_sell", "trust_net"]
    df = df[df["stock_id"].str.match(r"^\d{4}$", na=False)]
    df["trust_buy"] = df["trust_buy"].apply(clean_numeric)
    df["trust_sell"] = df["trust_sell"].apply(clean_numeric)
    df["trust_net"] = df["trust_net"].apply(clean_numeric)
    out = os.path.join(CLEANED_DIR, "cleaned_twt44u.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[✅] twt44u cleaned → {out}")

def process_twt38u():
    ensure_dir(CLEANED_DIR)
    p = latest_raw("twt38u")
    df = read_csv_auto(p, skiprows=2, dtype=str)
    df.columns = df.columns.str.strip()
    df.iloc[:, 1] = df.iloc[:, 1].str.replace("=", "").str.strip()
    result_df = pd.DataFrame()
    result_df["stock_id"] = df.iloc[:, 1]
    result_df["FI_Buy"] = df.iloc[:, 3].apply(clean_numeric)
    result_df["FI_Sell"] = df.iloc[:, 4].apply(clean_numeric)
    result_df["FI_Net"] = df.iloc[:, 5].apply(clean_numeric)
    result_df["PD_Buy"] = 0
    result_df["PD_Sell"] = 0
    result_df["PD_Net"] = 0
    result_df["FA_Buy"] = df.iloc[:, 9].apply(clean_numeric)
    result_df["FA_Sell"] = df.iloc[:, 10].apply(clean_numeric)
    result_df["FA_Net"] = df.iloc[:, 11].apply(clean_numeric)
    result_df = result_df[result_df["stock_id"].str.match(r"^\d{4}$", na=False)]
    out = os.path.join(CLEANED_DIR, "cleaned_twt38u.csv")
    result_df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[✅] twt38u cleaned → {out}")

def process_margen():
    ensure_dir(CLEANED_DIR)
    p = latest_raw("mi_margn")
    df = read_csv_auto(p, skiprows=7, dtype=str)
    df.columns = df.columns.str.strip()
    df["stock_id"] = df.iloc[:, 0].str.strip()
    df = df[df["stock_id"].str.match(r"^\d{4}$", na=False)]
    df["margin_diff"] = df.iloc[:, 6].apply(clean_numeric) - df.iloc[:, 5].apply(clean_numeric)
    df["short_diff"] = df.iloc[:, 12].apply(clean_numeric) - df.iloc[:, 11].apply(clean_numeric)
    out = os.path.join(CLEANED_DIR, "cleaned_margen.csv")
    df[["stock_id", "margin_diff", "short_diff"]].to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[✅] mi_margn cleaned → {out}")

def process_mi_index():
    ensure_dir(CLEANED_DIR)
    p = latest_raw("mi_index")
    header_row = None
    with open(p, "r", encoding="cp950", errors="ignore") as f:
        for idx, line in enumerate(f):
            if "證券代號" in line and "收盤價" in line:
                header_row = idx
                break
    if header_row is None:
        raise RuntimeError("找不到 MI_INDEX 標題")
    print(f"[ℹ] mi_index header at line {header_row+1}")
    df = read_csv_auto(p, skiprows=header_row, dtype=str)
    df.columns = df.columns.str.strip()

    # 移除 Unnamed 欄位
    df = df.drop(columns=[c for c in df.columns if c.startswith("Unnamed")], errors="ignore")

    # 只保留 4 位數股票代號
    df = df[df["證券代號"].str.match(r"^\d{4}$", na=False)]

    # 重新命名欄位（包含 '證券名稱' → 'name'）
    df = df.rename(columns={
        "證券代號": "stock_id",
        "證券名稱": "name",
        "成交股數": "volume",
        "成交金額": "value",
        "成交筆數": "transactions",
        "開盤價": "open",
        "最高價": "high",
        "最低價": "low",
        "收盤價": "close",
        "漲跌價差": "change",
        "最後揭示買價": "last_bid_price",
        "最後揭示買量": "last_bid_volume",
        "最後揭示賣價": "last_ask_price",
        "最後揭示賣量": "last_ask_volume",
        "本益比": "per"
    })

    # 針對數值欄位做 clean_numeric；保留 'stock_id' 和 'name' 不轉為數字
    for col in df.columns:
        if col not in ["stock_id", "name"]:
            df[col] = df[col].apply(clean_numeric)

    out = os.path.join(CLEANED_DIR, "cleaned_mi_index.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[✅] mi_index cleaned → {out}")

if __name__ == "__main__":
    print("── Downloading raw data ──")
    download_all()
    print("── Cleaning each source ──")
    process_t86()
    process_twt44u()
    process_twt38u()
    process_margen()
    process_mi_index()
