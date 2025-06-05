#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Historical Batch Downloader - 台股歷史資料批量下載器
基於現有 daily_data_updater.py 修改，專門用於歷史資料補強
日期範圍：2025/01/01 到今天
"""

import os
import re
import requests
import urllib3
import pandas as pd
import time
import random
from datetime import datetime, timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===== 設定區域 =====
RAW_DIR = r"C:\05model\raw"
CLEANED_DIR = r"C:\05model\cleaned"

# 日期範圍設定
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime.today()

# 下載設定
MIN_DELAY = 3.0      # 最小間隔秒數
MAX_DELAY = 6.0      # 最大間隔秒數
MAX_RETRIES = 3      # 最大重試次數
RETRY_DELAY = 10     # 重試間隔秒數

# HTTP 設定
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

# ===== 工具函數 =====
def ensure_dir(path):
    """確保目錄存在"""
    if not os.path.exists(path):
        os.makedirs(path)

def is_html_bytes(b: bytes) -> bool:
    """檢查回應是否為HTML（表示無資料）"""
    text = b.decode("utf-8", errors="ignore").lower()
    return any(tag in text[:500] for tag in ("<html", "<!doctype", "<head", "<script"))

def smart_delay():
    """智能延遲，避免被偵測"""
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    print(f"[⏳] 等待 {delay:.1f} 秒...")
    time.sleep(delay)

def get_existing_dates():
    """取得已存在的日期，避免重複下載"""
    if not os.path.exists(RAW_DIR):
        return set()
    
    existing_dates = set()
    for filename in os.listdir(RAW_DIR):
        if filename.endswith('.csv'):
            match = re.search(r'(\d{8})_', filename)
            if match:
                existing_dates.add(match.group(1))
    
    return existing_dates

def generate_trading_dates():
    """生成交易日期列表（跳過週末）"""
    dates = []
    current_date = START_DATE
    
    while current_date <= END_DATE:
        # 跳過週末
        if current_date.weekday() < 5:  # 0-4 是週一到週五
            dates.append(current_date)
        current_date += timedelta(days=1)
    
    return dates

# ===== 下載功能 =====
def download_one_date(session, name, url_func, date_obj):
    """下載單一日期的單一資料源"""
    d = date_obj.strftime("%Y%m%d")
    tw = f"{date_obj.year-1911}/{date_obj.month:02}/{date_obj.day:02}"
    
    # 檢查檔案是否已存在
    fn = os.path.join(RAW_DIR, f"{d}_{name}.csv")
    if os.path.exists(fn):
        print(f"[⏭] {name} {d} 已存在，跳過")
        return True
    
    # 嘗試下載
    for retry in range(MAX_RETRIES):
        try:
            print(f"[🔄] 下載 {name} {d} (嘗試 {retry+1}/{MAX_RETRIES})")
            
            r = session.get(
                url_func(d, tw),
                headers={**HEADERS, "Referer": REFERER[name]},
                verify=False,
                timeout=15
            )
            
            if r.status_code == 200 and len(r.content) > 500:
                # t86 特殊處理，其他檢查是否為HTML
                if name == "t86" or not is_html_bytes(r.content):
                    ensure_dir(RAW_DIR)
                    with open(fn, "wb") as f:
                        f.write(r.content)
                    print(f"[✅] {name} {d} → {fn}")
                    return True
                else:
                    print(f"[⚠] {name} {d} 回傳 HTML (可能無資料)")
                    return False
            else:
                print(f"[⚠] {name} {d} 狀態碼: {r.status_code}, 大小: {len(r.content)}")
                
        except Exception as e:
            print(f"[❌] {name} {d} 錯誤: {e}")
            if retry < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** retry)  # 指數退避
                print(f"[⏳] 等待 {wait_time} 秒後重試...")
                time.sleep(wait_time)
    
    print(f"[❌] {name} {d} 下載失敗")
    return False

def download_all_historical():
    """下載所有歷史資料"""
    print("=== 歷史資料批量下載開始 ===")
    
    # 生成日期列表
    dates = generate_trading_dates()
    existing_dates = get_existing_dates()
    
    total_dates = len(dates)
    total_requests = total_dates * len(URLS)
    
    print(f"[ℹ] 日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"[ℹ] 總交易日: {total_dates} 天")
    print(f"[ℹ] 已存在資料: {len(existing_dates)} 個日期")
    print(f"[ℹ] 總請求數: {total_requests}")
    print(f"[ℹ] 預估時間: {total_requests * 4.5 / 60:.1f} 分鐘")
    
    # 統計變數
    success_count = 0
    fail_count = 0
    skip_count = 0
    
    sess = requests.Session()
    
    # 開始下載
    for i, date_obj in enumerate(dates, 1):
        d = date_obj.strftime("%Y%m%d")
        print(f"\n── 處理日期 {date_obj.strftime('%Y-%m-%d')} ({i}/{total_dates}) ──")
        
        # 檢查這個日期是否已有完整資料
        date_files_exist = all(
            os.path.exists(os.path.join(RAW_DIR, f"{d}_{name}.csv"))
            for name in URLS.keys()
        )
        
        if date_files_exist:
            print(f"[⏭] {d} 所有檔案已存在，跳過整日")
            skip_count += len(URLS)
            continue
        
        # 下載各個資料源
        for j, (name, url_func) in enumerate(URLS.items()):
            if download_one_date(sess, name, url_func, date_obj):
                success_count += 1
            else:
                fail_count += 1
            
            # 除了最後一個請求，都要延遲
            if not (i == total_dates and j == len(URLS) - 1):
                smart_delay()
    
    print(f"\n[📊] 下載統計:")
    print(f"    - 成功: {success_count}")
    print(f"    - 失敗: {fail_count}")
    print(f"    - 跳過: {skip_count}")
    print(f"    - 總計: {success_count + fail_count + skip_count}")

# ===== 清洗功能（保持原有邏輯） =====
def clean_numeric(val):
    """清洗數值資料"""
    s = str(val).replace(",", "").strip()
    if s in ("", "-", "NA") or all(ch == "#" for ch in s):
        return 0.0
    try:
        return float(s)
    except:
        return 0.0

def read_csv_auto(path, **kwargs):
    """自動偵測編碼讀取CSV"""
    for enc in ("cp950", "utf-8"):
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except:
            pass
    return pd.read_csv(path, encoding="cp950", errors="ignore", **kwargs)

def get_raw_files_by_date(date_str):
    """取得指定日期的所有原始檔案"""
    if not os.path.exists(RAW_DIR):
        return {}
    
    files = {}
    for name in URLS.keys():
        filepath = os.path.join(RAW_DIR, f"{date_str}_{name}.csv")
        if os.path.exists(filepath):
            files[name] = filepath
    
    return files

def process_date_t86(date_str, filepath):
    """處理指定日期的T86資料"""
    try:
        df = read_csv_auto(filepath, skiprows=1, dtype=str)
        df.columns = df.columns.str.strip()
        df = df.rename(columns={
            "證券代號": "stock_id",
            "外陸資買賣超股數(不含外資自營商)": "foreign_buy",
            "三大法人買賣超股數": "insti_net"
        })[["stock_id", "foreign_buy", "insti_net"]]
        df = df[df["stock_id"].str.match(r"^\d{4}$", na=False)]
        df["foreign_buy"] = df["foreign_buy"].apply(clean_numeric)
        df["insti_net"] = df["insti_net"].apply(clean_numeric)
        
        ensure_dir(CLEANED_DIR)
        out = os.path.join(CLEANED_DIR, f"{date_str}_cleaned_t86.csv")
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"[✅] t86 {date_str} cleaned → {out}")
        return True
    except Exception as e:
        print(f"[❌] t86 {date_str} 清洗失敗: {e}")
        return False

def process_date_twt44u(date_str, filepath):
    """處理指定日期的TWT44U資料"""
    try:
        df = read_csv_auto(filepath, skiprows=1, dtype=str)
        df.columns = df.columns.str.strip()
        df.iloc[:, 1] = df.iloc[:, 1].str.replace("=", "").str.strip()
        df = df.iloc[:, [1, 3, 4, 5]].copy()
        df.columns = ["stock_id", "trust_buy", "trust_sell", "trust_net"]
        df = df[df["stock_id"].str.match(r"^\d{4}$", na=False)]
        df["trust_buy"] = df["trust_buy"].apply(clean_numeric)
        df["trust_sell"] = df["trust_sell"].apply(clean_numeric)
        df["trust_net"] = df["trust_net"].apply(clean_numeric)
        
        ensure_dir(CLEANED_DIR)
        out = os.path.join(CLEANED_DIR, f"{date_str}_cleaned_twt44u.csv")
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"[✅] twt44u {date_str} cleaned → {out}")
        return True
    except Exception as e:
        print(f"[❌] twt44u {date_str} 清洗失敗: {e}")
        return False

def process_date_twt38u(date_str, filepath):
    """處理指定日期的TWT38U資料"""
    try:
        df = read_csv_auto(filepath, skiprows=2, dtype=str)
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
        
        ensure_dir(CLEANED_DIR)
        out = os.path.join(CLEANED_DIR, f"{date_str}_cleaned_twt38u.csv")
        result_df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"[✅] twt38u {date_str} cleaned → {out}")
        return True
    except Exception as e:
        print(f"[❌] twt38u {date_str} 清洗失敗: {e}")
        return False

def process_date_margen(date_str, filepath):
    """處理指定日期的MI_MARGN資料"""
    try:
        df = read_csv_auto(filepath, skiprows=7, dtype=str)
        df.columns = df.columns.str.strip()
        df["stock_id"] = df.iloc[:, 0].str.strip()
        df = df[df["stock_id"].str.match(r"^\d{4}$", na=False)]
        df["margin_diff"] = df.iloc[:, 6].apply(clean_numeric) - df.iloc[:, 5].apply(clean_numeric)
        df["short_diff"] = df.iloc[:, 12].apply(clean_numeric) - df.iloc[:, 11].apply(clean_numeric)
        
        ensure_dir(CLEANED_DIR)
        out = os.path.join(CLEANED_DIR, f"{date_str}_cleaned_margen.csv")
        df[["stock_id", "margin_diff", "short_diff"]].to_csv(out, index=False, encoding="utf-8-sig")
        print(f"[✅] mi_margn {date_str} cleaned → {out}")
        return True
    except Exception as e:
        print(f"[❌] mi_margn {date_str} 清洗失敗: {e}")
        return False

def process_date_mi_index(date_str, filepath):
    """處理指定日期的MI_INDEX資料"""
    try:
        # 找到標題列
        header_row = None
        with open(filepath, "r", encoding="cp950", errors="ignore") as f:
            for idx, line in enumerate(f):
                if "證券代號" in line and "收盤價" in line:
                    header_row = idx
                    break
        
        if header_row is None:
            print(f"[⚠] {filepath} 找不到標題列")
            return False
        
        df = read_csv_auto(filepath, skiprows=header_row, dtype=str)
        df.columns = df.columns.str.strip()
        
        # 移除 Unnamed 欄位
        df = df.drop(columns=[c for c in df.columns if c.startswith("Unnamed")], errors="ignore")
        
        # 只保留 4 位數股票代號
        df = df[df["證券代號"].str.match(r"^\d{4}$", na=False)]
        
        # 重新命名欄位
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
        
        # 針對數值欄位做 clean_numeric
        for col in df.columns:
            if col not in ["stock_id", "name"]:
                df[col] = df[col].apply(clean_numeric)
        
        ensure_dir(CLEANED_DIR)
        out = os.path.join(CLEANED_DIR, f"{date_str}_cleaned_mi_index.csv")
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"[✅] mi_index {date_str} cleaned → {out}")
        return True
    except Exception as e:
        print(f"[❌] mi_index {date_str} 清洗失敗: {e}")
        return False

def clean_all_downloaded():
    """清洗所有已下載的原始資料"""
    print("\n=== 開始清洗所有資料 ===")
    
    if not os.path.exists(RAW_DIR):
        print("[⚠] Raw 資料夾不存在")
        return
    
    # 取得所有日期
    all_dates = set()
    for filename in os.listdir(RAW_DIR):
        if filename.endswith('.csv'):
            match = re.search(r'(\d{8})_', filename)
            if match:
                all_dates.add(match.group(1))
    
    all_dates = sorted(all_dates)
    print(f"[ℹ] 找到 {len(all_dates)} 個日期的資料")
    
    # 處理器映射
    processors = {
        't86': process_date_t86,
        'twt44u': process_date_twt44u,
        'twt38u': process_date_twt38u,
        'mi_margn': process_date_margen,
        'mi_index': process_date_mi_index
    }
    
    total_success = 0
    total_fail = 0
    
    # 逐日處理
    for i, date_str in enumerate(all_dates, 1):
        print(f"\n── 清洗日期 {date_str} ({i}/{len(all_dates)}) ──")
        
        # 取得這個日期的所有檔案
        date_files = get_raw_files_by_date(date_str)
        
        if not date_files:
            print(f"[⚠] {date_str} 沒有找到任何原始檔案")
            continue
        
        # 處理各個資料源
        for name, filepath in date_files.items():
            if name in processors:
                if processors[name](date_str, filepath):
                    total_success += 1
                else:
                    total_fail += 1
            else:
                print(f"[⚠] 不認識的資料源: {name}")
    
    print(f"\n[📊] 清洗統計:")
    print(f"    - 成功: {total_success}")
    print(f"    - 失敗: {total_fail}")

# ===== 主程式 =====
def main():
    """主執行函數"""
    print("=== 台股歷史資料批量下載器 ===")
    print(f"目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"資料類型: 上市股票")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEANED_DIR} (清洗)")
    
    # 確認執行
    response = input("\n是否開始執行? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    
    try:
        # 步驟 1: 下載所有歷史資料
        download_all_historical()
        
        # 步驟 2: 清洗所有已下載的資料
        clean_all_downloaded()
        
        # 完成
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n[🎉] 所有程序完成!")
        print(f"[⏱] 總執行時間: {duration}")
        print(f"[📁] 原始資料: {RAW_DIR}")
        print(f"[📁] 清洗資料: {CLEANED_DIR}")
        
    except KeyboardInterrupt:
        print("\n[⏹] 使用者中斷執行")
    except Exception as e:
        print(f"\n[❌] 執行過程發生錯誤: {e}")
        raise

if __name__ == "__main__":
    main()
