#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OTC Historical Batch Downloader - 上櫃歷史資料批量下載器
基於現有 otc_downloader_optimized.py 修改，專門用於歷史資料補強
日期範圍：2025/01/01 到今天
"""

import os
import json
import urllib3
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
import time
import shutil
import holidays
import re
import logging
import psutil
from pathlib import Path
from typing import Dict, Any, Optional, List
from contextlib import contextmanager
import traceback
import random

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===== 設定區域 =====
BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "otc_raw"
DOWNLOAD_DIR = Path.home() / "Downloads"
CLEAN_DIR = BASE_DIR / "otc_cleaned"
LOG_DIR = BASE_DIR / "logs"

# 日期範圍設定
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime.today()

# 下載設定
MIN_DELAY = 10.0     # 最小間隔秒數（比上市更保守）
MAX_DELAY = 20.0     # 最大間隔秒數
MAX_RETRIES = 2      # 最大重試次數
RETRY_DELAY = 30     # 重試間隔秒數

# 預設設定 - 歷史批量下載優化
DEFAULT_CONFIG = {
    "download_items": {
        "daily_close_no1430": {
            "name": "上櫃股票每日收盤行情(不含定價)",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/mi-pricing.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": True,
            "retry_count": 2,
            "skiprows": 3,
            "priority": 1  # 最重要的資料
        },
        "margin_transactions": {
            "name": "上櫃股票融資融券餘額",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/transactions.html",
            "wait_element": "table.table-default",
            "download_text": "下載 CSV 檔(UTF-8)",
            "needs_query": False,
            "retry_count": 2,
            "skiprows": 3,
            "priority": 2
        },
        "institutional_detail": {
            "name": "三大法人買賣明細資訊",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/detail/day.html",
            "select_element": {"name": "sect", "value": "AL"},
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 2,
            "skiprows": 1,
            "priority": 2
        },
        "day_trading": {
            "name": "現股當沖交易統計資訊",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/day-trading/statistics/day.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 2,
            "skiprows": 6,
            "priority": 3
        },
        "sec_trading": {
            "name": "各券商當日營業金額統計表",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/sec-trading.html",
            "wait_element": "table.table-default",
            "download_text": "下載 CSV",
            "needs_query": False,
            "retry_count": 2,
            "skiprows": 2,
            "priority": 4
        },
        "investment_trust_buy": {
            "name": "投信買賣超彙總表（買超）",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/domestic-inst/day.html",
            "select_element": {"name": "searchType", "value": "buy"},
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 2,
            "skiprows": 1,
            "priority": 3
        },
        "investment_trust_sell": {
            "name": "投信買賣超彙總表（賣超）",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/domestic-inst/day.html",
            "select_element": {"name": "searchType", "value": "sell"},
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 2,
            "skiprows": 1,
            "priority": 3
        },
        "highlight": {
            "name": "上櫃股票信用交易融資融券餘額概況表",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/highlight.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 2,
            "skiprows": 2,
            "priority": 4
        },
        "sbl": {
            "name": "信用額度總量管制餘額表",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/sbl.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 2,
            "skiprows": 2,
            "priority": 5
        },
        "exempted": {
            "name": "平盤下得融(借)券賣出之證券名單",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/exempted.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 2,
            "skiprows": 1,
            "priority": 5
        }
    },
    "settings": {
        "max_retry_days": 7,
        "download_timeout": 30,     # 增加逾時時間
        "page_load_timeout": 15,    # 增加頁面載入時間
        "implicit_wait": 8,         # 增加隱式等待
        "headless": True,           # 建議無頭模式提高穩定性
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
}

def setup_logging():
    """設定日誌系統"""
    LOG_DIR.mkdir(exist_ok=True)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 日誌檔案包含批量下載標識
    file_handler = logging.FileHandler(
        LOG_DIR / f"otc_historical_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

class PerformanceMonitor:
    """效能監控器"""
    
    def __init__(self):
        self.metrics = {}
    
    @contextmanager
    def measure_time(self, operation_name: str):
        start_time = time.time()
        start_memory = psutil.virtual_memory().used / 1024 / 1024
        
        try:
            yield
        finally:
            end_time = time.time()
            end_memory = psutil.virtual_memory().used / 1024 / 1024
            duration = end_time - start_time
            memory_diff = end_memory - start_memory
            
            self.metrics[operation_name] = {
                "duration_seconds": round(duration, 2),
                "memory_change_mb": round(memory_diff, 2),
                "timestamp": datetime.now().isoformat()
            }
            logging.info(f"{operation_name} 完成 - 耗時: {duration:.2f}秒, 記憶體變化: {memory_diff:+.2f}MB")
    
    def get_summary(self) -> Dict[str, Any]:
        if not self.metrics:
            return {"message": "無效能資料"}
        total_time = sum(m["duration_seconds"] for m in self.metrics.values())
        return {
            "total_operations": len(self.metrics),
            "total_duration_seconds": round(total_time, 2),
            "operations": self.metrics
        }

class DataValidator:
    """資料驗證器 - 保持原有邏輯"""
    
    def validate_dataframe(self, df: pd.DataFrame, file_type: str) -> Dict[str, Any]:
        results = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "statistics": {}
        }
        results["statistics"] = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "null_counts": df.isnull().sum().to_dict(),
            "unique_stocks": df['stock_id'].nunique() if 'stock_id' in df.columns else 0
        }
        
        if 'stock_id' in df.columns:
            invalid_stock_ids = df[~df['stock_id'].str.match(r'^\d{4}$', na=False)]
            if not invalid_stock_ids.empty:
                results["errors"].append({
                    "type": "invalid_stock_id",
                    "count": len(invalid_stock_ids),
                    "samples": invalid_stock_ids['stock_id'].head(5).tolist()
                })
                results["is_valid"] = False
        
        return results

class OTCHistoricalDownloader:
    """OTC歷史資料批量下載器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.download_items = config.get("download_items", {})
        self.settings = config.get("settings", {})
        self.driver = None
        self.performance_monitor = PerformanceMonitor()
    
    def ensure_dir(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        logging.info(f"確保目錄存在: {path}")
    
    def setup_chrome_driver(self) -> webdriver.Chrome:
        """設定 Chrome 瀏覽器 - 針對長時間執行優化"""
        options = Options()
        prefs = {
            "download.default_directory": str(DOWNLOAD_DIR),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_setting_values": {
                "images": 2,
                "plugins": 2,
                "popups": 2,
                "geolocation": 2,
                "notifications": 2,
                "media_stream": 2,
            }
        }
        options.add_experimental_option("prefs", prefs)
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-images')
        options.add_argument('--disable-gpu')  # 減少資源使用
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        if self.settings.get('headless', True):
            options.add_argument('--headless')
        
        if 'user_agent' in self.settings:
            options.add_argument(f'--user-agent={self.settings["user_agent"]}')
        
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.implicitly_wait(self.settings.get('implicit_wait', 8))
            driver.set_page_load_timeout(self.settings.get('page_load_timeout', 15))
            logging.info("Chrome WebDriver 初始化成功（歷史批量模式）")
            return driver
        except Exception as e:
            logging.error(f"Chrome WebDriver 初始化失敗: {e}")
            raise
    
    def smart_delay(self):
        """智能延遲，避免被偵測"""
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        logging.info(f"[⏳] 等待 {delay:.1f} 秒...")
        time.sleep(delay)
    
    def convert_date_to_roc(self, date_obj: datetime) -> tuple:
        """轉換為民國年格式，回傳 (年, 月, 日)"""
        roc_year = date_obj.year - 1911
        return roc_year, date_obj.month, date_obj.day
    
    def generate_trading_dates(self) -> List[datetime]:
        """生成交易日期列表（跳過週末和假日）"""
        tw_holidays = holidays.TW()
        dates = []
        current_date = START_DATE
        
        while current_date <= END_DATE:
            # 跳過週末和假日
            if current_date.weekday() < 5 and current_date.date() not in tw_holidays:
                dates.append(current_date)
            current_date += timedelta(days=1)
        
        return dates
    
    def get_existing_files(self) -> set:
        """取得已存在的檔案，避免重複下載"""
        if not RAW_DIR.exists():
            return set()
        
        existing_files = set()
        for file_path in RAW_DIR.glob("*.csv"):
            # 提取日期和資料類型
            match = re.search(r'(\d{8})_(.+)\.csv', file_path.name)
            if match:
                date_str, data_type = match.groups()
                existing_files.add(f"{date_str}_{data_type}")
        
        return existing_files
    
    def set_date_on_page(self, date_obj: datetime) -> bool:
        """在頁面上設定日期 - 支援年月下拉 + 日期點選"""
        try:
            roc_year, month, day = self.convert_date_to_roc(date_obj)
            logging.info(f"  設定日期：民國{roc_year}年{month}月{day}日")
            
            # 等待頁面載入
            time.sleep(2)
            
            # 方法1: 嘗試使用文字輸入（如 daily_close_no1430）
            try:
                date_input = self.driver.find_element(By.CSS_SELECTOR, "input[name='date'], input[type='text'].date")
                roc_date_str = f"{roc_year}/{month:02d}/{day:02d}"
                self.driver.execute_script(f"""
                    var dateInput = arguments[0];
                    dateInput.removeAttribute('readonly');
                    dateInput.value = '{roc_date_str}';
                    dateInput.dispatchEvent(new Event('change', {{ bubbles: true }}));
                """, date_input)
                logging.info(f"    使用文字輸入設定日期：{roc_date_str}")
                time.sleep(1)
                return True
            except:
                pass
            
            # 方法2: 使用年月下拉 + 日期點選
            try:
                # 設定年份
                year_elements = self.driver.find_elements(By.NAME, "year")
                if year_elements:
                    year_select = Select(year_elements[0])
                    year_select.select_by_value(str(roc_year))
                    logging.info(f"    設定年份：{roc_year}")
                    time.sleep(1)
                
                # 設定月份
                month_elements = self.driver.find_elements(By.NAME, "month")
                if month_elements:
                    month_select = Select(month_elements[0])
                    month_select.select_by_value(str(month))
                    logging.info(f"    設定月份：{month}")
                    time.sleep(1)
                
                # 點選日期（如果有日曆）
                try:
                    day_elements = self.driver.find_elements(By.XPATH, f"//td[text()='{day}' and not(contains(@class, 'disabled'))]")
                    if day_elements:
                        day_elements[0].click()
                        logging.info(f"    點選日期：{day}")
                        time.sleep(1)
                except:
                    logging.debug("    未找到可點選的日期元素")
                
                return True
            except Exception as e:
                logging.warning(f"    年月日設定失敗：{e}")
            
            # 方法3: 直接使用資料日期輸入框
            try:
                date_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[placeholder*='日期'], input[id*='date'], input[class*='date']")
                for date_input in date_inputs:
                    try:
                        roc_date_str = f"{roc_year}/{month:02d}/{day:02d}"
                        self.driver.execute_script(f"""
                            arguments[0].value = '{roc_date_str}';
                            arguments[0].dispatchEvent(new Event('change'));
                        """, date_input)
                        logging.info(f"    使用通用日期輸入：{roc_date_str}")
                        time.sleep(1)
                        return True
                    except:
                        continue
            except:
                pass
            
            logging.warning("  所有日期設定方法均失敗")
            return False
            
        except Exception as e:
            logging.error(f"  日期設定錯誤：{e}")
            return False
    
    def close_cookie_banner(self) -> None:
        """關閉 Cookie 橫幅"""
        try:
            cookie_btn = WebDriverWait(self.driver, 2).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".cookie-banner .btn-close"))
            )
            cookie_btn.click()
            time.sleep(0.5)
            logging.debug("Cookie banner 已關閉")
        except:
            logging.debug("未找到 cookie banner")
    
    def wait_for_download(self, filename_pattern: str, timeout: int = None) -> Optional[Path]:
        """等待檔案下載完成"""
        if timeout is None:
            timeout = self.settings.get('download_timeout', 30)
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            for filename in DOWNLOAD_DIR.iterdir():
                if filename_pattern in filename.name and not filename.name.endswith('.crdownload'):
                    logging.info(f"    下載完成: {filename}")
                    return filename
            time.sleep(0.5)
        logging.warning(f"    下載逾時: {filename_pattern}")
        return None
    
    def download_single_item(self, name: str, config: Dict[str, Any], date_obj: datetime) -> bool:
        """下載單一項目的單一日期資料"""
        try:
            date_str = date_obj.strftime("%Y%m%d")
            logging.info(f"  [處理] {name} - {config['name']}")
            
            # 檢查檔案是否已存在
            expected_filename_patterns = [
                f"{date_str}_{name}.csv",
                f"{date_str}_{name}_buy.csv",
                f"{date_str}_{name}_sell.csv"
            ]
            
            for pattern in expected_filename_patterns:
                if (RAW_DIR / pattern).exists():
                    logging.info(f"    [⏭] 檔案已存在，跳過：{pattern}")
                    return True
            
            # 前往頁面
            self.driver.get(config['url'])
            time.sleep(3)
            self.close_cookie_banner()
            
            # 設定日期
            if not self.set_date_on_page(date_obj):
                logging.warning(f"    日期設定失敗，跳過")
                return False
            
            # 處理特殊設定
            if "select_element" in config:
                try:
                    sel_name = config["select_element"]["name"]
                    sel_val = config["select_element"]["value"]
                    select_elem = self.driver.find_element(By.NAME, sel_name)
                    select = Select(select_elem)
                    select.select_by_value(sel_val)
                    logging.info(f"    設定下拉 {sel_name} = {sel_val}")
                    time.sleep(1)
                except Exception as e:
                    logging.warning(f"    下拉設定失敗：{e}")
            
            # 點擊查詢按鈕（如果需要）
            if config.get("needs_query", False):
                try:
                    query_btns = self.driver.find_elements(By.CSS_SELECTOR, "button.btn-primary, button[type='submit']")
                    if query_btns:
                        query_btns[0].click()
                        logging.info("    點擊查詢按鈕")
                        time.sleep(4)
                except Exception as e:
                    logging.warning(f"    查詢按鈕點擊失敗：{e}")
            
            # 等待表格載入
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, config["wait_element"]))
                )
                logging.info("    資料表格載入完成")
            except:
                logging.warning("    資料表格載入逾時，嘗試繼續下載")
            
            # 執行下載
            return self._execute_download(name, config, date_str)
            
        except Exception as e:
            logging.error(f"    {name} 處理失敗：{e}")
            return False
    
    def _execute_download(self, name: str, config: Dict[str, Any], date_str: str) -> bool:
        """執行下載動作"""
        download_texts = [config["download_text"], "下載CSV", "另存CSV", "下載 CSV", "另存 CSV"]
        
        for txt in download_texts:
            try:
                # 尋找下載按鈕
                xpath = f"//a[contains(text(), '{txt}')] | //button[contains(text(), '{txt}')]"
                btn = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                self.driver.execute_script("arguments[0].click();", btn)
                logging.info(f"    點擊下載：『{txt}』")
                time.sleep(3)
                
                # 等待下載完成
                dl_file = self.wait_for_download(".csv", 30)
                if dl_file:
                    return self._move_downloaded_file(dl_file, name, date_str)
                    
            except Exception as e:
                logging.debug(f"    下載方法 '{txt}' 失敗：{e}")
                continue
        
        logging.error("    [❌] 所有下載方法均失敗")
        return False
    
    def _move_downloaded_file(self, dl_file: Path, name: str, date_str: str) -> bool:
        """移動下載的檔案到指定位置"""
        try:
            orig_name = dl_file.name
            
            # 根據不同類型設定檔案名稱
            if name == "investment_trust_buy":
                base, ext = os.path.splitext(orig_name)
                new_name = f"{date_str}_{name}.csv"
            elif name == "investment_trust_sell":
                base, ext = os.path.splitext(orig_name)
                new_name = f"{date_str}_{name}.csv"
            else:
                new_name = f"{date_str}_{name}.csv"
            
            new_path = RAW_DIR / new_name
            shutil.move(str(dl_file), str(new_path))
            logging.info(f"    [✅] 下載成功 → {new_path}")
            return True
            
        except Exception as e:
            logging.error(f"    移動檔案失敗：{e}")
            return False
    
    def download_all_historical(self) -> Dict[str, int]:
        """下載所有歷史資料"""
        self.ensure_dir(RAW_DIR)
        self.driver = self.setup_chrome_driver()
        
        # 生成交易日期列表
        trading_dates = self.generate_trading_dates()
        existing_files = self.get_existing_files()
        
        # 統計資訊
        total_dates = len(trading_dates)
        total_items = len(self.download_items)
        total_tasks = total_dates * total_items
        
        # 按優先順序排序下載項目
        sorted_items = sorted(
            self.download_items.items(),
            key=lambda x: x[1].get('priority', 999)
        )
        
        logging.info(f"\n=== 上櫃歷史資料批量下載開始 ===")
        logging.info(f"日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
        logging.info(f"交易日總數: {total_dates}")
        logging.info(f"資料項目數: {total_items}")
        logging.info(f"預計總任務: {total_tasks}")
        logging.info(f"已存在檔案: {len(existing_files)}")
        estimated_time = total_tasks * 15 / 60  # 每個任務約15秒
        logging.info(f"預估執行時間: {estimated_time:.1f} 分鐘")
        
        # 統計變數
        results = {
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "failed_tasks": []
        }
        
        try:
            # 雙重迴圈：外層日期，內層資料項目
            for date_idx, date_obj in enumerate(trading_dates, 1):
                date_str = date_obj.strftime("%Y%m%d")
                logging.info(f"\n── 處理日期 {date_obj.strftime('%Y-%m-%d')} ({date_idx}/{total_dates}) ──")
                
                # 內層：各個資料項目
                for item_idx, (name, config) in enumerate(sorted_items, 1):
                    task_desc = f"{name}_{date_str}"
                    logging.info(f"\n  任務 {item_idx}/{total_items}: {name}")
                    
                    # 檢查是否已存在
                    if task_desc in existing_files:
                        logging.info(f"    [⏭] 已存在，跳過")
                        results["skipped"] += 1
                        continue
                    
                    # 執行下載
                    try:
                        if self.download_single_item(name, config, date_obj):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_tasks"].append(task_desc)
                    except Exception as e:
                        logging.error(f"    任務執行異常：{e}")
                        results["failed"] += 1
                        results["failed_tasks"].append(task_desc)
                    
                    # 智能延遲（除了最後一個任務）
                    if not (date_idx == total_dates and item_idx == total_items):
                        self.smart_delay()
                
                # 每完成一個日期，記錄進度
                progress = (date_idx / total_dates) * 100
                logging.info(f"  日期 {date_str} 完成，整體進度：{progress:.1f}%")
        
        except KeyboardInterrupt:
            logging.warning("\n[⏹] 使用者中斷下載")
        except Exception as e:
            logging.error(f"\n[❌] 下載過程發生錯誤：{e}")
        finally:
            if self.driver:
                self.driver.quit()
                logging.info("Chrome WebDriver 已關閉")
        
        # 輸出最終統計
        logging.info(f"\n[📊] 下載統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        logging.info(f"    - 跳過: {results['skipped']}")
        logging.info(f"    - 總計: {results['success'] + results['failed'] + results['skipped']}")
        
        if results["failed_tasks"]:
            logging.warning(f"失敗任務清單: {results['failed_tasks'][:10]}...")  # 只顯示前10個
        
        return results

# ===== 清洗功能（保持原有邏輯） =====
class OTCDataCleaner:
    """OTC資料清洗器類別 - 完全保持原有邏輯"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.download_items = config.get("download_items", {})
        self.validator = DataValidator()
        self.performance_monitor = PerformanceMonitor()
    
    def ensure_dir(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
    
    def read_csv_with_encoding(self, file_path: Path, skiprows: int = 0) -> Optional[pd.DataFrame]:
        encodings = ["cp950", "big5", "utf-8-sig", "utf-8", "gb2312", "gbk", "gb18030"]
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, skiprows=skiprows,
                                 dtype=object, low_memory=False, thousands=',')
                logging.debug(f"成功使用 {encoding} 編碼讀取 {file_path.name}，skiprows={skiprows}")
                if df is not None and len(df.columns) > 0:
                    first_col = str(df.columns[0]).replace(',', '').replace('.', '')
                    if first_col.isdigit() and skiprows > 0:
                        logging.debug(f"偵測到欄位名稱異常（{df.columns[0]}），嘗試 skiprows={skiprows-1}")
                        return self.read_csv_with_encoding(file_path, skiprows - 1)
                return df
            except Exception as e:
                logging.debug(f"使用 {encoding} 編碼讀取失敗（skiprows={skiprows}）：{e}")
                continue
        logging.error(f"無法讀取檔案 {file_path.name}，skiprows={skiprows}")
        return None
    
    def get_file_type_and_config(self, filename: str) -> tuple:
        filename_lower = filename.lower()
        file_patterns = {
            "daily_close_no1430": ("daily_close_no1430", 3),
            "bigd_": ("institutional_detail", 1),
            "brktop1_": ("sec_trading", 2),
            "daytraderpt_": ("day_trading", 5),
            "margratio_": ("highlight", 2),
            "owz66u_": ("sbl", 2),
            "rsta3106_": ("margin_transactions", 2),
            "margmark_": ("exempted", 1)
        }
        for pattern, (config_key, skiprows) in file_patterns.items():
            if pattern in filename_lower:
                logging.debug(f"  檔案 {filename} 匹配模式 {pattern}，類型={config_key}，跳過行數={skiprows}")
                return config_key, skiprows
        
        if filename_lower.startswith("sit_"):
            if "_buy" in filename_lower:
                return "investment_trust_buy", 1
            elif "_sell" in filename_lower:
                return "investment_trust_sell", 1
        
        return None, 0
    
    def clean_numeric_column(self, series: pd.Series) -> pd.Series:
        cleaned = series.astype(str).str.replace(",", "").str.strip()
        cleaned = cleaned.replace(["--", "---", "----", "　", ""], "0")
        numeric_values = pd.to_numeric(cleaned, errors="coerce").fillna(0)
        
        result = []
        for val in numeric_values:
            if pd.isna(val):
                result.append(0)
            elif val == int(val):
                result.append(int(val))
            else:
                result.append(float(val))
        
        return pd.Series(result, index=series.index)
    
    def extract_stock_id(self, series: pd.Series) -> pd.Series:
        """提取4位數股票代號，排除含字母的代號"""
        def extract_4_digits(code_str):
            if pd.isna(code_str):
                return None
            code_str = str(code_str).strip()
            
            if code_str.isdigit():
                if len(code_str) < 3:
                    return None
                elif len(code_str) == 3:
                    return code_str.zfill(4)
                elif len(code_str) == 4:
                    return code_str
                else:
                    return None
            
            if any('\u4e00' <= char <= '\u9fff' for char in code_str):
                return None
            
            if re.search(r'[A-Za-z]', code_str):
                return None
            
            match = re.match(r'^(\d{3,4})', code_str)
            if match:
                digits = match.group(1)
                if len(digits) == 3:
                    return digits.zfill(4)
                elif len(digits) == 4:
                    return digits
            
            return None
        
        return series.apply(extract_4_digits)
    
    def get_all_raw_files_by_date(self) -> Dict[str, List[Path]]:
        """取得所有原始檔案，按日期分組"""
        if not RAW_DIR.exists():
            return {}
        
        files_by_date = {}
        for file_path in RAW_DIR.glob("*.csv"):
            # 提取日期
            match = re.search(r'(\d{8})_', file_path.name)
            if match:
                date_str = match.group(1)
                if date_str not in files_by_date:
                    files_by_date[date_str] = []
                files_by_date[date_str].append(file_path)
        
        return files_by_date
    
    def clean_single_file(self, file_path: Path) -> bool:
        """清洗單一檔案 - 保持原有清洗邏輯"""
        filename = file_path.name
        logging.info(f"  處理：{filename}")
        
        file_type, skiprows = self.get_file_type_and_config(filename)
        skiprows = max(skiprows, 0)
        if file_type is None:
            logging.warning("    [❌] 未匹配清洗規則，跳過")
            return False
        
        df = self.read_csv_with_encoding(file_path, skiprows)
        if df is None:
            return False
        
        df.columns = df.columns.str.strip()
        df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
        if len(df) == 0:
            logging.warning(f"    [❌] 檔案 {filename} 清理後無資料")
            return False
        
        try:
            clean_df = self._clean_by_type(df, file_type, filename)
            if clean_df is None or len(clean_df) == 0:
                logging.error(f"    [❌] 檔案 {filename} 清理失敗")
                return False
            
            # 最終過濾：只保留純4位數且 >=1000 的股票代號
            if 'stock_id' in clean_df.columns:
                clean_df = clean_df[
                    (clean_df['stock_id'].str.len() == 4) &
                    (clean_df['stock_id'].str.isdigit()) &
                    (clean_df['stock_id'].astype(int) >= 1000)
                ]
            
            validation_result = self.validator.validate_dataframe(clean_df, file_type)
            if not validation_result["is_valid"]:
                logging.warning(f"    [⚠️] 資料驗證發現問題：{validation_result['errors']}")
            
            # 輸出到 otc_cleaned 目錄，保持原檔名
            output_path = CLEAN_DIR / filename
            with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
                float_format = lambda x: '{:.0f}'.format(x) if isinstance(x, (int, float)) and x == int(x) else '{:.2f}'.format(x) if isinstance(x, float) else str(x)
                clean_df.to_csv(f, index=False, float_format=float_format)
            
            logging.info(f"    [✅] 清洗完成: {filename} ({len(clean_df)} 行)")
            return True
            
        except Exception as e:
            logging.error(f"    [❌] 清洗失敗：{e}")
            logging.error(traceback.format_exc())
            return False
    
    def _clean_by_type(self, df: pd.DataFrame, file_type: str, filename: str) -> Optional[pd.DataFrame]:
        """根據檔案類型選擇清洗方法 - 保持原有邏輯"""
        if file_type == "daily_close_no1430" or "daily_close_no1430" in filename.lower():
            return self._clean_daily_close(df)
        elif file_type == "institutional_detail" or "bigd_" in filename.lower():
            return self._clean_institutional_detail(df)
        elif file_type == "sec_trading" or "brktop1_" in filename.lower():
            return self._clean_sec_trading(df)
        elif file_type == "day_trading" or "daytraderpt_" in filename.lower():
            return self._clean_day_trading(df)
        elif file_type == "highlight" or "margratio_" in filename.lower():
            return self._clean_highlight(df)
        elif file_type == "sbl" or "owz66u_" in filename.lower():
            return self._clean_sbl(df)
        elif file_type == "margin_transactions" or "rsta3106_" in filename.lower():
            return self._clean_margin_transactions(df)
        elif file_type == "exempted" or "margmark_" in filename.lower():
            return self._clean_exempted(df)
        elif "investment_trust" in file_type or filename.lower().startswith("sit_"):
            return self._clean_investment_trust(df)
        else:
            logging.warning(f"  未知檔案類型：{file_type}")
            return None
    
    def _clean_daily_close(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗每日收盤行情資料"""
        cols = list(df.columns)
        
        if len(cols) > 0 and str(cols[0]).replace(',', '').replace('.', '').isdigit():
            logging.error(f"  欄位識別異常，第一欄為數字：{cols[0]}")
            return None
        
        code_col = next((c for c in cols if "代號" in c or "代碼" in c), None)
        name_col = next((c for c in cols if "名稱" in c), None)
        close_col = next((c for c in cols if "收盤" in c and "收盤" == c[:2]), None)
        
        if not all([code_col, name_col, close_col]):
            logging.error("  缺少必要欄位")
            return None
        
        column_mapping = {
            code_col: "stock_id",
            name_col: "name",
            close_col: "close"
        }
        
        optional_fields = {
            "漲跌": "change", "開盤": "open", "最高": "high", "最低": "low",
            "均價": "avg_price", "成交股數": "volume", "成交金額": "amount",
            "成交筆數": "trades", "最後買價": "last_bid_price", "最後買量": "last_bid_vol",
            "最後賣價": "last_ask_price", "最後賣量": "last_ask_vol", "發行股數": "issued_shares",
            "次日參考價": "next_ref_price", "次日漲停價": "next_up_limit", "次日跌停價": "next_down_limit"
        }
        
        for pattern, new_name in optional_fields.items():
            matching_col = next((c for c in cols if pattern in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
def verify_clean_data():
    """驗證清理後的資料品質"""
    logging.info("\n=== 資料品質驗證 ===")
    
    issues = []
    
    for csv_file in CLEAN_DIR.glob("*.csv"):
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            
            if 'stock_id' in df.columns:
                invalid_ids = df[~df['stock_id'].astype(str).str.match(r'^\d{4}
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_institutional_detail(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗三大法人買賣明細資料"""
        cols = list(df.columns)
        
        code_col = next((c for c in cols if "代號" in c or "代碼" in c), None)
        name_col = next((c for c in cols if "名稱" in c), None)
        
        if not all([code_col, name_col]):
            logging.error("  缺少必要欄位")
            return None
        
        column_mapping = {
            code_col: "stock_id",
            name_col: "name"
        }
        
        institutional_fields = {
            "外資及陸資": "ii_foreign_net",
            "外資自營商": "ii_foreign_self_net",
            "投信": "ii_trust_net",
            "自營商(自行買賣)": "ii_dealer_self_net",
            "自營商(避險)": "ii_dealer_hedge_net",
            "合計": "ii_total_net"
        }
        
        for pattern, new_name in institutional_fields.items():
            matching_col = next((c for c in cols if pattern in c and "買賣超" in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_sec_trading(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗券商營業額統計資料"""
        cols = list(df.columns)
        
        if len(cols) < 5:
            logging.error("  欄位數量不足")
            return None
        
        left_cols = cols[:5]
        column_mapping = {
            left_cols[0]: "rank",
            left_cols[1]: "prev_rank",
            left_cols[2]: "broker",
            left_cols[3]: "name",
            left_cols[4]: "amount_thousands"
        }
        
        clean_df = df[left_cols].rename(columns=column_mapping).copy()
        
        numeric_cols = ["rank", "prev_rank", "amount_thousands"]
        for col in numeric_cols:
            if col in clean_df.columns:
                clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("broker").reset_index(drop=True)
    
    def _clean_day_trading(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗現股當沖交易統計資料"""
        cols = list(df.columns)
        
        # 過濾統計行
        if len(df) > 0:
            first_col = df.iloc[:, 0].astype(str)
            mask = ~first_col.str.contains('共計|合計|總計|統計|說明|註[：:]', na=False, regex=True)
            df = df[mask].copy()
        
        code_col = None
        name_col = None
        
        for pattern in ["證券代號", "代號", "股票代號", "代碼"]:
            for c in cols:
                if pattern in c:
                    code_col = c
                    break
            if code_col:
                break
        
        for pattern in ["證券名稱", "名稱", "股票名稱"]:
            for c in cols:
                if pattern in c:
                    name_col = c
                    break
            if name_col:
                break
        
        if not code_col and len(cols) >= 2:
            if df.iloc[0, 0] and str(df.iloc[0, 0]).strip().replace(' ', ''):
                if any(char.isdigit() for char in str(df.iloc[0, 0])):
                    code_col = cols[0]
                    name_col = cols[1] if len(cols) > 1 else None
        
        if not code_col:
            logging.error(f"  無法識別證券代號欄位，欄位列表：{cols}")
            return None
        
        column_mapping = {code_col: "stock_id"}
        if name_col:
            column_mapping[name_col] = "name"
        
        dt_fields = {
            "暫停": "flag",
            "成交股數": "dt_volume",
            "買進成交金額": "dt_buy_amount",
            "賣出成交金額": "dt_sell_amount",
            "買賣總額": "dt_total_amount",
            "當沖率": "dt_rate"
        }
        
        for pattern, new_name in dt_fields.items():
            matching_col = next((c for c in cols if pattern in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name", "flag"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_highlight(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗融資融券餘額概況資料"""
        cols = list(df.columns)
        
        rank_col = next((c for c in cols if "排名" in c), None)
        code_col = next((c for c in cols if c == "代號"), None)
        name_col = next((c for c in cols if c == "名稱"), None)
        
        if not all([rank_col, code_col, name_col]):
            logging.error("  缺少必要欄位")
            return None
        
        column_mapping = {
            rank_col: "rank",
            code_col: "stock_id",
            name_col: "name"
        }
        
        margin_fields = {
            "月均融資餘額": "hg_margin_balance",
            "月均融券餘額": "hg_short_balance",
            "券資比": "hg_ratio"
        }
        
        for pattern, new_name in margin_fields.items():
            matching_col = next((c for c in cols if pattern in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_sbl(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗信用額度總量管制餘額資料"""
        cols = list(df.columns)
        
        code_col = next((c for c in cols if "股票代號" in c), None)
        name_col = next((c for c in cols if "股票名稱" in c), None)
        
        if not all([code_col, name_col]):
            logging.error("  缺少必要欄位")
            return None
        
        column_mapping = {
            code_col: "stock_id",
            name_col: "name"
        }
        
        sbl_fields = {
            "融券前日餘額": "owz_short_prev_balance",
            "融券賣出": "owz_short_sell",
            "融券買進": "owz_short_buy",
            "融券現券": "owz_short_spot",
            "融券當日餘額": "owz_short_today_balance",
            "融券限額": "owz_short_limit",
            "借券前日餘額": "owz_borrow_prev_balance",
            "借券當日賣出": "owz_borrow_sell",
            "借券當日還券": "owz_borrow_return",
            "借券當日調整數額": "owz_borrow_adj",
            "借券當日餘額": "owz_borrow_today_balance",
            "借券次一營業日可借券賣出限額": "owz_borrow_next_limit",
            "備註": "remark"
        }
        
        for pattern, new_name in sbl_fields.items():
            matching_col = next((c for c in cols if pattern in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name", "remark"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_margin_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗融資融券餘額資料"""
        cols = list(df.columns)
        
        # 過濾只保留數字開頭的行
        if len(df) > 0:
            first_col_str = df.iloc[:, 0].astype(str)
            mask = first_col_str.str.match(r'^\d', na=False)
            df = df[mask].copy()
        
        code_col = None
        name_col = None
        
        for pattern in ["代號", "代碼", "股票代號", "證券代號"]:
            for c in cols:
                if pattern in c:
                    code_col = c
                    break
            if code_col:
                break
        
        for pattern in ["名稱", "股票名稱", "證券名稱"]:
            for c in cols:
                if pattern in c:
                    name_col = c
                    break
            if name_col:
                break
        
        if not code_col and len(cols) >= 2:
            if df.iloc[0, 0] and re.match(r'^\d+', str(df.iloc[0, 0])):
                code_col = cols[0]
                name_col = cols[1]
        
        if not code_col:
            logging.error(f"  無法識別證券代號欄位，欄位列表：{cols}")
            return None
        
        column_mapping = {code_col: "stock_id"}
        if name_col:
            column_mapping[name_col] = "name"
        
        mt_fields = {
            "前資餘額": "mt_prev_balance",
            "資買": "mt_buy",
            "資賣": "mt_sell",
            "現償": "mt_pay",
            "資餘額": "mt_balance",
            "資屬證金": "mt_cash",
            "資使用率": "mt_usage_rate",
            "資限額": "mt_limit",
            "前券餘額": "st_prev_balance",
            "券賣": "st_sell",
            "券買": "st_buy",
            "券償": "st_pay",
            "券餘額": "st_balance",
            "券屬證金": "st_cash",
            "券使用率": "st_usage_rate",
            "券限額": "st_limit",
            "資券相抵": "mt_st_offset",
            "備註": "remark"
        }
        
        for pattern, new_name in mt_fields.items():
            matching_col = next((c for c in cols if pattern in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name", "remark"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_exempted(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗平盤下得融券賣出證券名單"""
        cols = list(df.columns)
        
        # 找到數據開始的行
        data_start_row = 0
        for i, row in df.iterrows():
            first_val = str(row.iloc[0])
            if re.match(r'^\d{3,4}', first_val):
                data_start_row = i
                break
        
        if data_start_row > 0:
            df = df.iloc[data_start_row:].copy()
            df.columns = cols
        
        code_col = None
        name_col = None
        
        for c in cols:
            if "證券代號" in c or "代號" in c or "代碼" in c:
                code_col = c
                break
        
        for c in cols:
            if "證券名稱" in c or "名稱" in c:
                name_col = c
                break
        
        if not code_col:
            if len(cols) >= 2:
                code_col = cols[0]
                name_col = cols[1] if not name_col else name_col
        
        if not code_col:
            logging.error("  無法識別證券代號欄位")
            return None
        
        column_mapping = {code_col: "stock_id"}
        if name_col:
            column_mapping[name_col] = "name"
        
        # 找到標記欄位
        mark_cols = []
        for c in cols:
            if c not in [code_col, name_col]:
                if "暫停" in c or "標記" in c or "註記" in c or len(c) <= 3:
                    mark_cols.append(c)
        
        for i, mark_col in enumerate(mark_cols):
            column_mapping[mark_col] = f"mark_{i+1}" if i > 0 else "mark"
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        # 過濾統計行
        if "name" in clean_df.columns:
            clean_df = clean_df[~clean_df["name"].str.contains("共.*筆|合計|統計|註:|說明:", na=False, regex=True)]
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_investment_trust(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗投信買賣超資料"""
        cols = list(df.columns)
        
        rank_col = next((c for c in cols if "排行" in c), None)
        code_col = next((c for c in cols if "代號" in c), None)
        name_col = next((c for c in cols if "名稱" in c), None)
        
        if not all([rank_col, code_col, name_col]):
            logging.error("  缺少必要欄位")
            return None
        
        column_mapping = {
            rank_col: "rank",
            code_col: "stock_id",
            name_col: "name"
        }
        
        it_fields = {
            "買進": "it_buy_shares",
            "賣出": "it_sell_shares",
            "買賣超": "it_diff_shares",
            "買進金額": "it_buy_amount",
            "賣出金額": "it_sell_amount",
            "買賣超金額": "it_diff_amount"
        }
        
        for pattern, new_name in it_fields.items():
            matching_col = next((c for c in cols if pattern in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["rank", "stock_id", "name"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
                if len(invalid_ids) > 0:
                    issues.append({
                        'file': csv_file.name,
                        'issue': '包含非4位數字股票代號',
                        'count': len(invalid_ids),
                        'samples': invalid_ids['stock_id'].head(5).tolist()
                    })
                
                for col in df.select_dtypes(include=['float64', 'int64']).columns:
                    if df[col].astype(str).str.contains(r'[eE][+-]?\d+', regex=True).any():
                        issues.append({
                            'file': csv_file.name,
                            'issue': f'欄位 {col} 包含科學記號',
                            'column': col
                        })
        
        except Exception as e:
            logging.error(f"驗證檔案 {csv_file.name} 時發生錯誤: {e}")
    
    if issues:
        logging.warning("發現以下資料品質問題：")
        for issue in issues:
            logging.warning(f"  - {issue}")
    else:
        logging.info("所有檔案資料品質良好！")
    
    return issues
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_institutional_detail(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗三大法人買賣明細資料"""
        cols = list(df.columns)
        
        code_col = next((c for c in cols if "代號" in c or "代碼" in c), None)
        name_col = next((c for c in cols if "名稱" in c), None)
        
        if not all([code_col, name_col]):
            logging.error("  缺少必要欄位")
            return None
        
        column_mapping = {
            code_col: "stock_id",
            name_col: "name"
        }
        
        institutional_fields = {
            "外資及陸資": "ii_foreign_net",
            "外資自營商": "ii_foreign_self_net",
            "投信": "ii_trust_net",
            "自營商(自行買賣)": "ii_dealer_self_net",
            "自營商(避險)": "ii_dealer_hedge_net",
            "合計": "ii_total_net"
        }
        
        for pattern, new_name in institutional_fields.items():
            matching_col = next((c for c in cols if pattern in c and "買賣超" in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_sec_trading(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗券商營業額統計資料"""
        cols = list(df.columns)
        
        if len(cols) < 5:
            logging.error("  欄位數量不足")
            return None
        
        left_cols = cols[:5]
        column_mapping = {
            left_cols[0]: "rank",
            left_cols[1]: "prev_rank",
            left_cols[2]: "broker",
            left_cols[3]: "name",
            left_cols[4]: "amount_thousands"
        }
        
        clean_df = df[left_cols].rename(columns=column_mapping).copy()
        
        numeric_cols = ["rank", "prev_rank", "amount_thousands"]
        for col in numeric_cols:
            if col in clean_df.columns:
                clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("broker").reset_index(drop=True)
    
    def _clean_day_trading(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗現股當沖交易統計資料"""
        cols = list(df.columns)
        
        # 過濾統計行
        if len(df) > 0:
            first_col = df.iloc[:, 0].astype(str)
            mask = ~first_col.str.contains('共計|合計|總計|統計|說明|註[：:]', na=False, regex=True)
            df = df[mask].copy()
        
        code_col = None
        name_col = None
        
        for pattern in ["證券代號", "代號", "股票代號", "代碼"]:
            for c in cols:
                if pattern in c:
                    code_col = c
                    break
            if code_col:
                break
        
        for pattern in ["證券名稱", "名稱", "股票名稱"]:
            for c in cols:
                if pattern in c:
                    name_col = c
                    break
            if name_col:
                break
        
        if not code_col and len(cols) >= 2:
            if df.iloc[0, 0] and str(df.iloc[0, 0]).strip().replace(' ', ''):
                if any(char.isdigit() for char in str(df.iloc[0, 0])):
                    code_col = cols[0]
                    name_col = cols[1] if len(cols) > 1 else None
        
        if not code_col:
            logging.error(f"  無法識別證券代號欄位，欄位列表：{cols}")
            return None
        
        column_mapping = {code_col: "stock_id"}
        if name_col:
            column_mapping[name_col] = "name"
        
        dt_fields = {
            "暫停": "flag",
            "成交股數": "dt_volume",
            "買進成交金額": "dt_buy_amount",
            "賣出成交金額": "dt_sell_amount",
            "買賣總額": "dt_total_amount",
            "當沖率": "dt_rate"
        }
        
        for pattern, new_name in dt_fields.items():
            matching_col = next((c for c in cols if pattern in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name", "flag"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_highlight(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗融資融券餘額概況資料"""
        cols = list(df.columns)
        
        rank_col = next((c for c in cols if "排名" in c), None)
        code_col = next((c for c in cols if c == "代號"), None)
        name_col = next((c for c in cols if c == "名稱"), None)
        
        if not all([rank_col, code_col, name_col]):
            logging.error("  缺少必要欄位")
            return None
        
        column_mapping = {
            rank_col: "rank",
            code_col: "stock_id",
            name_col: "name"
        }
        
        margin_fields = {
            "月均融資餘額": "hg_margin_balance",
            "月均融券餘額": "hg_short_balance",
            "券資比": "hg_ratio"
        }
        
        for pattern, new_name in margin_fields.items():
            matching_col = next((c for c in cols if pattern in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_sbl(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗信用額度總量管制餘額資料"""
        cols = list(df.columns)
        
        code_col = next((c for c in cols if "股票代號" in c), None)
        name_col = next((c for c in cols if "股票名稱" in c), None)
        
        if not all([code_col, name_col]):
            logging.error("  缺少必要欄位")
            return None
        
        column_mapping = {
            code_col: "stock_id",
            name_col: "name"
        }
        
        sbl_fields = {
            "融券前日餘額": "owz_short_prev_balance",
            "融券賣出": "owz_short_sell",
            "融券買進": "owz_short_buy",
            "融券現券": "owz_short_spot",
            "融券當日餘額": "owz_short_today_balance",
            "融券限額": "owz_short_limit",
            "借券前日餘額": "owz_borrow_prev_balance",
            "借券當日賣出": "owz_borrow_sell",
            "借券當日還券": "owz_borrow_return",
            "借券當日調整數額": "owz_borrow_adj",
            "借券當日餘額": "owz_borrow_today_balance",
            "借券次一營業日可借券賣出限額": "owz_borrow_next_limit",
            "備註": "remark"
        }
        
        for pattern, new_name in sbl_fields.items():
            matching_col = next((c for c in cols if pattern in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name", "remark"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_margin_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗融資融券餘額資料"""
        cols = list(df.columns)
        
        # 過濾只保留數字開頭的行
        if len(df) > 0:
            first_col_str = df.iloc[:, 0].astype(str)
            mask = first_col_str.str.match(r'^\d', na=False)
            df = df[mask].copy()
        
        code_col = None
        name_col = None
        
        for pattern in ["代號", "代碼", "股票代號", "證券代號"]:
            for c in cols:
                if pattern in c:
                    code_col = c
                    break
            if code_col:
                break
        
        for pattern in ["名稱", "股票名稱", "證券名稱"]:
            for c in cols:
                if pattern in c:
                    name_col = c
                    break
            if name_col:
                break
        
        if not code_col and len(cols) >= 2:
            if df.iloc[0, 0] and re.match(r'^\d+', str(df.iloc[0, 0])):
                code_col = cols[0]
                name_col = cols[1]
        
        if not code_col:
            logging.error(f"  無法識別證券代號欄位，欄位列表：{cols}")
            return None
        
        column_mapping = {code_col: "stock_id"}
        if name_col:
            column_mapping[name_col] = "name"
        
        mt_fields = {
            "前資餘額": "mt_prev_balance",
            "資買": "mt_buy",
            "資賣": "mt_sell",
            "現償": "mt_pay",
            "資餘額": "mt_balance",
            "資屬證金": "mt_cash",
            "資使用率": "mt_usage_rate",
            "資限額": "mt_limit",
            "前券餘額": "st_prev_balance",
            "券賣": "st_sell",
            "券買": "st_buy",
            "券償": "st_pay",
            "券餘額": "st_balance",
            "券屬證金": "st_cash",
            "券使用率": "st_usage_rate",
            "券限額": "st_limit",
            "資券相抵": "mt_st_offset",
            "備註": "remark"
        }
        
        for pattern, new_name in mt_fields.items():
            matching_col = next((c for c in cols if pattern in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name", "remark"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_exempted(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗平盤下得融券賣出證券名單"""
        cols = list(df.columns)
        
        # 找到數據開始的行
        data_start_row = 0
        for i, row in df.iterrows():
            first_val = str(row.iloc[0])
            if re.match(r'^\d{3,4}', first_val):
                data_start_row = i
                break
        
        if data_start_row > 0:
            df = df.iloc[data_start_row:].copy()
            df.columns = cols
        
        code_col = None
        name_col = None
        
        for c in cols:
            if "證券代號" in c or "代號" in c or "代碼" in c:
                code_col = c
                break
        
        for c in cols:
            if "證券名稱" in c or "名稱" in c:
                name_col = c
                break
        
        if not code_col:
            if len(cols) >= 2:
                code_col = cols[0]
                name_col = cols[1] if not name_col else name_col
        
        if not code_col:
            logging.error("  無法識別證券代號欄位")
            return None
        
        column_mapping = {code_col: "stock_id"}
        if name_col:
            column_mapping[name_col] = "name"
        
        # 找到標記欄位
        mark_cols = []
        for c in cols:
            if c not in [code_col, name_col]:
                if "暫停" in c or "標記" in c or "註記" in c or len(c) <= 3:
                    mark_cols.append(c)
        
        for i, mark_col in enumerate(mark_cols):
            column_mapping[mark_col] = f"mark_{i+1}" if i > 0 else "mark"
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}$'
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        # 過濾統計行
        if "name" in clean_df.columns:
            clean_df = clean_df[~clean_df["name"].str.contains("共.*筆|合計|統計|註:|說明:", na=False, regex=True)]
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def _clean_investment_trust(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗投信買賣超資料"""
        cols = list(df.columns)
        
        rank_col = next((c for c in cols if "排行" in c), None)
        code_col = next((c for c in cols if "代號" in c), None)
        name_col = next((c for c in cols if "名稱" in c), None)
        
        if not all([rank_col, code_col, name_col]):
            logging.error("  缺少必要欄位")
            return None
        
        column_mapping = {
            rank_col: "rank",
            code_col: "stock_id",
            name_col: "name"
        }
        
        it_fields = {
            "買進": "it_buy_shares",
            "賣出": "it_sell_shares",
            "買賣超": "it_diff_shares",
            "買進金額": "it_buy_amount",
            "賣出金額": "it_sell_amount",
            "買賣超金額": "it_diff_amount"
        }
        
        for pattern, new_name in it_fields.items():
            matching_col = next((c for c in cols if pattern in c), None)
            if matching_col:
                column_mapping[matching_col] = new_name
        
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        clean_df = clean_df[clean_df["stock_id"].str.match(r'^\d{4}
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main(), na=False)]
        
        numeric_cols = [col for col in clean_df.columns if col not in ["rank", "stock_id", "name"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
        
        return clean_df.sort_values("stock_id").reset_index(drop=True)
    
    def clean_all_historical_files(self) -> Dict[str, int]:
        """清洗所有歷史檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        # 取得所有檔案，按日期分組
        files_by_date = self.get_all_raw_files_by_date()
        total_dates = len(files_by_date)
        
        logging.info(f"\n=== 開始清洗歷史資料 ===")
        logging.info(f"找到 {total_dates} 個日期的資料")
        
        with self.performance_monitor.measure_time("歷史資料清洗"):
            for date_idx, (date_str, file_list) in enumerate(sorted(files_by_date.items()), 1):
                logging.info(f"\n── 清洗日期 {date_str} ({date_idx}/{total_dates}) ──")
                
                for file_path in file_list:
                    try:
                        if self.clean_single_file(file_path):
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(file_path.name)
                    except Exception as e:
                        logging.error(f"清理檔案 {file_path.name} 時發生錯誤: {e}")
                        results["failed"] += 1
                        results["failed_files"].append(file_path.name)
        
        logging.info(f"\n[📊] 清洗統計:")
        logging.info(f"    - 成功: {results['success']}")
        logging.info(f"    - 失敗: {results['failed']}")
        
        return results

def main():
    """主要執行函數"""
    setup_logging()
    logging.info("=== 上櫃歷史資料批量下載 + 清洗系統 ===")
    
    config = load_config()
    
    # 確認執行
    print(f"\n目標日期範圍: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print(f"預估交易日: ~{len([d for d in pd.date_range(START_DATE, END_DATE) if d.weekday() < 5])} 天")
    print(f"資料項目: {len(config['download_items'])} 種")
    print(f"預估總時間: 4-6 小時")
    print(f"輸出目錄: {RAW_DIR} (原始), {CLEAN_DIR} (清洗)")
    
    response = input("\n⚠️  這是長時間執行任務，是否確定開始? (y/N): ").strip().lower()
    if response != 'y':
        print("取消執行")
        return
    
    start_time = datetime.now()
    performance_monitor = PerformanceMonitor()
    
    try:
        # 步驟 1: 批量下載歷史資料
        logging.info("\n=== 步驟 1: 批量下載歷史資料 ===")
        downloader = OTCHistoricalDownloader(config)
        
        with performance_monitor.measure_time("總下載時間"):
            download_results = downloader.download_all_historical()
        
        # 步驟 2: 清洗所有下載的資料
        logging.info("\n=== 步驟 2: 清洗歷史資料 ===")
        cleaner = OTCDataCleaner(config)
        
        with performance_monitor.measure_time("總清洗時間"):
            clean_results = cleaner.clean_all_historical_files()
        
        # 完成報告
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"\n[🎉] 所有程序完成!")
        logging.info(f"[⏱] 總執行時間: {duration}")
        logging.info(f"[📊] 下載結果: 成功 {download_results['success']}, 失敗 {download_results['failed']}, 跳過 {download_results['skipped']}")
        logging.info(f"[📊] 清洗結果: 成功 {clean_results['success']}, 失敗 {clean_results['failed']}")
        logging.info(f"[📁] 原始資料: {RAW_DIR}")
        logging.info(f"[📁] 清洗資料: {CLEAN_DIR}")
        
        # 保存執行報告
        performance_report_path = LOG_DIR / f"historical_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
    except KeyboardInterrupt:
        logging.warning("\n[⏹] 使用者中斷執行")
    except Exception as e:
        logging.error(f"\n[❌] 執行過程發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise

def load_config() -> Dict[str, Any]:
    """載入設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logging.info(f"已載入設定檔: {config_file}")
            return config
        except Exception as e:
            logging.warning(f"設定檔載入失敗，使用預設設定: {e}")
    else:
        logging.info("未找到設定檔，使用預設設定")
    
    return DEFAULT_CONFIG

if __name__ == "__main__":
    main()
