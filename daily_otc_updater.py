import json
CONFIG_JSON = '''#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定目錄
BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "otc_raw"
DOWNLOAD_DIR = Path.home() / "Downloads"
CLEAN_DIR = BASE_DIR / "otc_cleaned"
LOG_DIR = BASE_DIR / "logs"

# 預設設定
DEFAULT_CONFIG = {
    "download_items": {
        "daily_close_no1430": {
            "name": "上櫃股票每日收盤行情(不含定價)",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/mi-pricing.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": True,
            "retry_count": 3,
            "skiprows": 3
        },
        "margin_transactions": {
            "name": "上櫃股票融資融券餘額",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/transactions.html",
            "wait_element": "table.table-default",
            "download_text": "下載 CSV 檔(UTF-8)",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 2
        },
        "institutional_detail": {
            "name": "三大法人買賣明細資訊",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/detail/day.html",
            "select_element": {"name": "sect", "value": "AL"},
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 1
        },
        "day_trading": {
            "name": "現股當沖交易統計資訊",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/day-trading/statistics/day.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 5
        },
        "sec_trading": {
            "name": "各券商當日營業金額統計表",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/sec-trading.html",
            "wait_element": "table.table-default",
            "download_text": "下載 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 2
        },
        "investment_trust_buy": {
            "name": "投信買賣超彙總表（買超）",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/domestic-inst/day.html",
            "select_element": {"name": "searchType", "value": "buy"},
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 1
        },
        "investment_trust_sell": {
            "name": "投信買賣超彙總表（賣超）",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/domestic-inst/day.html",
            "select_element": {"name": "searchType", "value": "sell"},
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 1
        },
        "highlight": {
            "name": "上櫃股票信用交易融資融券餘額概況表",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/highlight.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 2
        },
        "sbl": {
            "name": "信用額度總量管制餘額表",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/sbl.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 2
        },
        "exempted": {
            "name": "平盤下得融(借)券賣出之證券名單",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/exempted.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 2
        }
    },
    "settings": {
        "max_retry_days": 7,
        "download_timeout": 30,
        "page_load_timeout": 15,
        "implicit_wait": 10,
        "headless": False,
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
}

def setup_logging():
    """設定日誌系統"""
    LOG_DIR.mkdir(exist_ok=True)
    
    # 設定日誌格式
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 設定根日誌器
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # 清除現有處理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 檔案處理器
    file_handler = logging.FileHandler(
        LOG_DIR / f"otc_downloader_{datetime.now().strftime('%Y%m%d')}.log",
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 控制台處理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

class PerformanceMonitor:
    """效能監控器"""
    
    def __init__(self):
        self.metrics = {}
        
    @contextmanager
    def measure_time(self, operation_name: str):
        """測量操作時間的上下文管理器"""
        start_time = time.time()
        start_memory = psutil.virtual_memory().used / 1024 / 1024  # MB
        
        try:
            yield
        finally:
            end_time = time.time()
            end_memory = psutil.virtual_memory().used / 1024 / 1024  # MB
            
            duration = end_time - start_time
            memory_diff = end_memory - start_memory
            
            self.metrics[operation_name] = {
                "duration_seconds": round(duration, 2),
                "memory_change_mb": round(memory_diff, 2),
                "timestamp": datetime.now().isoformat()
            }
            
            logging.info(f"{operation_name} 完成 - 耗時: {duration:.2f}秒, 記憶體變化: {memory_diff:+.2f}MB")
    
    def get_summary(self) -> Dict[str, Any]:
        """取得效能摘要"""
        if not self.metrics:
            return {"message": "無效能資料"}
            
        total_time = sum(metric["duration_seconds"] for metric in self.metrics.values())
        return {
            "total_operations": len(self.metrics),
            "total_duration_seconds": round(total_time, 2),
            "operations": self.metrics
        }

    def save_report(self, filepath: Path):
        """儲存效能報告"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": self.get_summary(),
            "system_info": {
                "cpu_percent": psutil.cpu_percent(),
                "memory_percent": psutil.virtual_memory().percent
            }
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

class DataValidator:
    """資料驗證器"""
    
    def validate_dataframe(self, df: pd.DataFrame, file_type: str) -> Dict[str, any]:
        """驗證整個資料框"""
        results = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "statistics": {}
        }
        
        # 基本統計
        results["statistics"] = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "null_counts": df.isnull().sum().to_dict(),
            "unique_stocks": df['stock_id'].nunique() if 'stock_id' in df.columns else 0
        }
        
        # 股票代號驗證
        if 'stock_id' in df.columns:
            invalid_stock_ids = df[~df['stock_id'].str.match(r'^\d{4}$', na=False)]
            if not invalid_stock_ids.empty:
                results["errors"].append({
                    "type": "invalid_stock_id",
                    "count": len(invalid_stock_ids),
                    "samples": invalid_stock_ids['stock_id'].head(5).tolist()
                })
                results["is_valid"] = False
        
        # 重複資料檢查
        if 'stock_id' in df.columns:
            duplicates = df[df.duplicated(subset=['stock_id'])]
            if not duplicates.empty:
                results["warnings"].append({
                    "type": "duplicate_stock_id",
                    "count": len(duplicates),
                    "description": "發現重複的股票代號"
                })
        
        return results

class OTCDataDownloader:
    """OTC資料下載器類別"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.download_items = config.get("download_items", {})
        self.settings = config.get("settings", {})
        self.driver = None
        self.performance_monitor = PerformanceMonitor()
        
    def ensure_dir(self, path: Path) -> None:
        """確保目錄存在"""
        path.mkdir(parents=True, exist_ok=True)
        logging.info(f"確保目錄存在: {path}")
        
    def setup_chrome_driver(self) -> webdriver.Chrome:
        """設定 Chrome WebDriver"""
        options = Options()
        prefs = {
            "download.default_directory": str(DOWNLOAD_DIR),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        if self.settings.get('headless', False):
            options.add_argument('--headless')
            
        if 'user_agent' in self.settings:
            options.add_argument(f'--user-agent={self.settings["user_agent"]}')
            
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.implicitly_wait(self.settings.get('implicit_wait', 10))
            logging.info("Chrome WebDriver 初始化成功")
            return driver
        except Exception as e:
            logging.error(f"Chrome WebDriver 初始化失敗: {e}")
            raise
            
    def convert_date_to_roc(self, date_obj: datetime) -> str:
        """轉換為民國年格式"""
        roc_year = date_obj.year - 1911
        return f"{roc_year}/{date_obj.month:02d}/{date_obj.day:02d}"
        
    def wait_for_download(self, filename_pattern: str, timeout: int = None) -> Optional[Path]:
        """等待下載完成，返回檔案路徑"""
        if timeout is None:
            timeout = self.settings.get('download_timeout', 30)
            
        start_time = time.time()
        while time.time() - start_time < timeout:
            for filename in DOWNLOAD_DIR.iterdir():
                if filename_pattern in filename.name and not filename.name.endswith('.crdownload'):
                    logging.info(f"下載完成: {filename}")
                    return filename
            time.sleep(1)
        logging.warning(f"下載逾時: {filename_pattern}")
        return None
        
    def get_latest_trading_date(self) -> datetime:
        """取得最近的交易日"""
        tw_holidays = holidays.TW()
        today = datetime.today()
        max_days = self.settings.get('max_retry_days', 7)
        
        for _ in range(max_days):
            if today.weekday() < 5 and today.date() not in tw_holidays:
                break
            today -= timedelta(days=1)
        else:
            logging.warning(f"在過去 {max_days} 天內未找到交易日")
            
        logging.info(f"最近交易日: {today.strftime('%Y-%m-%d')}")
        return today
        
    def close_cookie_banner(self) -> None:
        """關閉 cookie 提示"""
        try:
            cookie_btn = WebDriverWait(self.driver, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".cookie-banner .btn-close"))
            )
            cookie_btn.click()
            time.sleep(1)
            logging.debug("Cookie banner 已關閉")
        except:
            logging.debug("未找到 cookie banner")
            
    def download_with_retry(self, name: str, config: Dict[str, Any], date_obj: datetime) -> bool:
        """帶重試機制的下載方法"""
        retry_count = config.get('retry_count', 3)
        
        with self.performance_monitor.measure_time(f"下載_{name}"):
            for attempt in range(retry_count):
                try:
                    if self.download_single_file(name, config, date_obj):
                        return True
                    logging.warning(f"{name} 第 {attempt + 1} 次嘗試失敗")
                except Exception as e:
                    logging.error(f"{name} 第 {attempt + 1} 次嘗試發生錯誤: {e}")
                    
                if attempt < retry_count - 1:
                    time.sleep(5)  # 重試前等待
                    
            logging.error(f"{name} 在 {retry_count} 次嘗試後仍然失敗")
            return False
            
    def download_single_file(self, name: str, config: Dict[str, Any], date_obj: datetime) -> bool:
        """下載單一檔案"""
        try:
            date_str = date_obj.strftime("%Y%m%d")
            roc_date = self.convert_date_to_roc(date_obj)
            logging.info(f"[處理] {name} - {config['name']}")

            self.driver.get(config['url'])
            time.sleep(5)

            # 關閉 cookie 提示
            self.close_cookie_banner()

            # 專門處理 daily_close_no1430
            if name == "daily_close_no1430":
                return self._handle_daily_close(date_str, roc_date)

            # 其他檔案的一般處理
            return self._handle_general_download(name, config, date_str, roc_date)

        except Exception as e:
            logging.error(f"{name} 整體處理錯誤：{e}")
            logging.error(traceback.format_exc())
            return False

    def _handle_daily_close(self, date_str: str, roc_date: str) -> bool:
        """處理每日收盤資料下載"""
        try:
            # 設定日期
            date_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='date'], input[type='text'].date"))
            )
            self.driver.execute_script(f"""
                var dateInput = arguments[0];
                dateInput.removeAttribute('readonly');
                dateInput.value = '{roc_date}';
                dateInput.dispatchEvent(new Event('change', {{ bubbles: true }}));
                dateInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
            """, date_input)
            logging.info(f"  設定日期：{roc_date}")
            time.sleep(2)

            # 選「所有證券」
            select_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "select.form-select, select[name='type'], select"))
            )
            select = Select(select_element)
            try:
                select.select_by_visible_text("所有證券")
                logging.info("  已選擇「所有證券」(用 visible text)")
            except:
                try:
                    select.select_by_value("AL")
                    logging.info("  已選擇「所有證券」(用 value='AL')")
                except:
                    self.driver.execute_script("""
                        var sel = arguments[0];
                        for(var i=0; i<sel.options.length; i++){
                            if(sel.options[i].text.includes('所有證券') || sel.options[i].value==='AL'){
                                sel.selectedIndex = i;
                                sel.dispatchEvent(new Event('change', { bubbles: true }));
                                break;
                            }
                        }
                    """, select_element)
                    logging.info("  已選擇「所有證券」(用 JavaScript)")
            time.sleep(2)

            # 點「另存 CSV」
            csv_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                    "//button[contains(text(), '另存CSV') or contains(text(), '另存 CSV')]"))
            )
            csv_btn.click()
            logging.info("  點擊「另存 CSV」")
            time.sleep(5)

            dl_file = self.wait_for_download(".csv", 20)
            if dl_file:
                new_name = f"{date_str}_daily_close_no1430.csv"
                new_path = RAW_DIR / new_name
                shutil.move(str(dl_file), str(new_path))
                logging.info(f"  [✅] 下載並移動為 → {new_path}")
                return True
            else:
                logging.error("  [❌] 下載逾時")
                return False

        except Exception as e:
            logging.error(f"  daily_close_no1430 處理失敗：{e}")
            return False

    def _handle_general_download(self, name: str, config: Dict[str, Any], date_str: str, roc_date: str) -> bool:
        """處理一般檔案下載"""
        try:
            # 年月設定（針對特定檔案）
            if name in ["highlight", "sbl", "exempted"]:
                try:
                    sel_year = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.NAME, "year"))
                    )
                    Select(sel_year).select_by_value(roc_date.split("/")[0])
                    sel_month = self.driver.find_element(By.NAME, "month")
                    Select(sel_month).select_by_value(roc_date.split("/")[1])
                    logging.info(f"  選擇年份 {roc_date.split('/')[0]}、月份 {roc_date.split('/')[1]}")
                    time.sleep(1)
                except Exception as e:
                    logging.warning(f"  年月下拉失敗：{e}")

            # 下拉選單設定
            if "select_element" in config:
                try:
                    sel_name = config["select_element"]["name"]
                    sel_val = config["select_element"]["value"]
                    self.driver.execute_script(f"""
                        var sel = document.querySelector('select[name="{sel_name}"]') ||
                                  document.querySelector('#{sel_name}');
                        if(sel){{
                            sel.value = '{sel_val}';
                            sel.dispatchEvent(new Event('change'));
                        }}
                    """)
                    logging.info(f"  設定下拉 {sel_name} = {sel_val}")
                    time.sleep(1)
                except Exception as e:
                    logging.warning(f"  下拉設定失敗：{e}")

            # 查詢按鈕
            if config.get("needs_query", False):
                try:
                    btns = self.driver.find_elements(By.CSS_SELECTOR, "button.btn-primary, input[type='submit'], button[type='submit']")
                    clicked = False
                    for b in btns:
                        if b.is_displayed() and b.is_enabled():
                            b.click()
                            logging.info("  點擊查詢")
                            clicked = True
                            time.sleep(8)
                            break
                    if not clicked:
                        logging.warning("  未找到可點擊的查詢按鈕，跳過")
                except Exception as e:
                    logging.warning(f"  查詢按鈕點擊失敗：{e}")

            # 等待表格載入
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, config["wait_element"]))
                )
                logging.info("  資料表格載入完成")
            except:
                logging.warning("  資料載入逾時，仍嘗試下載")

            # 執行下載
            return self._execute_download(name, config)

        except Exception as e:
            logging.error(f"  {name} 一般處理失敗：{e}")
            return False

    def _execute_download(self, name: str, config: Dict[str, Any]) -> bool:
        """執行實際下載"""
        # 方法1：尋找 CSV 下載按鈕
        try:
            self.driver.execute_script("""
                var btn = document.querySelector('.response[data-format="csv"]') ||
                          document.querySelector('.response[data-format="csv-u8"]') ||
                          document.querySelector('button[data-format="csv"]');
                if(btn){ btn.click(); return true; }
                return false;
            """)
            time.sleep(5)
            dl_file = self.wait_for_download(".csv", 20)
            if dl_file:
                return self._move_downloaded_file(dl_file, name)
        except Exception as e:
            logging.warning(f"  方法1 下載失敗：{e}")

        # 方法2：用文字搜尋下載連結
        texts = [
            config["download_text"], "下載CSV", "另存CSV", "下載 CSV", "另存 CSV",
            "下載 CSV 檔(UTF-8)", "下載 CSV 檔(BIG5)"
        ]
        for txt in texts:
            try:
                xpath = f"//a[contains(text(), '{txt}')] | //button[contains(text(), '{txt}')]"
                btn = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                self.driver.execute_script("arguments[0].click();", btn)
                logging.info(f"  點擊下載：『{txt}』")
                time.sleep(5)
                dl_file = self.wait_for_download(".csv", 20)
                if dl_file:
                    return self._move_downloaded_file(dl_file, name)
            except:
                continue

        logging.error("  [❌] 所有下載方法均失敗")
        return False

    def _move_downloaded_file(self, dl_file: Path, name: str) -> bool:
        """移動下載的檔案"""
        try:
            orig = dl_file.name
            if name == "investment_trust_buy":
                base, ext = os.path.splitext(orig)
                newf = f"{base}_buy{ext}"
            elif name == "investment_trust_sell":
                base, ext = os.path.splitext(orig)
                newf = f"{base}_sell{ext}"
            else:
                newf = orig
            
            new_path = RAW_DIR / newf
            shutil.move(str(dl_file), str(new_path))
            logging.info(f"  [✅] 下載成功 → {new_path}")
            return True
        except Exception as e:
            logging.error(f"  移動檔案失敗：{e}")
            return False
        
    def download_all(self) -> int:
        """下載所有資料"""
        self.ensure_dir(RAW_DIR)
        self.driver = self.setup_chrome_driver()
        
        try:
            latest_date = self.get_latest_trading_date()
            success_count = 0
            
            logging.info(f"\n嘗試下載日期: {latest_date.strftime('%Y-%m-%d')}（民國 {self.convert_date_to_roc(latest_date)}）")
            logging.info("=" * 60)
            
            for name, cfg in self.download_items.items():
                if self.download_with_retry(name, cfg, latest_date):
                    success_count += 1
                logging.info("-" * 60)
                time.sleep(2)
                
            logging.info(f"\n總計：成功下載 {success_count}/{len(self.download_items)} 檔")
            return success_count
            
        finally:
            if self.driver:
                self.driver.quit()
                logging.info("Chrome WebDriver 已關閉")

class OTCDataCleaner:
    """OTC資料清洗器類別"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.download_items = config.get("download_items", {})
        self.validator = DataValidator()
        self.performance_monitor = PerformanceMonitor()
        
    def ensure_dir(self, path: Path) -> None:
        """確保目錄存在"""
        path.mkdir(parents=True, exist_ok=True)
        
    def clean_numeric_column(self, series: pd.Series) -> pd.Series:
        """清理數值欄位：移除逗號並轉換為數值"""
        return (series.astype(str)
                     .str.replace(",", "")
                     .str.replace("", "0")
                     .pipe(lambda x: pd.to_numeric(x, errors="coerce"))
                     .fillna(0)
                     .astype(int))
                     
    def extract_stock_id(self, series: pd.Series) -> pd.Series:
        """提取4位數股票代號"""
        return series.astype(str).str.extract(r"(\d{4})")[0]
        
    def read_csv_with_encoding(self, file_path: Path, skiprows: int = 0) -> Optional[pd.DataFrame]:
        """嘗試多種編碼讀取CSV"""
        encodings = ["cp950", "big5", "utf-8-sig", "utf-8"]
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, skiprows=skiprows, dtype=str, low_memory=False)
                logging.debug(f"成功使用 {encoding} 編碼讀取 {file_path.name}")
                return df
            except Exception as e:
                logging.debug(f"使用 {encoding} 編碼讀取失敗：{e}")
                continue
                
        logging.error(f"無法讀取檔案 {file_path.name}")
        return None
        
    def get_file_type_and_config(self, filename: str) -> tuple:
        """根據檔案名稱判斷類型和取得設定"""
        filename_lower = filename.lower()
        
        # 檔案類型對應
        file_patterns = {
            "daily_close_no1430": ("daily_close_no1430", 3),
            "bigd_": ("institutional_detail", 1), 
            "brktop1_": ("sec_trading", 2),
            "daytraderpt_": ("day_trading", 5),
            "margratio_": ("highlight", 2),
            "owz66u_": ("sbl", 2),
            "rsta3106_": ("margin_transactions", 2)
        }
        
        for pattern, (config_key, skiprows) in file_patterns.items():
            if pattern in filename_lower:
                return config_key, skiprows
                
        # 特殊處理投信買賣超
        if filename_lower.startswith("sit_"):
            if "_buy" in filename_lower:
                return "investment_trust_buy", 1
            elif "_sell" in filename_lower:
                return "investment_trust_sell", 1
                
        return None, 0
        
    def clean_single_file(self, file_path: Path) -> bool:
        """清理單一檔案"""
        filename = file_path.name
        logging.info(f"\n處理：{filename}")
        
        file_type, skiprows = self.get_file_type_and_config(filename)
        
        if file_type is None:
            logging.warning("  [❌] 未匹配清洗規則，跳過")
            return False
            
        # 讀取檔案
        df = self.read_csv_with_encoding(file_path, skiprows)
        if df is None:
            return False
            
        # 清理資料
        df.columns = df.columns.str.strip()
        df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
        
        if len(df) == 0:
            logging.warning(f"  [❌] 檔案 {filename} 清理後無資料")
            return False
            
        logging.info(f"  欄位：{list(df.columns)}")
        
        try:
            clean_df = self._clean_by_type(df, file_type, filename)
            if clean_df is None or len(clean_df) == 0:
                logging.error(f"  [❌] 檔案 {filename} 清理失敗")
                return False
                
            # 資料驗證
            validation_result = self.validator.validate_dataframe(clean_df, file_type)
            if not validation_result["is_valid"]:
                logging.warning(f"  [⚠️] 資料驗證發現問題：{validation_result['errors']}")
                
            # 儲存清理後的檔案
            output_path = CLEAN_DIR / filename
            clean_df.to_csv(output_path, index=False, encoding="utf-8-sig")
            logging.info(f"  [✅] 清洗完成: {filename} ({len(clean_df)} 行)")
            return True
            
        except Exception as e:
            logging.error(f"  [❌] 清洗失敗：{e}")
            logging.error(traceback.format_exc())
            return False
            
    def _clean_by_type(self, df: pd.DataFrame, file_type: str, filename: str) -> Optional[pd.DataFrame]:
        """根據檔案類型進行清理"""
        
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
        elif "investment_trust" in file_type or filename.lower().startswith("sit_"):
            return self._clean_investment_trust(df)
        else:
            logging.warning(f"  未知檔案類型：{file_type}")
            return None
            
    def _clean_daily_close(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理每日收盤資料"""
        cols = list(df.columns)
        
        # 尋找對應欄位
        code_col = next((c for c in cols if "代號" in c or "代碼" in c), None)
        name_col = next((c for c in cols if "名稱" in c), None)
        close_col = next((c for c in cols if "收盤" in c and "收盤" == c[:2]), None)
        
        if not all([code_col, name_col, close_col]):
            logging.error("  缺少必要欄位")
            return None
            
        # 建立欄位對應
        column_mapping = {
            code_col: "stock_id",
            name_col: "name",
            close_col: "close"
        }
        
        # 可選欄位
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
                
        # 建立清理後的DataFrame
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        clean_df = df[available_cols].rename(columns=column_mapping).copy()
        
        # 提取股票代號
        clean_df["stock_id"] = self.extract_stock_id(clean_df["stock_id"])
        clean_df = clean_df.dropna(subset=["stock_id"])
        
        # 清理數值欄位
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
            
        return clean_df.sort_values("stock_id").reset_index(drop=True)
        
    def _clean_institutional_detail(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理三大法人買賣明細"""
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
        
        # 三大法人買賣超欄位
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
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
            
        return clean_df.sort_values("stock_id").reset_index(drop=True)
        
    def _clean_sec_trading(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理券商交易統計"""
        cols = list(df.columns)
        
        # 只取前5欄
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
        
        # 清理數值欄位
        clean_df["amount_thousands"] = self.clean_numeric_column(clean_df["amount_thousands"])
        
        return clean_df.sort_values("broker").reset_index(drop=True)
        
    def _clean_day_trading(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理當沖交易統計"""
        cols = list(df.columns)
        
        code_col = next((c for c in cols if "證券代號" in c), None)
        name_col = next((c for c in cols if "證券名稱" in c), None)
        
        if not all([code_col, name_col]):
            logging.error("  缺少必要欄位")
            return None
            
        column_mapping = {
            code_col: "stock_id",
            name_col: "name"
        }
        
        # 當沖相關欄位
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
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name", "flag"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
            
        return clean_df.sort_values("stock_id").reset_index(drop=True)
        
    def _clean_highlight(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理融資融券餘額概況"""
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
        
        # 融資融券相關欄位
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
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
            
        return clean_df.sort_values("stock_id").reset_index(drop=True)
        
    def _clean_sbl(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理借券賣出餘額"""
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
        
        # SBL相關欄位
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
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name", "remark"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
            
        return clean_df.sort_values("stock_id").reset_index(drop=True)
        
    def _clean_margin_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理融資融券餘額"""
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
        
        # 融資融券相關欄位
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
        
        numeric_cols = [col for col in clean_df.columns if col not in ["stock_id", "name", "remark"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
            
        return clean_df.sort_values("stock_id").reset_index(drop=True)
        
    def _clean_investment_trust(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理投信買賣超"""
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
        
        # 投信買賣相關欄位
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
        
        numeric_cols = [col for col in clean_df.columns if col not in ["rank", "stock_id", "name"]]
        for col in numeric_cols:
            clean_df[col] = self.clean_numeric_column(clean_df[col])
            
        return clean_df.sort_values("stock_id").reset_index(drop=True)
        
    def clean_all_files(self) -> Dict[str, int]:
        """清理所有檔案"""
        self.ensure_dir(CLEAN_DIR)
        results = {"success": 0, "failed": 0, "failed_files": []}
        
        with self.performance_monitor.measure_time("資料清洗"):
            for csv_file in RAW_DIR.glob("*.csv"):
                try:
                    if self.clean_single_file(csv_file):
                        results["success"] += 1
                        logging.info(f"成功清理: {csv_file.name}")
                    else:
                        results["failed"] += 1
                        results["failed_files"].append(csv_file.name)
                        
                except Exception as e:
                    logging.error(f"清理檔案 {csv_file.name} 時發生錯誤: {e}")
                    results["failed"] += 1
                    results["failed_files"].append(csv_file.name)
                    
        return results

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

def save_config(config: Dict[str, Any]):
    """儲存設定檔"""
    config_file = BASE_DIR / "otc_config.json"
    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        logging.info(f"設定檔已儲存: {config_file}")
    except Exception as e:
        logging.error(f"設定檔儲存失敗: {e}")

def main():
    """主要執行函數"""
    # 設定日誌
    setup_logging()
    logging.info("=== OTC 櫃買中心資料下載 + 清洗系統開始 ===")
    
    # 載入設定
    config = load_config()
    
    # 建立效能監控器
    performance_monitor = PerformanceMonitor()
    
    try:
        # 初始化下載器
        downloader = OTCDataDownloader(config)
        
        # 下載資料
        with performance_monitor.measure_time("總下載時間"):
            success_count = downloader.download_all()
            
        logging.info(f"下載階段完成: 成功 {success_count}/{len(config['download_items'])} 檔")
        
        # 檢查是否有下載的檔案
        raw_files = list(RAW_DIR.glob("*.csv"))
        if not raw_files:
            logging.warning("RAW_DIR 中沒有任何 CSV 檔案，無法進行清洗")
            return
            
        logging.info(f"找到 {len(raw_files)} 個 CSV 檔案，開始清洗...")
        
        # 初始化清洗器
        cleaner = OTCDataCleaner(config)
        
        # 清洗資料
        clean_results = cleaner.clean_all_files()
        
        logging.info(f"清洗階段完成: 成功 {clean_results['success']} 檔, 失敗 {clean_results['failed']} 檔")
        if clean_results['failed_files']:
            logging.warning(f"失敗檔案: {clean_results['failed_files']}")
            
        # 儲存效能報告
        performance_report_path = LOG_DIR / f"performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        performance_monitor.save_report(performance_report_path)
        
        # 顯示摘要
        summary = performance_monitor.get_summary()
        logging.info(f"執行摘要: 總耗時 {summary['total_duration_seconds']} 秒")
        
    except Exception as e:
        logging.error(f"程式執行過程中發生錯誤: {e}")
        logging.error(traceback.format_exc())
        raise
        
    logging.info("=== 程式執行完成 ===")

if __name__ == "__main__":
    main()'''
CONFIG = json.loads(CONFIG_JSON)
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定目錄
BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "otc_raw"
DOWNLOAD_DIR = Path.home() / "Downloads"
CLEAN_DIR = BASE_DIR / "otc_cleaned"
LOG_DIR = BASE_DIR / "logs"

# 預設設定
DEFAULT_CONFIG = {
    "download_items": {
        "daily_close_no1430": {
            "name": "上櫃股票每日收盤行情(不含定價)",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/mi-pricing.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": True,
            "retry_count": 3,
            "skiprows": 3
        },
        "margin_transactions": {
            "name": "上櫃股票融資融券餘額",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/transactions.html",
            "wait_element": "table.table-default",
            "download_text": "下載 CSV 檔(UTF-8)",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 2
        },
        "institutional_detail": {
            "name": "三大法人買賣明細資訊",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/detail/day.html",
            "select_element": {"name": "sect", "value": "AL"},
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 1
        },
        "day_trading": {
            "name": "現股當沖交易統計資訊",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/day-trading/statistics/day.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 5
        },
        "sec_trading": {
            "name": "各券商當日營業金額統計表",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/sec-trading.html",
            "wait_element": "table.table-default",
            "download_text": "下載 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 2
        },
        "investment_trust_buy": {
            "name": "投信買賣超彙總表（買超）",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/domestic-inst/day.html",
            "select_element": {"name": "searchType", "value": "buy"},
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 1
        },
        "investment_trust_sell": {
            "name": "投信買賣超彙總表（賣超）",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/domestic-inst/day.html",
            "select_element": {"name": "searchType", "value": "sell"},
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 1
        },
        "highlight": {
            "name": "上櫃股票信用交易融資融券餘額概況表",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/highlight.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 2
        },
        "sbl": {
            "name": "信用額度總量管制餘額表",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/sbl.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 2
        },
        "exempted": {
            "name": "平盤下得融(借)券賣出之證券名單",
            "url": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/exempted.html",
            "wait_element": "table.table-default",
            "download_text": "另存 CSV",
            "needs_query": False,
            "retry_count": 3,
            "skiprows": 2
        }
    },
    "settings": {
        "max_retry_days": 7,
        "download_timeout": 30,
        "page_load_timeout": 15,
        "implicit_wait": 10,
        "headless": False,
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
}

def setup_logging():
    """設定日誌系統"""
    LOG_DIR.mkdir(exist_ok=True)
    
    # 設定日誌格式
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 設定根日誌器
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # 清除現有處理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 檔案處理器
    file_handler = logging.FileHandler(
        LOG_DIR / f"otc_downloader_{datetime.now().strftime('%Y%m%d')}.log",
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 控制台處理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

class PerformanceMonitor:
    """效能監控器"""
    
    def __init__(self):
        self.metrics = {}
        
    @contextmanager
    def measure_time(self, operation_name: str):
        """測量操作時間的上下文管理器"""
        start_time = time.time()
        start_memory = psutil.virtual_memory().used / 1024 / 1024  # MB
        
        try:
            yield
        finally:
            end_time = time.time()
            end_memory = psutil.virtual_memory().used / 1024 / 1024  # MB
            
            duration = end_time - start_time
            memory_diff = end_memory - start_memory
            
            self.metrics[operation_name] = {
                "duration_seconds": round(duration, 2),
                "memory_change_mb": round(memory_diff, 2),
                "timestamp": datetime.now().isoformat()
            }
            
            logging.info(f"{operation_name} 完成 - 耗時: {duration:.2f}秒, 記憶體變化: {memory_diff:+.2f}MB")
    
    def get_summary(self) -> Dict[str, Any]:
        """取得效能摘要"""
        if not self.metrics:
            return {"message": "無效能資料"}
            
        total_time = sum(metric["duration_seconds"] for metric in self.metrics.values())
        return {
            "total_operations": len(self.metrics),
            "total_duration_seconds": round(total_time, 2),
            "operations": self.metrics
        }

    def save_report(self, filepath: Path):
        """儲存效能報告"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": self.get_summary(),
            "system_info": {
                "cpu_percent": psutil.cpu_percent(),
                "memory_percent": psutil.virtual_memory().percent
            }
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

class DataValidator:
    """資料驗證器"""
    
    def validate_dataframe(self, df: pd.DataFrame, file_type: str) -> Dict[str, any]:
        """驗證整個資料框"""
        results = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "statistics": {}
        }
        
        # 基本統計
        results["statistics"] = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "null_counts": df.isnull().sum().to_dict(),
            "unique_stocks": df['stock_id'].nunique() if 'stock_id' in df.columns else 0
        }
        
        # 股票代號驗證
        if 'stock_id' in df.columns:
            invalid_stock_ids = df[~df['stock_id'].str.match(r'^\d{4}$', na=False)]
            if not invalid_stock_ids.empty:
                results["errors"].append({
                    "type": "invalid_stock_id",
                    "count": len(invalid_stock_ids),
                    "samples": invalid_stock_ids['stock_id'].head(5).tolist()
                })
                results["is_valid"] = False
        
        # 重複資料檢查
        if 'stock_id' in df.columns:
            duplicates = df[df.duplicated(subset=['stock_id'])]
            if not duplicates.empty:
                results["warnings"].append({
                    "type": "duplicate_stock_id",
                    "count": len(duplicates),
                    "description": "發現重複的股票代號"
                })
        
        return results

class OTCDataDownloader:
    """OTC資料下載器類別"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.download_items = config.get("download_items", {})
        self.settings = config.get("settings", {})
        self.driver = None
        self.performance_monitor = PerformanceMonitor()
        
    def ensure_dir(self, path: Path) -> None:
        """確保目錄存在"""
        path.mkdir(parents=True, exist_ok=True)
        logging.info(f"確保目錄存在: {path}")
        
    def setup_chrome_driver(self) -> webdriver.Chrome:
        """設定 Chrome WebDriver"""
        options = Options()
        prefs = {
            "download.default_directory": str(DOWNLOAD_DIR),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        if self.settings.get('headless', False):
            options.add_argument('--headless')
            
        if 'user_agent' in self.settings:
            options.add_argument(f'--user-agent={self.settings["user_agent"]}')
            
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.implicitly_wait(self.settings.get('implicit_wait', 10))
            logging.info("Chrome WebDriver 初始化成功")
            return driver
        except Exception as e:
            logging.error(f"Chrome WebDriver 初始化失敗: {e}")
            raise
            
    def convert_date_to_roc(self, date_obj: datetime) -> str:
        """轉換為民國年格式"""
        roc_year = date_obj.year - 1911
        return f"{roc_year}/{date_obj.month:02d}/{date_obj.day:02d}"
        
    def wait_for_download(self, filename_pattern: str, timeout: int = None) -> Optional[Path]:
        """等待下載完成，返回檔案路徑"""
        if timeout is None:
            timeout = self.settings.get('download_timeout', 30)
            
        start_time = time.time()
        while time.time() - start_time < timeout:
            for filename in DOWNLOAD_DIR.iterdir():
                if filename_pattern in filename.name and not filename.name.endswith('.crdownload'):
                    logging.info(f"下載完成: {filename}")
                    return filename
            time.sleep(1)
        logging.warning(f"下載逾時: {filename_pattern}")
        return None
        
    def get_latest_trading_date(self) -> datetime:
        """取得最近的交易日"""
        tw_holidays = holidays.TW()
        today = datetime.today()
        max_days = self.settings.get('max_retry_days', 7)
        
        for _ in range(max_days):
            if today.weekday() < 5 and today.date() not in tw_holidays:
                break
            today -= timedelta(days=1)
        else:
            logging.warning(f"在過去 {max_days} 天內未找到交易日")
            
        logging.info(f"最近交易日: {today.strftime('%Y-%m-%d')}")
        return today
        
    def close_cookie_banner(self) -> None:
        """關閉 cookie 提示"""
        try:
            cookie_btn = WebDriverWait(self.driver, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".cookie-banner .btn-close"))
            )
            cookie_btn.click()
            time.sleep(1)
            logging.debug("Cookie banner 已關閉")
        except:
            logging.debug("未找到 cookie banner")
            
    def download_with_retry(self, name: str, config: Dict[str, Any], date_obj: datetime) -> bool:
        """帶重試機制的下載方法"""
        retry_count = config.get('retry_count', 3)
        
        with self.performance_monitor.measure_time(f"下載_{name}"):
            for attempt in range(retry_count):
                try:
                    if self.download_single_file(name, config, date_obj):
                        return True
                    logging.warning(f"{name} 第 {attempt + 1} 次嘗試失敗")
                except Exception as e:
                    logging.error(f"{name} 第 {attempt + 1} 次嘗試發生錯誤: {e}")
                    
                if attempt < retry_count - 1:
                    time.sleep(5)  # 重試前等待
                    
            logging.error(f"{name} 在 {retry_count} 次嘗試後仍然失敗")
            return False
            
    def download_single_file(self, name: str, config: Dict[str, Any], date_obj: datetime) -> bool:
        """下載單一檔案"""
        try:
            date_str = date_obj.strftime("%Y%m%d")
            roc_date = self.convert_date_to_roc(date_obj)
            logging.info(f"[處理] {name} - {config['name']}")

            self.driver.get(config['url'])
            time.sleep(5)

            # 關閉 cookie 提示
            self.close_cookie_banner()

            # 專門處理 daily_close_no1430
            if name == "daily_close_no1430":
                return self._handle_daily_close(date_str, roc_date)

            # 其他檔案的一般處理
            return self._handle_general_download(name, config, date_str, roc_date)

        except Exception as e:
            logging.error(f"{name} 整體處理錯誤：{e}")
            logging.error(traceback.format_exc())
            return False

    def _handle_daily_close(self, date_str: str, roc_date: str) -> bool:
        """處理每日收盤資料下載"""
        try:
            # 設定日期
            date_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='date'], input[type='text'].date"))
            )
            self.driver.execute_script(f"""
                var dateInput = arguments[0];
                dateInput.removeAttribute('readonly');
                dateInput.value = '{roc_date}';
                dateInput.dispatchEvent(new Event('change', {{ bubbles: true }}));
                dateInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
            """, date_input)
            logging.info(f"  設定日期：{roc_date}")
            time.sleep(2)

            # 選「所有證券」
            select_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "select.form-select, select[name='type'], select"))
            )
            select = Select(select_element)
            try:
                select.select_by_visible_text("所有證券")
                logging.info("  已選擇「所有證券」(用 visible text)")
            except:
                try:
                    select.select_by_value("AL")
                    logging.info("  已選擇「所有證券」(用 value='AL')")
                except:
                    self.driver.execute_script("""
                        var sel = arguments[0];
                        for(var i=0; i<sel.options.length; i++){
                            if(sel.options[i].text.includes('所有證券') || sel.options[i].value==='AL'){
                                sel.selectedIndex = i;
                                sel.dispatchEvent(new Event('change', { bubbles: true }));
                                break;
                            }
                        }
                    """, select_element)
                    logging.info("  已選擇「所有證券」(用 JavaScript)")
            time.sleep(2)

            # 點「另存 CSV」
            csv_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                    "//button[contains(text(), '另存CSV') or contains(text(), '另存 CSV')]"))
            )
            csv_btn.click()
            logging.info("  點擊「另存 CSV」")
            time.sleep(5)

            dl_file = self.wait_for_download(".csv", 20)
            if dl_file:
                new_name = f"{date_str}_daily_close_no1430.csv"
                new_path = RAW_DIR / new_name
                shutil.move(str(dl_file), str(new_path))
                logging.info(f"  [✅] 下載並移動為 → {new_path}")
                return True
            else:
                logging.error("  [❌] 下載逾時")
                return False

        except Exception as e:
            logging.error(f"  daily_close_no1430 處理失敗：{e}")
            return False

    def _handle_general_download(self, name: str, config: Dict[str, Any], date_str: str, roc_date: str) -> bool:
        """處理一般檔案下載"""
        try:
            # 年月設定（針對特定檔案）
            if name in ["highlight", "sbl", "exempted"]:
                try:
                    sel_year = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.NAME, "year"))
                    )
                    Select(sel_year).select_by_value(roc_date.split("/")[0])
                    sel_month = self.driver.find_element(By.NAME, "month")
                    Select(sel_month).select_by_value(roc_date.split("/")[1])
                    logging.info(f"  選擇年份 {roc_date.split('/')[0]}、月份 {roc_date.split('/')[1]}")
                    time.sleep(1)
                except Exception as e:
                    logging.warning(f"  年月下拉失敗：{e}")

            # 下拉選單設定
            if "select_element" in config:
                try:
                    sel_name = config["select_element"]["name"]
                    sel_val = config["select_element"]["value"]
                    self.driver.execute_script(f"""
                        var sel = document.querySelector('select[name="{sel_name}"]') ||
                                  document.querySelector('#{sel_name}');
                        if(sel){{
                            sel.value = '{sel_val}';
                            sel.dispatchEvent(new Event('change'));
                        }}
                    """)
                    logging.info(f"  設定下拉 {sel_name} = {sel_val}")
                    time.sleep(1)
                except Exception as e:
                    logging.warning(f"  下拉設定失敗：{e}")

            # 查詢按鈕
            if config.get("needs_query", False):
                try:
                    btns = self.driver.find_elements(By.CSS_SELECTOR, "button.btn-primary, input[type='submit'], button[type='submit']")
                    clicked = False
                    for b in btns:
                        if b.is_displayed() and b.is_enabled():
                            b.click()
                            logging.info("  點擊查詢")
                            clicked = True
                            time.sleep(8)
                            break
                    if not clicked:
                        logging.warning("  未找到可點擊的查詢按鈕，跳過")
                except Exception as e:
                    logging.warning(f"  查詢按鈕點擊失敗：{e}")

            # 等待表格載入
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, config["wait_element"]))
                )
                logging.info("  資料表格載入完成")
            except:
                logging.warning("  資料載入逾時，仍嘗試下載")

            # 執行下載
            return self._execute_download(name, config)

        except Exception as e:
            logging.error(f"  {name} 一般處理失敗：{e}")
            return False

    def _execute_download(self, name: str, config: Dict[str, Any]) -> bool:
        """執行實際下載"""
        # 方法1：尋找 CSV 下載按鈕
        try:
            self.driver.execute_script("""
                var btn = document.querySelector('.response[data-format="csv"]') ||
                          document.querySelector('.response[data-format="csv-u8"]') ||
                          document.querySelector('button[data-format="csv"]');
                if(btn){ btn.click(); return true; }
                return false;
            """)
            time.sleep(5)
            dl_file = self.wait_for_download(".csv", 20)
            if dl_file:
                return self._move_downloaded_file(dl_file, name)
        except Exception as e:
            logging.warning(f"  方法1 下載失敗：{e}")

        # 方法2：用文字搜尋下載連結
        texts = [
            config["download_text"], "下載CSV", "另存CSV", "下載 CSV", "另存 CSV",
            "下載 CSV 檔(UTF-8)", "下載 CSV 檔(BIG5)"
        ]
        for txt in texts:
            try:
                xpath = f"//a[contains(text(), '{txt}')] | //button[contains(text(), '{txt}')]"
                btn = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                self.driver.execute_script("arguments[0].click();", btn)
                logging.info(f"  點擊下載：『{txt}』")
                time.sleep(5)
                dl_file = self.wait_for_download(".csv", 20)
                if dl_file:
                    return self._move_downloaded_file(dl_file, name)
            except:
                continue

        logging.error("  [❌] 所有下載方法均失敗")
        return False

    def _move_downloaded_file(self, dl_file: Path, name: str) -> bool:
        """移動下載的檔案"""
        try:
            orig = dl_file.name
            if name == "investment_trust_buy":
                base, ext = os.path.splitext(orig)
                newf = f"{base}_buy{ext}"
            elif name == "investment_trust_sell":
                base, ext = os.path.splitext(orig)
                newf = f"{base}_sell{ext}"
            else:
                newf = orig
            
            new_path = RAW_DIR / newf
            shutil.move(str(dl_file), str(new_path))
            logging.info(f"  [✅] 下載成功 → {new_path}")
            return True
        except Exception as e:
            logging.error(f"  移動檔案失敗：{e}")
            return False
        
    def download_all(self) -> int:
        """下載所有資料"""
        self.ensure_dir(RAW_DIR)
        self.driver = self.setup_chrome_driver()
        
        try:
            latest_date = self.get_latest_trading_date()
            success_count = 0
            
            logging.info(f"\n嘗試下載日期: {latest_date.strftime('%Y-%m-%d')}（民國 {self.convert_date_to_roc(latest_date)}）")
            logging.info("=" * 60)
            
            for name, cfg in self.download_items.items():
                if self.download_with_retry(name, cfg, latest_date):
                    success_count += 1
                logging.info("-" * 60)
                time.sleep(2)
                
            logging.info(f"\n總計：成功下載 {success_count}/{len(self.download_items)} 檔")
            return success_count
            
        finally:
            if self.driver:
                self.driver.quit()
                logging.info("Chrome WebDriver 已關閉")

class OTCDataCleaner:
    """OTC資料清洗器類別"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.download_items = config.get("download_items", {})
        self.validator = DataValidator()
        self.performance_monitor = PerformanceMonitor()
        
    def ensure_dir(self, path: Path) -> None:
        """確保目錄存在"""
        path.mkdir(parents=True, exist_ok=True)
        
    def clean_numeric_column(self, series: pd.Series) -> pd.Series:
        """清理數值欄位：移除逗號並轉換為數值"""
        return (series.astype(str)
                     .str.replace(",", "")
                     .str.replace("", "0")
                     .pipe(lambda x: pd.to_numeric(x, errors="coerce"))
                     .fillna(0)
                     .astype(int))
                     
    def extract_stock_id(self, series: pd.Series) -> pd.Series:
        """提取4位數股票代號"""
        return series.astype(str).str.extract(r"(\d{4})")[0]
        
    def read_csv_with_encoding(self, file_path: Path, skiprows: int = 0) -> Optional[pd.DataFrame]:
        """嘗試多種編碼讀取CSV"""
        encodings = ["cp950", "big5", "utf-8-sig", "utf-8"]
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, skiprows=skiprows, dtype=str, low_memory=False)
                logging.debug(f"成功使用 {encoding} 編碼讀取 {file_path.name}")
