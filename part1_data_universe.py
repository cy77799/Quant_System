"""
長線量化系統 - Part 1: 數據層 + Universe 篩選
修正版（處理 yfinance 數據結構）
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================
# 1) 配置參數
# ============================================

CONFIG = {
    # Universe 篩選標準
    'min_market_cap': 2_000_000_000,  # $2B
    'min_avg_volume': 10_000_000,     # $10M
    'min_price': 5.0,                 # $5
    
    # 質量篩選
    'min_roe': -50,                   # ROE 下限（%）
    'max_debt_ratio': 500,            # 負債比上限（%）
    'ipo_exclusion_months': 12,       # 排除 12 個月內 IPO
    
    # 數據期間
    'lookback_years': 10,             # 回測 10 年
    'start_date': '2015-01-01',
    'end_date': '2025-12-31',
    
    # 數據質量
    'max_missing_ratio': 0.10,        # 最多 10% 缺失數據（放寬）
}

# ============================================
# 2) 數據下載模組
# ============================================

class DataDownloader:
    """下載並清理股票數據"""
    
    def __init__(self, config):
        self.config = config
        
    def get_sp500_tickers(self):
        """獲取股票列表（內置主要美股）"""
        print("📥 載入股票列表...")
        
        # 200+ 隻主要美股（跨行業）
        tickers = [
            # 科技 (Technology)
            'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'GOOG', 'META', 'AVGO', 'TSLA', 
            'ADBE', 'CRM', 'ORCL', 'ACN', 'CSCO', 'AMD', 'IBM', 'INTU', 
            'NOW', 'QCOM', 'TXN', 'AMAT', 'MU', 'ADI', 'LRCX', 'KLAC',
            'INTC', 'PYPL', 'ABNB', 'SNOW', 'PANW', 'CRWD', 'FTNT', 'NET',
            'TEAM', 'WDAY', 'DDOG', 'ZS', 'OKTA', 'SPLK', 'MRVL', 'SNPS',
            
            # 金融 (Financials)
            'BRK-B', 'JPM', 'V', 'MA', 'BAC', 'WFC', 'GS', 'MS', 'SPGI', 
            'AXP', 'BLK', 'C', 'SCHW', 'CB', 'MMC', 'PGR', 'AON', 'ICE',
            'CME', 'MCO', 'USB', 'TFC', 'PNC', 'COF', 'AIG', 'MET', 'PRU',
            'ALL', 'TRV', 'AFL', 'HIG', 'WTW', 'BRO', 'AJG', 'RJF',
            
            # 醫療保健 (Healthcare)
            'UNH', 'JNJ', 'LLY', 'ABBV', 'MRK', 'TMO', 'ABT', 'DHR', 'PFE',
            'AMGN', 'ISRG', 'BMY', 'CVS', 'CI', 'ELV', 'GILD', 'VRTX', 'HUM',
            'ZTS', 'REGN', 'MRNA', 'IDXX', 'DXCM', 'BDX', 'BSX', 'SYK', 'MDT',
            'EW', 'RMD', 'ALGN', 'HOLX', 'A', 'BAX', 'ILMN', 'BIIB', 'IQV',
            
            # 消費品 (Consumer Discretionary & Staples)
            'AMZN', 'WMT', 'COST', 'HD', 'MCD', 'PG', 'KO', 'PEP', 'NKE',
            'SBUX', 'TGT', 'LOW', 'TJX', 'CMG', 'BKNG', 'MAR', 'DHI', 'YUM',
            'LULU', 'ROST', 'DG', 'DLTR', 'ULTA', 'BBY', 'ORLY', 'AZO',
            'CL', 'KMB', 'GIS', 'K', 'HSY', 'MKC', 'CHD', 'CLX', 'CAG',
            
            # 工業 (Industrials)
            'CAT', 'BA', 'UNP', 'HON', 'UPS', 'RTX', 'LMT', 'DE', 'GE',
            'MMM', 'GD', 'NOC', 'ETN', 'ITW', 'PH', 'EMR', 'FDX', 'NSC',
            'CSX', 'WM', 'RSG', 'CARR', 'OTIS', 'PCAR', 'IR', 'FAST',
            'ROK', 'DOV', 'XYL', 'VRSK', 'IEX', 'FTV', 'CPRT', 'ODFL',
            
            # 能源 (Energy)
            'XOM', 'CVX', 'COP', 'SLB', 'EOG', 'MPC', 'PSX', 'VLO', 'OXY',
            'WMB', 'KMI', 'HAL', 'BKR', 'DVN', 'FANG', 'MRO', 'APA',
            'OKE', 'TRGP', 'LNG', 'EQT', 'CTRA', 'TPL', 'PR', 'CVE',
            
            # 通訊服務 (Communication Services)
            'NFLX', 'DIS', 'CMCSA', 'T', 'VZ', 'TMUS', 'CHTR', 'EA', 'TTWO',
            'NWSA', 'FOXA', 'OMC', 'LYV', 'MTCH', 'WBD',
            
            # 公用事業 (Utilities)
            'NEE', 'DUK', 'SO', 'D', 'AEP', 'EXC', 'SRE', 'PEG', 'XEL',
            'ED', 'WEC', 'ES', 'AWK', 'DTE', 'PPL', 'FE', 'CMS', 'AEE',
            'ATO', 'CNP', 'NI', 'LNT', 'EVRG', 'PNW', 'NWE', 'OGE',
            
            # 房地產 (Real Estate)
            'AMT', 'PLD', 'CCI', 'EQIX', 'PSA', 'WELL', 'SPG', 'O', 'DLR',
            'SBAC', 'VICI', 'AVB', 'EQR', 'INVH', 'MAA', 'ESS', 'VTR',
            'ARE', 'BXP', 'KIM', 'REG', 'UDR', 'CPT', 'HST', 'SLG',
            
            # 材料 (Materials)
            'LIN', 'APD', 'SHW', 'ECL', 'NEM', 'FCX', 'CTVA', 'DD', 'DOW',
            'NUE', 'VMC', 'MLM', 'PPG', 'ALB', 'BALL', 'AVY', 'CE', 'IP',
            'EMN', 'CF', 'FMC', 'MOS', 'IFF', 'LYB', 'SEE', 'WRK',
        ]
        
        print(f"✅ 獲取 {len(tickers)} 隻股票")
        return tickers
    
    def download_price_data(self, tickers):
        """下載價格數據"""
        print(f"\n📥 下載價格數據 ({self.config['start_date']} 至 {self.config['end_date']})...")
        print(f"   下載 {len(tickers)} 隻股票...")
        
        data = yf.download(
            tickers,
            start=self.config['start_date'],
            end=self.config['end_date'],
            group_by='ticker',
            auto_adjust=True,
            threads=True,
            progress=False
        )
        
        print(f"✅ 下載完成")
        return data
    
    def download_fundamental_data(self, tickers):
        """下載基本面數據"""
        print(f"\n📥 下載基本面數據...")
        
        fundamentals = []
        total = len(tickers)
        
        for i, ticker in enumerate(tickers):
            if (i + 1) % 20 == 0 or (i + 1) == total:
                print(f"   進度: {i+1}/{total} ({(i+1)/total*100:.1f}%)")
            
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                
                fundamentals.append({
                    'ticker': ticker,
                    'market_cap': info.get('marketCap', np.nan),
                    'sector': info.get('sector', 'Unknown'),
                    'pe_ratio': info.get('trailingPE', np.nan),
                    'pb_ratio': info.get('priceToBook', np.nan),
                    'roe': info.get('returnOnEquity', np.nan) * 100 if info.get('returnOnEquity') else np.nan,
                    'debt_to_equity': info.get('debtToEquity', np.nan),
                    'avg_volume': info.get('averageVolume', np.nan),
                    'current_price': info.get('currentPrice', np.nan),
                    'ipo_date': info.get('firstTradeDateEpochUtc', None),
                })
            except Exception as e:
                fundamentals.append({
                    'ticker': ticker,
                    'market_cap': np.nan,
                    'sector': 'Unknown',
                    'pe_ratio': np.nan,
                    'pb_ratio': np.nan,
                    'roe': np.nan,
                    'debt_to_equity': np.nan,
                    'avg_volume': np.nan,
                    'current_price': np.nan,
                    'ipo_date': None,
                })
        
        df = pd.DataFrame(fundamentals)
        print(f"✅ 下載完成")
        return df

# ============================================
# 3) Universe 篩選模組
# ============================================

class UniverseFilter:
    """兩層篩選：流動性 + 質量"""
    
    def __init__(self, config):
        self.config = config
        
    def apply_filters(self, fundamentals_df):
        """應用所有篩選條件"""
        print(f"\n🔍 開始 Universe 篩選...")
        print(f"   初始股票數量: {len(fundamentals_df)}")
        
        df = fundamentals_df.copy()
        
        # 第一層：流動性篩選
        df = self._filter_liquidity(df)
        
        # 第二層：質量篩選
        df = self._filter_quality(df)
        
        print(f"✅ 篩選完成，剩餘 {len(df)} 隻股票\n")
        
        return df
    
    def _filter_liquidity(self, df):
        """第一層：流動性篩選"""
        print("\n📌 第一層篩選（流動性）")
        
        initial_count = len(df)
        
        # 1. 市值篩選
        df = df[df['market_cap'] >= self.config['min_market_cap']]
        print(f"   市值 ≥ ${self.config['min_market_cap']:,.0f}: {len(df)} 隻 (排除 {initial_count - len(df)})")
        
        # 2. 成交量篩選
        initial_count = len(df)
        df = df[df['avg_volume'] >= self.config['min_avg_volume']]
        print(f"   日均成交量 ≥ {self.config['min_avg_volume']:,.0f}: {len(df)} 隻 (排除 {initial_count - len(df)})")
        
        # 3. 股價篩選
        initial_count = len(df)
        df = df[df['current_price'] >= self.config['min_price']]
        print(f"   股價 ≥ ${self.config['min_price']}: {len(df)} 隻 (排除 {initial_count - len(df)})")
        
        return df
    
    def _filter_quality(self, df):
        """第二層：質量篩選"""
        print("\n📌 第二層篩選（質量）")
        
        # 1. ROE 篩選
        initial_count = len(df)
        df = df[df['roe'] >= self.config['min_roe']]
        print(f"   ROE ≥ {self.config['min_roe']}%: {len(df)} 隻 (排除 {initial_count - len(df)})")
        
        # 2. 負債比篩選
        initial_count = len(df)
        df = df[df['debt_to_equity'] <= self.config['max_debt_ratio']]
        print(f"   負債比 ≤ {self.config['max_debt_ratio']}%: {len(df)} 隻 (排除 {initial_count - len(df)})")
        
        # 3. IPO 日期篩選
        initial_count = len(df)
        cutoff_date = datetime.now() - timedelta(days=self.config['ipo_exclusion_months'] * 30)
        df = df[
            (df['ipo_date'].isna()) | 
            (pd.to_datetime(df['ipo_date'], unit='s') < cutoff_date)
        ]
        print(f"   排除 {self.config['ipo_exclusion_months']} 個月內 IPO: {len(df)} 隻 (排除 {initial_count - len(df)})")
        
        return df

# ============================================
# 4) 數據清理模組（修正版）
# ============================================

# ============================================
# 4) 數據清理模組（調試版）
# ============================================

class DataCleaner:
    """清理並驗證數據質量"""
    
    def __init__(self, config):
        self.config = config
    
    def clean_price_data(self, price_data, universe_tickers):
        """清理價格數據"""
        print(f"\n🧹 清理價格數據...")
        print(f"   數據結構類型: {type(price_data)}")
        print(f"   Columns 類型: {type(price_data.columns)}")
        
        # 調試：檢查數據結構
        if isinstance(price_data.columns, pd.MultiIndex):
            print(f"   MultiIndex 層級: {price_data.columns.nlevels}")
            print(f"   前 5 個 columns: {list(price_data.columns[:5])}")
        else:
            print(f"   Columns: {list(price_data.columns)}")
        
        cleaned_data = {}
        total = len(universe_tickers)
        error_log = []
        
        for i, ticker in enumerate(universe_tickers):
            if (i + 1) % 10 == 0 or (i + 1) == total:
                print(f"   進度: {i+1}/{total} ({(i+1)/total*100:.1f}%) - 成功: {len(cleaned_data)}")
            
            try:
                # 處理 yfinance 數據結構
                if isinstance(price_data.columns, pd.MultiIndex):
                    # 多股票下載：columns = MultiIndex
                    # 檢查 ticker 是否存在
                    available_tickers = price_data.columns.get_level_values(0).unique()
                    
                    if ticker not in available_tickers:
                        error_log.append(f"{ticker}: 不在下載數據中")
                        continue
                    
                    df = price_data[ticker].copy()
                else:
                    # 單股票下載
                    df = price_data.copy()
                
                # 確保有 Close 列
                if 'Close' not in df.columns:
                    error_log.append(f"{ticker}: 缺少 Close 列")
                    continue
                
                # 移除全 NaN 的行
                df = df.dropna(how='all')
                
                # 檢查數據完整性
                close_series = df['Close'].dropna()
                
                if len(close_series) == 0:
                    error_log.append(f"{ticker}: Close 列全為 NaN")
                    continue
                
                missing_ratio = (len(df) - len(close_series)) / len(df)
                
                if missing_ratio > self.config['max_missing_ratio']:
                    error_log.append(f"{ticker}: 缺失數據過多 ({missing_ratio:.1%})")
                    continue
                
                # 填充缺失值（使用新語法）
                df = df.ffill().bfill()
                
                # 移除異常值（單日變化 > 50%）
                returns = df['Close'].pct_change()
                mask = abs(returns) < 0.50
                df = df[mask]
                
                # 確保至少有 252 個交易日（1年）
                if len(df) < 252:
                    error_log.append(f"{ticker}: 數據不足 ({len(df)} 天)")
                    continue
                
                cleaned_data[ticker] = df
                
            except Exception as e:
                error_log.append(f"{ticker}: {str(e)}")
                continue
        
        print(f"\n✅ 清理完成，剩餘 {len(cleaned_data)} 隻股票")
        
        # 顯示前 10 個錯誤
        if len(error_log) > 0:
            print(f"\n⚠️  前 10 個失敗原因:")
            for err in error_log[:10]:
                print(f"   {err}")
        
        return cleaned_data


# ============================================
# 5) 主流程
# ============================================

def main():
    """主執行流程"""
    
    print("=" * 60)
    print("🚀 長線量化系統 - Part 1: 數據 + Universe 篩選")
    print("=" * 60)
    
    # 初始化模組
    downloader = DataDownloader(CONFIG)
    universe_filter = UniverseFilter(CONFIG)
    cleaner = DataCleaner(CONFIG)
    
    # Step 1: 獲取股票列表
    tickers = downloader.get_sp500_tickers()
    
    # Step 2: 下載基本面數據
    fundamentals = downloader.download_fundamental_data(tickers)
    
    # Step 3: Universe 篩選
    filtered_universe = universe_filter.apply_filters(fundamentals)
    
    # Step 4: 下載篩選後的價格數據
    universe_tickers = filtered_universe['ticker'].tolist()
    
    if len(universe_tickers) == 0:
        print("❌ 沒有股票通過篩選，請檢查篩選條件")
        return None, None
    
    price_data = downloader.download_price_data(universe_tickers)
    
    # Step 5: 清理價格數據
    cleaned_prices = cleaner.clean_price_data(price_data, universe_tickers)
    
    # Step 6: 更新 universe（只保留有完整數據的股票）
    final_tickers = list(cleaned_prices.keys())
    filtered_universe = filtered_universe[filtered_universe['ticker'].isin(final_tickers)]
    
    # Step 7: 輸出結果
    print("\n" + "=" * 60)
    print("📊 最終 Universe 統計")
    print("=" * 60)
    print(f"股票數量: {len(cleaned_prices)}")
    
    if len(cleaned_prices) > 0:
        print(f"\n行業分佈:")
        sector_dist = filtered_universe['sector'].value_counts()
        for sector, count in sector_dist.items():
            print(f"   {sector}: {count}")
        
        # 保存結果
        filtered_universe.to_csv('universe.csv', index=False)
        print("\n✅ Universe 已保存至 universe.csv")
    else:
        print("\n❌ 無有效數據")
    
    return filtered_universe, cleaned_prices

if __name__ == "__main__":
    universe, prices = main()
