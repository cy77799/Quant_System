"""
長線量化系統 - Part 4: 回測引擎（修正版）
10年歷史回測 + 完整性能指標
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================
# 1) 回測引擎
# ============================================

class Backtester:
    """歷史回測引擎"""
    
    def __init__(self, initial_capital=100000, rebalance_freq='Q', transaction_cost=0.003):
        """
        Parameters:
        -----------
        initial_capital : float
            初始資金
        rebalance_freq : str
            再平衡頻率 ('Q'=季度, 'M'=月度, 'Y'=年度)
        transaction_cost : float
            交易成本（單邊，0.3%）
        """
        self.initial_capital = initial_capital
        self.rebalance_freq = rebalance_freq
        self.transaction_cost = transaction_cost
    
    def run_backtest(self, portfolio_df, price_data, start_date='2015-01-01', end_date='2025-12-31'):
        """
        執行回測
        
        Parameters:
        -----------
        portfolio_df : DataFrame
            Portfolio 配置（ticker, weight）
        price_data : dict or DataFrame
            價格數據
        start_date : str
            回測起始日
        end_date : str
            回測結束日
        
        Returns:
        --------
        dict: 回測結果
        """
        
        print("\n" + "=" * 60)
        print("📈 開始回測")
        print("=" * 60)
        print(f"期間: {start_date} 至 {end_date}")
        print(f"初始資金: ${self.initial_capital:,.0f}")
        print(f"再平衡頻率: {self.rebalance_freq}")
        print(f"交易成本: {self.transaction_cost:.2%}")
        
        # 準備數據
        tickers = portfolio_df['ticker'].tolist()
        weights = dict(zip(portfolio_df['ticker'], portfolio_df['weight']))
        
        # 獲取所有價格數據並對齊日期
        aligned_prices = self._align_prices(price_data, tickers, start_date, end_date)
        
        if aligned_prices is None:
            print("❌ 數據對齊失敗")
            return None
        
        # 生成再平衡日期
        rebalance_dates = self._generate_rebalance_dates(aligned_prices.index, self.rebalance_freq)
        
        print(f"\n再平衡次數: {len(rebalance_dates)}")
        
        # 執行回測
        results = self._simulate_portfolio(aligned_prices, weights, rebalance_dates)
        
        print("\n✅ 回測完成")
        
        return results
    
    def _align_prices(self, price_data, tickers, start_date, end_date):
        """對齊所有股票的價格數據"""
        
        print("\n📊 對齊價格數據...")
        print(f"   Price data 類型: {type(price_data)}")
        
        price_dict = {}
        
        # 處理不同的數據格式
        if isinstance(price_data, dict):
            # Dict 格式：{ticker: DataFrame}
            for ticker in tickers:
                if ticker not in price_data:
                    print(f"   ⚠️  {ticker}: 無價格數據")
                    continue
                
                try:
                    prices = price_data[ticker]['Close']
                    prices = prices[start_date:end_date]
                    price_dict[ticker] = prices
                except Exception as e:
                    print(f"   ⚠️  {ticker}: 數據提取失敗")
                    continue
        
        elif isinstance(price_data, pd.DataFrame):
            # DataFrame 格式（MultiIndex columns）
            if isinstance(price_data.columns, pd.MultiIndex):
                for ticker in tickers:
                    if (ticker, 'Close') in price_data.columns:
                        prices = price_data[(ticker, 'Close')]
                        prices = prices[start_date:end_date]
                        price_dict[ticker] = prices
                    else:
                        print(f"   ⚠️  {ticker}: 無價格數據")
            else:
                print("   ❌ 不支援的 DataFrame 格式")
                return None
        
        else:
            print(f"   ❌ 不支援的數據類型: {type(price_data)}")
            return None
        
        if len(price_dict) == 0:
            print("   ❌ 沒有有效的價格數據")
            return None
        
        # 合併成 DataFrame
        aligned_df = pd.DataFrame(price_dict)
        
        # 前向填充缺失值
        aligned_df = aligned_df.ffill()
        
        # 移除仍有缺失值的行
        aligned_df = aligned_df.dropna()
        
        print(f"   ✅ 股票數量: {len(aligned_df.columns)}")
        print(f"   ✅ 數據期間: {aligned_df.index[0].date()} 至 {aligned_df.index[-1].date()}")
        print(f"   ✅ 交易日數量: {len(aligned_df)}")
        
        return aligned_df
    
    def _generate_rebalance_dates(self, date_index, freq):
        """生成再平衡日期"""
        
        # 轉換為 DatetimeIndex
        date_index = pd.DatetimeIndex(date_index)
        
        # 按頻率分組
        if freq == 'Q':
            grouped = date_index.to_period('Q')
        elif freq == 'M':
            grouped = date_index.to_period('M')
        elif freq == 'Y':
            grouped = date_index.to_period('Y')
        else:
            grouped = date_index.to_period('Q')
        
        # 每個期間的第一個交易日
        rebalance_dates = []
        for period in grouped.unique():
            period_dates = date_index[grouped == period]
            if len(period_dates) > 0:
                rebalance_dates.append(period_dates[0])
        
        return rebalance_dates
    
    def _simulate_portfolio(self, prices, target_weights, rebalance_dates):
        """模擬 Portfolio 表現"""
        
        print("\n🔄 模擬交易...")
        
        # 初始化
        portfolio_value = []
        dates = []
        holdings = {}  # {ticker: shares}
        cash = self.initial_capital
        
        total_transaction_cost = 0
        rebalance_count = 0
        
        for i, date in enumerate(prices.index):
            
            # 檢查是否需要再平衡
            if date in rebalance_dates:
                rebalance_count += 1
                
                # 計算當前持倉價值
                current_prices = prices.loc[date]
                holdings_value = sum(holdings.get(ticker, 0) * current_prices[ticker] 
                                    for ticker in current_prices.index)
                total_value = cash + holdings_value
                
                # 賣出所有持倉
                if len(holdings) > 0:
                    cash = total_value * (1 - self.transaction_cost)  # 扣除賣出成本
                    total_transaction_cost += total_value * self.transaction_cost
                    holdings = {}
                
                # 按目標權重買入
                for ticker, weight in target_weights.items():
                    if ticker in current_prices.index:
                        target_value = cash * weight
                        shares = target_value / current_prices[ticker]
                        holdings[ticker] = shares
                
                # 扣除買入成本
                purchase_cost = cash * self.transaction_cost
                cash = 0  # 全部投資
                total_transaction_cost += purchase_cost
            
            # 計算當日 Portfolio 價值
            current_prices = prices.loc[date]
            holdings_value = sum(holdings.get(ticker, 0) * current_prices.get(ticker, 0) 
                                for ticker in holdings.keys())
            total_value = cash + holdings_value
            
            portfolio_value.append(total_value)
            dates.append(date)
        
        print(f"   ✅ 再平衡次數: {rebalance_count}")
        print(f"   ✅ 總交易成本: ${total_transaction_cost:,.0f} ({total_transaction_cost/self.initial_capital:.2%})")
        
        # 構建結果
        results = {
            'dates': dates,
            'portfolio_value': portfolio_value,
            'prices': prices,
            'total_cost': total_transaction_cost,
            'rebalance_count': rebalance_count
        }
        
        return results

# ============================================
# 2) 性能分析引擎
# ============================================

class PerformanceAnalyzer:
    """計算所有性能指標"""
    
    def __init__(self):
        pass
    
    def analyze(self, backtest_results, benchmark_prices=None):
        """
        分析回測結果
        
        Parameters:
        -----------
        backtest_results : dict
            回測結果
        benchmark_prices : Series
            基準指數價格（可選，如 SPY）
        
        Returns:
        --------
        dict: 性能指標
        """
        
        print("\n" + "=" * 60)
        print("📊 性能分析")
        print("=" * 60)
        
        dates = backtest_results['dates']
        portfolio_value = backtest_results['portfolio_value']
        
        # 轉換為 Series
        portfolio_series = pd.Series(portfolio_value, index=dates)
        
        # 計算回報
        returns = portfolio_series.pct_change().dropna()
        
        # 核心指標
        metrics = {}
        
        # 1. CAGR
        years = (dates[-1] - dates[0]).days / 365.25
        total_return = (portfolio_value[-1] / portfolio_value[0]) - 1
        cagr = (1 + total_return) ** (1 / years) - 1
        metrics['CAGR'] = cagr
        
        # 2. Total Return
        metrics['Total Return'] = total_return
        
        # 3. Volatility (年化)
        metrics['Volatility'] = returns.std() * np.sqrt(252)
        
        # 4. Sharpe Ratio (假設無風險利率 2%)
        risk_free_rate = 0.02
        excess_returns = returns - risk_free_rate / 252
        metrics['Sharpe Ratio'] = excess_returns.mean() / returns.std() * np.sqrt(252)
        
        # 5. Sortino Ratio (只考慮下行波動)
        downside_returns = returns[returns < 0]
        downside_std = downside_returns.std() * np.sqrt(252)
        metrics['Sortino Ratio'] = (cagr - risk_free_rate) / downside_std if downside_std > 0 else 0
        
        # 6. Max Drawdown
        cumulative = (1 + returns).cumprod()
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        max_dd = drawdown.min()
        metrics['Max Drawdown'] = max_dd
        
        # 7. MAR Ratio / Calmar Ratio
        metrics['Calmar Ratio'] = cagr / abs(max_dd) if max_dd != 0 else 0
        
        # 8. Longest Drawdown Duration
        drawdown_periods = self._calculate_drawdown_duration(cumulative)
        metrics['Longest DD Duration'] = drawdown_periods
        
        # 9. Win Rate
        metrics['Win Rate'] = (returns > 0).sum() / len(returns)
        
        # 10. Best/Worst Year
        yearly_returns = portfolio_series.resample('YE').last().pct_change().dropna()
        if len(yearly_returns) > 0:
            metrics['Best Year'] = yearly_returns.max()
            metrics['Worst Year'] = yearly_returns.min()
            metrics['% Positive Years'] = (yearly_returns > 0).sum() / len(yearly_returns)
        else:
            metrics['Best Year'] = 0
            metrics['Worst Year'] = 0
            metrics['% Positive Years'] = 0
        
        return metrics, returns, drawdown
    
    def _calculate_drawdown_duration(self, cumulative):
        """計算最長回撤期（天數）"""
        
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        
        # 找出所有回撤期間
        in_drawdown = drawdown < 0
        
        if not in_drawdown.any():
            return 0
        
        # 計算連續回撤天數
        drawdown_lengths = []
        current_length = 0
        
        for dd in in_drawdown:
            if dd:
                current_length += 1
            else:
                if current_length > 0:
                    drawdown_lengths.append(current_length)
                current_length = 0
        
        if current_length > 0:
            drawdown_lengths.append(current_length)
        
        return max(drawdown_lengths) if drawdown_lengths else 0
    
    def print_metrics(self, metrics):
        """打印性能指標"""
        
        print("\n" + "-" * 60)
        print("核心回報指標")
        print("-" * 60)
        print(f"CAGR:                    {metrics['CAGR']:>8.2%}")
        print(f"Total Return:            {metrics['Total Return']:>8.2%}")
        print(f"Best Year:               {metrics['Best Year']:>8.2%}")
        print(f"Worst Year:              {metrics['Worst Year']:>8.2%}")
        print(f"% Positive Years:        {metrics['% Positive Years']:>8.1%}")
        
        print("\n" + "-" * 60)
        print("風險指標")
        print("-" * 60)
        print(f"Volatility:              {metrics['Volatility']:>8.2%}")
        print(f"Max Drawdown:            {metrics['Max Drawdown']:>8.2%}")
        print(f"Longest DD Duration:     {metrics['Longest DD Duration']:>8.0f} 天")
        
        print("\n" + "-" * 60)
        print("風險調整回報")
        print("-" * 60)
        print(f"Sharpe Ratio:            {metrics['Sharpe Ratio']:>8.2f}")
        print(f"Sortino Ratio:           {metrics['Sortino Ratio']:>8.2f}")
        print(f"Calmar Ratio:            {metrics['Calmar Ratio']:>8.2f}")
        print(f"Win Rate:                {metrics['Win Rate']:>8.1%}")

# ============================================
# 3) 主流程
# ============================================

def main():
    """主執行流程"""
    
    print("=" * 60)
    print("🚀 長線量化系統 - Part 4: 回測引擎")
    print("=" * 60)
    
    # 載入 Portfolio
    print("\n📂 載入 Portfolio...")
    portfolio = pd.read_csv('final_portfolio.csv')
    
    # 載入價格數據（從 Part 1）
    print("\n📂 載入價格數據...")
    from part1_data_universe import main as part1_main
    universe, prices = part1_main()
    
    # 初始化回測引擎
    backtester = Backtester(
        initial_capital=100000,
        rebalance_freq='Q',  # 季度再平衡
        transaction_cost=0.003  # 0.3%
    )
    
    # 執行回測
    results = backtester.run_backtest(
        portfolio,
        prices,
        start_date='2020-01-01',  # 5年回測
        end_date='2025-12-31'
    )
    
    if results is None:
        return
    
    # 性能分析
    analyzer = PerformanceAnalyzer()
    metrics, returns, drawdown = analyzer.analyze(results)
    
    # 打印指標
    analyzer.print_metrics(metrics)
    
    # 保存結果
    results_df = pd.DataFrame({
        'Date': results['dates'],
        'Portfolio_Value': results['portfolio_value']
    })
    results_df.to_csv('backtest_results.csv', index=False)
    
    metrics_df = pd.DataFrame([metrics])
    metrics_df.to_csv('performance_metrics.csv', index=False)
    
    print("\n" + "=" * 60)
    print("✅ 回測完成")
    print("=" * 60)
    print("已保存:")
    print("  - backtest_results.csv (每日組合價值)")
    print("  - performance_metrics.csv (性能指標)")
    
    return results, metrics

if __name__ == "__main__":
    results, metrics = main()
