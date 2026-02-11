"""
長線量化系統 - Part 3: Portfolio Construction
Score/Volatility 加權 + Sector 限制
"""

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ============================================
# 1) 波動率計算引擎
# ============================================

class VolatilityCalculator:
    """計算股票波動率"""
    
    def __init__(self, lookback_days=60):
        self.lookback_days = lookback_days
    
    def calculate_volatility(self, price_data, tickers):
        """
        計算年化波動率
        
        Parameters:
        -----------
        price_data : dict
            價格數據 {ticker: DataFrame}
        tickers : list
            股票列表
        
        Returns:
        --------
        dict: {ticker: volatility}
        """
        
        print(f"\n📉 計算波動率（過去 {self.lookback_days} 天）...")
        
        volatility_dict = {}
        
        for ticker in tickers:
            if ticker not in price_data:
                continue
            
            try:
                prices = price_data[ticker]['Close']
                
                # 取最近 N 天
                recent_prices = prices.tail(self.lookback_days)
                
                # 計算日回報
                returns = recent_prices.pct_change().dropna()
                
                # 年化波動率（假設 252 個交易日）
                vol = returns.std() * np.sqrt(252)
                
                volatility_dict[ticker] = vol
                
            except Exception as e:
                continue
        
        print(f"✅ 波動率計算完成")
        
        return volatility_dict

# ============================================
# 2) Portfolio 權重計算引擎
# ============================================

class PortfolioWeightCalculator:
    """Score/Volatility 加權方法"""
    
    def __init__(self, score_alpha=0.5):
        """
        Parameters:
        -----------
        score_alpha : float (0-1)
            Score 影響強度
            0 = 純 Inverse Volatility
            1 = Score 影響最大
            建議：0.5（平衡）
        """
        self.score_alpha = score_alpha
    
    def calculate_weights(self, stocks_df, volatility_dict):
        """
        計算 Score/Volatility 權重
        
        Parameters:
        -----------
        stocks_df : DataFrame
            選中的股票（含 composite_score）
        volatility_dict : dict
            波動率數據
        
        Returns:
        --------
        DataFrame: 含權重的數據
        """
        
        print(f"\n⚖️  計算 Score/Volatility 權重（alpha={self.score_alpha}）...")
        
        df = stocks_df.copy()
        
        # 添加波動率
        df['volatility'] = df['ticker'].map(volatility_dict)
        
        # 移除缺失波動率的股票
        df = df.dropna(subset=['volatility'])
        
        # Step 1: Inverse Volatility Weight（基礎）
        df['inv_vol'] = 1 / df['volatility']
        df['inv_vol_weight'] = df['inv_vol'] / df['inv_vol'].sum()
        
        # Step 2: Score Adjustment（Tilt）
        df['score_adjusted'] = df['composite_score'] ** self.score_alpha
        
        # Step 3: 組合權重
        df['raw_weight'] = df['inv_vol_weight'] * df['score_adjusted']
        
        # Step 4: Normalize 到 100%
        df['weight'] = df['raw_weight'] / df['raw_weight'].sum()
        
        # Step 5: 應用權重限制
        df = self._apply_weight_constraints(df)
        
        print(f"✅ 權重計算完成")
        
        return df
    
    def _apply_weight_constraints(self, df):
        """應用單股權重限制"""
        
        MAX_WEIGHT = 0.12  # 單股最多 12%
        MIN_WEIGHT = 0.04  # 單股最少 4%
        
        # 上限
        df.loc[df['weight'] > MAX_WEIGHT, 'weight'] = MAX_WEIGHT
        
        # 下限
        df.loc[df['weight'] < MIN_WEIGHT, 'weight'] = MIN_WEIGHT
        
        # 重新標準化
        df['weight'] = df['weight'] / df['weight'].sum()
        
        return df

# ============================================
# 3) Sector 限制引擎
# ============================================

class SectorConstraints:
    """行業限制：單一行業最多 4 隻、權重 ≤ 25%"""
    
    def __init__(self, max_stocks_per_sector=4, max_sector_weight=0.25):
        self.max_stocks_per_sector = max_stocks_per_sector
        self.max_sector_weight = max_sector_weight
    
    def apply_constraints(self, stocks_df):
        """
        應用 Sector 限制
        
        Parameters:
        -----------
        stocks_df : DataFrame
            候選股票（按 composite_score 排序）
        
        Returns:
        --------
        DataFrame: 符合 Sector 限制的股票
        """
        
        print(f"\n🏢 應用 Sector 限制...")
        print(f"   - 單一行業最多 {self.max_stocks_per_sector} 隻")
        print(f"   - 單一行業權重 ≤ {self.max_sector_weight:.0%}")
        
        df = stocks_df.copy()
        selected_stocks = []
        sector_count = {}
        
        # Step 1: 按評分選股，遵守行業數量限制
        for idx, row in df.iterrows():
            sector = row['sector']
            
            # 檢查該行業是否已達上限
            if sector_count.get(sector, 0) < self.max_stocks_per_sector:
                selected_stocks.append(row)
                sector_count[sector] = sector_count.get(sector, 0) + 1
        
        selected_df = pd.DataFrame(selected_stocks)
        
        print(f"   ✅ 選股後行業分佈:")
        for sector, count in sector_count.items():
            print(f"      {sector}: {count} 隻")
        
        return selected_df
    
    def check_sector_weight(self, portfolio_df):
        """檢查行業權重是否超標"""
        
        print(f"\n🔍 檢查行業權重...")
        
        sector_weights = portfolio_df.groupby('sector')['weight'].sum()
        
        violations = sector_weights[sector_weights > self.max_sector_weight]
        
        if len(violations) > 0:
            print(f"   ⚠️  以下行業超過 {self.max_sector_weight:.0%} 限制:")
            for sector, weight in violations.items():
                print(f"      {sector}: {weight:.1%}")
            
            # 調整超標行業
            portfolio_df = self._adjust_sector_weights(portfolio_df, violations)
        else:
            print(f"   ✅ 所有行業權重符合限制")
        
        return portfolio_df
    
    def _adjust_sector_weights(self, df, violations):
        """調整超標行業的權重"""
        
        for sector in violations.index:
            # 該行業的股票
            sector_stocks = df[df['sector'] == sector]
            
            # 當前總權重
            current_weight = sector_stocks['weight'].sum()
            
            # 縮放係數
            scale_factor = self.max_sector_weight / current_weight
            
            # 調整權重
            df.loc[df['sector'] == sector, 'weight'] *= scale_factor
        
        # 重新標準化所有權重
        df['weight'] = df['weight'] / df['weight'].sum()
        
        return df

# ============================================
# 4) Portfolio 構建主引擎
# ============================================

class PortfolioBuilder:
    """整合所有模組，構建最終 Portfolio"""
    
    def __init__(self, target_stocks=15, score_alpha=0.5):
        self.target_stocks = target_stocks
        self.vol_calculator = VolatilityCalculator(lookback_days=60)
        self.weight_calculator = PortfolioWeightCalculator(score_alpha=score_alpha)
        self.sector_constraints = SectorConstraints(max_stocks_per_sector=4, max_sector_weight=0.25)
    
    def build_portfolio(self, scored_df, price_data):
        """
        構建最終 Portfolio
        
        Parameters:
        -----------
        scored_df : DataFrame
            因子評分數據（從 Part 2）
        price_data : dict
            價格數據
        
        Returns:
        --------
        DataFrame: 最終 Portfolio
        """
        
        print("\n" + "=" * 60)
        print("🏗️  構建 Portfolio")
        print("=" * 60)
        
        # Step 1: 應用 Sector 限制，選出候選股票
        candidate_stocks = self.sector_constraints.apply_constraints(scored_df.head(30))  # 從 Top 30 選
        
        # Step 2: 計算波動率
        tickers = candidate_stocks['ticker'].tolist()
        volatility_dict = self.vol_calculator.calculate_volatility(price_data, tickers)
        
        # Step 3: 計算權重
        weighted_portfolio = self.weight_calculator.calculate_weights(candidate_stocks, volatility_dict)
        
        # Step 4: 選出最終持倉（Top N）
        final_portfolio = weighted_portfolio.head(self.target_stocks)
        
        # Step 5: 重新計算權重（只針對最終持倉）
        final_portfolio['weight'] = final_portfolio['weight'] / final_portfolio['weight'].sum()
        
        # Step 6: 檢查並調整 Sector 權重
        final_portfolio = self.sector_constraints.check_sector_weight(final_portfolio)
        
        print(f"\n✅ Portfolio 構建完成")
        
        return final_portfolio

# ============================================
# 5) 主流程
# ============================================

def main():
    """主執行流程"""
    
    print("=" * 60)
    print("🚀 長線量化系統 - Part 3: Portfolio Construction")
    print("=" * 60)
    
    # 載入 Part 2 數據
    print("\n📂 載入 Part 2 數據...")
    
    try:
        from part2_factor_engine import main as part2_main
        scored_df, weights = part2_main()
        
        if scored_df is None or len(scored_df) == 0:
            print("❌ Part 2 數據不可用")
            return
        
        # 載入價格數據
        from part1_data_universe import main as part1_main
        universe, prices = part1_main()
        
    except Exception as e:
        print(f"❌ 無法載入數據: {str(e)}")
        return
    
    # 初始化 Portfolio Builder
    portfolio_builder = PortfolioBuilder(target_stocks=15, score_alpha=0.5)
    
    # 構建 Portfolio
    final_portfolio = portfolio_builder.build_portfolio(scored_df, prices)
    
    # 輸出結果
    print("\n" + "=" * 60)
    print("📊 最終 Portfolio（15 隻股票）")
    print("=" * 60)
    
    print(f"\n{'Ticker':<8} {'Sector':<25} {'Score':<8} {'Vol':<8} {'Weight':<8}")
    print("-" * 60)
    
    for idx, row in final_portfolio.iterrows():
        print(f"{row['ticker']:<8} {row['sector']:<25} "
              f"{row['composite_score']:6.1f}   "
              f"{row['volatility']:6.1%}   "
              f"{row['weight']:6.1%}")
    
    # 行業分佈
    print("\n" + "-" * 60)
    print("行業權重分佈:")
    print("-" * 60)
    
    sector_weights = final_portfolio.groupby('sector')['weight'].sum().sort_values(ascending=False)
    for sector, weight in sector_weights.items():
        count = len(final_portfolio[final_portfolio['sector'] == sector])
        print(f"{sector:<30} {weight:6.1%}  ({count} 隻)")
    
    # 保存結果
    final_portfolio.to_csv('final_portfolio.csv', index=False)
    print("\n✅ Portfolio 已保存至 final_portfolio.csv")
    
    return final_portfolio

if __name__ == "__main__":
    portfolio = main()
