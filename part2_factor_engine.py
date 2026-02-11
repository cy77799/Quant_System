"""
長線量化系統 - Part 2: 因子引擎
計算 Value / Quality / Momentum 因子 + 動態權重
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================
# 1) 因子計算引擎
# ============================================

class FactorEngine:
    """計算三大因子：Value / Quality / Momentum"""
    
    def __init__(self):
        pass
    
    def calculate_all_factors(self, fundamentals_df, price_data, calculation_date=None):
        """
        計算所有因子
        
        Parameters:
        -----------
        fundamentals_df : DataFrame
            基本面數據（從 Part 1）
        price_data : dict
            價格數據字典 {ticker: DataFrame}
        calculation_date : str or None
            計算日期（用於回測），None = 使用最新數據
        
        Returns:
        --------
        DataFrame: 含所有因子的數據
        """
        
        print("\n📊 計算因子...")
        
        # 複製基本面數據
        factors_df = fundamentals_df.copy()
        
        # 計算 Momentum 因子
        factors_df = self._calculate_momentum(factors_df, price_data, calculation_date)
        
        print(f"✅ 因子計算完成")
        
        return factors_df
    
    def _calculate_momentum(self, df, price_data, calculation_date):
        """計算 12M Momentum（排除最近 1M）"""
        
        momentum_list = []
        
        for ticker in df['ticker']:
            try:
                prices = price_data[ticker]['Close']
                
                # 如果指定日期，使用該日期之前的數據
                if calculation_date:
                    prices = prices[:calculation_date]
                
                # 確保有足夠數據
                if len(prices) < 252:  # 至少 1 年數據
                    momentum_list.append(np.nan)
                    continue
                
                # 12M 前價格（排除最近 1M = 21 個交易日）
                price_12m_ago = prices.iloc[-252]  # 12 個月前
                price_1m_ago = prices.iloc[-21]    # 1 個月前
                
                # Momentum = (P_1m_ago - P_12m_ago) / P_12m_ago
                momentum = (price_1m_ago - price_12m_ago) / price_12m_ago
                
                momentum_list.append(momentum)
                
            except Exception as e:
                momentum_list.append(np.nan)
        
        df['momentum_12m'] = momentum_list
        
        return df

# ============================================
# 2) 因子評分引擎（Ranking + Normalize）
# ============================================

class FactorScorer:
    """因子排名 + 標準化評分"""
    
    def __init__(self):
        pass
    
    def score_factors(self, factors_df):
        """
        對因子進行排名並標準化到 0-100
        
        Parameters:
        -----------
        factors_df : DataFrame
            含因子的數據
        
        Returns:
        --------
        DataFrame: 含評分的數據
        """
        
        print("\n📈 因子評分...")
        
        df = factors_df.copy()
        
        # 移除缺失值
        df = df.dropna(subset=['pe_ratio', 'pb_ratio', 'roe', 'debt_to_equity', 'momentum_12m'])
        
        if len(df) == 0:
            print("❌ 沒有完整因子數據的股票")
            return df
        
        # 1. Value 因子評分（PE 和 PB 越低越好）
        df['value_score'] = self._rank_and_normalize([
            (df['pe_ratio'], False),   # PE 越低越好
            (df['pb_ratio'], False),   # PB 越低越好
        ])
        
        # 2. Quality 因子評分（ROE 越高越好，負債比越低越好）
        df['quality_score'] = self._rank_and_normalize([
            (df['roe'], True),              # ROE 越高越好
            (df['debt_to_equity'], False),  # 負債比越低越好
        ])
        
        # 3. Momentum 因子評分（越高越好）
        df['momentum_score'] = self._rank_and_normalize([
            (df['momentum_12m'], True),  # Momentum 越高越好
        ])
        
        print(f"✅ 評分完成，剩餘 {len(df)} 隻股票")
        
        return df
    
    def _rank_and_normalize(self, factor_list):
        """
        排名並標準化到 0-100
        
        Parameters:
        -----------
        factor_list : list of tuples
            [(Series, ascending), ...] 
            ascending = True 表示越大越好
        
        Returns:
        --------
        Series: 標準化後的評分 (0-100)
        """
        
        # 初始化評分
        combined_rank = 0
        
        for series, ascending in factor_list:
            # 排名（升序或降序）
            if ascending:
                rank = series.rank(ascending=True, method='average')
            else:
                rank = series.rank(ascending=False, method='average')
            
            # 標準化到 0-100
            normalized = (rank - rank.min()) / (rank.max() - rank.min()) * 100
            
            combined_rank += normalized
        
        # 平均評分
        final_score = combined_rank / len(factor_list)
        
        return final_score

# ============================================
# 3) 動態因子權重引擎
# ============================================

class DynamicFactorWeighting:
    """基於過去 12M 表現動態調整因子權重"""
    
    def __init__(self):
        self.default_weights = {
            'value': 0.33,
            'quality': 0.33,
            'momentum': 0.34
        }
    
    def calculate_factor_weights(self, factors_df, price_data, lookback_months=12):
        """
        計算因子權重（基於過去表現）
        
        Parameters:
        -----------
        factors_df : DataFrame
            因子數據
        price_data : dict
            價格數據
        lookback_months : int
            回溯月數
        
        Returns:
        --------
        dict: {'value': w1, 'quality': w2, 'momentum': w3}
        """
        
        print(f"\n⚖️  計算動態因子權重（過去 {lookback_months} 個月）...")
        
        # 計算每個因子的 Long-Short 回報
        factor_returns = {}
        
        for factor_name in ['value_score', 'quality_score', 'momentum_score']:
            returns = self._calculate_factor_return(
                factors_df, 
                price_data, 
                factor_name, 
                lookback_months
            )
            factor_returns[factor_name] = returns
        
        # 根據回報排名分配權重
        sorted_factors = sorted(factor_returns.items(), key=lambda x: x[1], reverse=True)
        
        weights = {
            sorted_factors[0][0].replace('_score', ''): 0.40,  # 最強
            sorted_factors[1][0].replace('_score', ''): 0.35,  # 第二
            sorted_factors[2][0].replace('_score', ''): 0.25,  # 第三
        }
        
        print(f"   因子表現排名:")
        for factor, ret in sorted_factors:
            factor_clean = factor.replace('_score', '')
            weight = weights[factor_clean]
            print(f"   {factor_clean}: {ret:.2%} (權重 {weight:.0%})")
        
        return weights
    
    def _calculate_factor_return(self, factors_df, price_data, factor_name, lookback_months):
        """
        計算單一因子的 Long-Short 回報
        
        策略：做多 Top 20%，做空 Bottom 20%
        """
        
        try:
            # 按因子排序
            sorted_df = factors_df.sort_values(factor_name, ascending=False)
            
            # Top 20% 和 Bottom 20%
            n = len(sorted_df)
            top_20pct = sorted_df.head(int(n * 0.2))
            bottom_20pct = sorted_df.tail(int(n * 0.2))
            
            # 計算過去 N 個月回報
            days_back = lookback_months * 21  # 約 21 個交易日/月
            
            top_returns = []
            bottom_returns = []
            
            for ticker in top_20pct['ticker']:
                if ticker in price_data:
                    prices = price_data[ticker]['Close']
                    if len(prices) >= days_back:
                        ret = (prices.iloc[-1] - prices.iloc[-days_back]) / prices.iloc[-days_back]
                        top_returns.append(ret)
            
            for ticker in bottom_20pct['ticker']:
                if ticker in price_data:
                    prices = price_data[ticker]['Close']
                    if len(prices) >= days_back:
                        ret = (prices.iloc[-1] - prices.iloc[-days_back]) / prices.iloc[-days_back]
                        bottom_returns.append(ret)
            
            # Long-Short 回報
            if len(top_returns) > 0 and len(bottom_returns) > 0:
                long_short_return = np.mean(top_returns) - np.mean(bottom_returns)
                return long_short_return
            else:
                return 0.0
                
        except Exception as e:
            return 0.0

# ============================================
# 4) 綜合評分引擎
# ============================================

class CompositeScorer:
    """綜合因子評分（加權合成）"""
    
    def __init__(self):
        pass
    
    def calculate_composite_score(self, factors_df, weights):
        """
        計算綜合評分
        
        Parameters:
        -----------
        factors_df : DataFrame
            含因子評分的數據
        weights : dict
            因子權重 {'value': w1, 'quality': w2, 'momentum': w3}
        
        Returns:
        --------
        DataFrame: 含綜合評分的數據
        """
        
        print(f"\n🎯 計算綜合評分...")
        
        df = factors_df.copy()
        
        # 加權合成
        df['composite_score'] = (
            df['value_score'] * weights.get('value', 0.33) +
            df['quality_score'] * weights.get('quality', 0.33) +
            df['momentum_score'] * weights.get('momentum', 0.34)
        )
        
        # 按評分排序
        df = df.sort_values('composite_score', ascending=False)
        
        print(f"✅ 綜合評分完成")
        
        return df

# ============================================
# 5) 主流程
# ============================================

def main():
    """主執行流程"""
    
    print("=" * 60)
    print("🚀 長線量化系統 - Part 2: 因子引擎")
    print("=" * 60)
    
    # 載入 Part 1 數據
    print("\n📂 載入 Part 1 數據...")
    
    # 這裡假設你已經運行了 Part 1，有 universe 和 prices
    # 如果你重新運行，需要先載入數據
    try:
        from part1_data_universe import main as part1_main
        universe, prices = part1_main()
        
        if universe is None or len(prices) == 0:
            print("❌ Part 1 數據不可用")
            return
            
    except Exception as e:
        print("❌ 無法載入 Part 1 數據，請先運行 Part 1")
        return
    
    # 初始化引擎
    factor_engine = FactorEngine()
    scorer = FactorScorer()
    dynamic_weighter = DynamicFactorWeighting()
    composite_scorer = CompositeScorer()
    
    # Step 1: 計算因子
    factors_df = factor_engine.calculate_all_factors(universe, prices)
    
    # Step 2: 因子評分
    scored_df = scorer.score_factors(factors_df)
    
    if len(scored_df) == 0:
        print("❌ 無有效評分數據")
        return
    
    # Step 3: 動態權重
    weights = dynamic_weighter.calculate_factor_weights(scored_df, prices)
    
    # Step 4: 綜合評分
    final_df = composite_scorer.calculate_composite_score(scored_df, weights)
    
    # 輸出結果
    print("\n" + "=" * 60)
    print("📊 Top 15 股票（綜合評分）")
    print("=" * 60)
    
    top_15 = final_df.head(15)[['ticker', 'sector', 'value_score', 'quality_score', 'momentum_score', 'composite_score']]
    
    for idx, row in top_15.iterrows():
        print(f"{row['ticker']:6s} | {row['sector']:25s} | "
              f"V:{row['value_score']:5.1f} Q:{row['quality_score']:5.1f} M:{row['momentum_score']:5.1f} | "
              f"總分:{row['composite_score']:5.1f}")
    
    # 保存結果
    final_df.to_csv('factor_scores.csv', index=False)
    print("\n✅ 因子評分已保存至 factor_scores.csv")
    
    return final_df, weights

if __name__ == "__main__":
    results, weights = main()
