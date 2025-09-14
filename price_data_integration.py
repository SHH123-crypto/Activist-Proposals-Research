# price_data_integration.py - Token price data for research correlation analysis
"""
Token price data integration for research questions:
1. Do activist governance proposals in DAOs affect the price of the cryptocurrency associated with the DAO?
2. Price impact analysis around proposal dates
"""

import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json

class TokenPriceAnalyzer:
    """Analyze token price movements around proposal dates"""
    
    def __init__(self):
        self.coingecko_base = "https://api.coingecko.com/api/v3"
        self.token_mappings = {
            # DAO space -> CoinGecko ID
            "aave.eth": "aave",
            "uniswap": "uniswap",
            "compound-governance.eth": "compound-governance-token",
            "ens.eth": "ethereum-name-service",
            "balancer.eth": "balancer",
            "curve.eth": "curve-dao-token",
            "yearn": "yearn-finance",
            "1inch.eth": "1inch",
            "frax.eth": "frax",
            "olympusdao.eth": "olympus",
            "fei.eth": "fei-usd",
            "cream-finance.eth": "cream-2",
            "pickle.eth": "pickle-finance",
            "uma.eth": "uma",
            "synthetix.eth": "havven",
            "makerdao.eth": "maker",
            "sushi.eth": "sushi",
            "gitcoin.eth": "gitcoin",
            "arbitrum-foundation.eth": "arbitrum",
            "optimism.eth": "optimism"
        }
        
        self.cache = {}
    
    def unix_to_date(self, unix_timestamp: int) -> str:
        """Convert Unix timestamp to date string"""
        try:
            return datetime.fromtimestamp(unix_timestamp).strftime('%d-%m-%Y')
        except:
            return datetime.now().strftime('%d-%m-%Y')
    
    def get_token_price_history(self, token_id: str, start_date: str, end_date: str) -> Dict:
        """Get token price history from CoinGecko"""
        cache_key = f"{token_id}_{start_date}_{end_date}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        url = f"{self.coingecko_base}/coins/{token_id}/history"
        
        try:
            # Get price at start date
            response_start = requests.get(
                url,
                params={"date": start_date, "localization": "false"},
                timeout=10
            )
            
            time.sleep(1)  # Rate limiting
            
            # Get price at end date
            response_end = requests.get(
                url,
                params={"date": end_date, "localization": "false"},
                timeout=10
            )
            
            if response_start.status_code == 200 and response_end.status_code == 200:
                start_data = response_start.json()
                end_data = response_end.json()
                
                start_price = start_data.get("market_data", {}).get("current_price", {}).get("usd", 0)
                end_price = end_data.get("market_data", {}).get("current_price", {}).get("usd", 0)
                
                start_volume = start_data.get("market_data", {}).get("total_volume", {}).get("usd", 0)
                end_volume = end_data.get("market_data", {}).get("total_volume", {}).get("usd", 0)
                
                start_mcap = start_data.get("market_data", {}).get("market_cap", {}).get("usd", 0)
                end_mcap = end_data.get("market_data", {}).get("market_cap", {}).get("usd", 0)
                
                price_change = ((end_price - start_price) / start_price * 100) if start_price > 0 else 0
                volume_change = ((end_volume - start_volume) / start_volume * 100) if start_volume > 0 else 0
                mcap_change = ((end_mcap - start_mcap) / start_mcap * 100) if start_mcap > 0 else 0
                
                result = {
                    "token_id": token_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "price_start": start_price,
                    "price_end": end_price,
                    "price_change_pct": round(price_change, 3),
                    "volume_start": start_volume,
                    "volume_end": end_volume,
                    "volume_change_pct": round(volume_change, 3),
                    "market_cap_start": start_mcap,
                    "market_cap_end": end_mcap,
                    "market_cap_change_pct": round(mcap_change, 3),
                    "data_available": True
                }
                
                self.cache[cache_key] = result
                return result
                
        except Exception as e:
            print(f"⚠ Price data error for {token_id}: {e}")
        
        # Return empty data if failed
        return {
            "token_id": token_id,
            "start_date": start_date,
            "end_date": end_date,
            "price_start": 0,
            "price_end": 0,
            "price_change_pct": 0,
            "volume_start": 0,
            "volume_end": 0,
            "volume_change_pct": 0,
            "market_cap_start": 0,
            "market_cap_end": 0,
            "market_cap_change_pct": 0,
            "data_available": False
        }
    
    def analyze_proposal_price_impact(self, dao_space: str, start_unix: int, end_unix: int, 
                                    days_before: int = 7, days_after: int = 7) -> Dict:
        """Analyze price impact around proposal dates"""
        
        token_id = self.token_mappings.get(dao_space)
        if not token_id:
            return {"error": f"No token mapping for {dao_space}"}
        
        try:
            # Calculate date ranges
            proposal_start = datetime.fromtimestamp(start_unix)
            proposal_end = datetime.fromtimestamp(end_unix)
            
            before_start = proposal_start - timedelta(days=days_before)
            after_end = proposal_end + timedelta(days=days_after)
            
            # Get price data for different periods
            periods = {
                "pre_proposal": {
                    "start": before_start.strftime('%d-%m-%Y'),
                    "end": proposal_start.strftime('%d-%m-%Y')
                },
                "during_proposal": {
                    "start": proposal_start.strftime('%d-%m-%Y'),
                    "end": proposal_end.strftime('%d-%m-%Y')
                },
                "post_proposal": {
                    "start": proposal_end.strftime('%d-%m-%Y'),
                    "end": after_end.strftime('%d-%m-%Y')
                }
            }
            
            price_analysis = {}
            for period_name, dates in periods.items():
                price_data = self.get_token_price_history(
                    token_id, dates["start"], dates["end"]
                )
                price_analysis[period_name] = price_data
                time.sleep(1)  # Rate limiting
            
            # Calculate overall impact
            pre_price = price_analysis["pre_proposal"]["price_end"]
            post_price = price_analysis["post_proposal"]["price_end"]
            
            overall_impact = ((post_price - pre_price) / pre_price * 100) if pre_price > 0 else 0
            
            return {
                "dao_space": dao_space,
                "token_id": token_id,
                "analysis_periods": price_analysis,
                "overall_price_impact_pct": round(overall_impact, 3),
                "proposal_duration_days": (proposal_end - proposal_start).days,
                "analysis_successful": True
            }
            
        except Exception as e:
            print(f"⚠ Price impact analysis failed for {dao_space}: {e}")
            return {
                "dao_space": dao_space,
                "token_id": token_id,
                "error": str(e),
                "analysis_successful": False
            }
    
    def batch_analyze_proposals(self, proposals: List[Dict]) -> List[Dict]:
        """Batch analyze price impact for multiple proposals"""
        print(f"📈 Analyzing price impact for {len(proposals)} proposals...")
        
        enhanced_proposals = []
        
        for i, proposal in enumerate(proposals):
            print(f"   Analyzing {i+1}/{len(proposals)}: {proposal.get('dao_space', 'unknown')}")
            
            dao_space = proposal.get("dao_space", "")
            start_unix = proposal.get("start_date", 0)
            end_unix = proposal.get("end_date", 0)
            
            if dao_space and start_unix and end_unix:
                price_impact = self.analyze_proposal_price_impact(
                    dao_space, start_unix, end_unix
                )
                
                # Add price impact data to proposal
                enhanced_proposal = proposal.copy()
                enhanced_proposal.update({
                    "price_impact_analysis": price_impact,
                    "overall_price_impact_pct": price_impact.get("overall_price_impact_pct", 0),
                    "price_data_available": price_impact.get("analysis_successful", False)
                })
                
                enhanced_proposals.append(enhanced_proposal)
            else:
                # Add proposal without price data
                enhanced_proposal = proposal.copy()
                enhanced_proposal.update({
                    "price_impact_analysis": {"error": "Missing date or DAO data"},
                    "overall_price_impact_pct": 0,
                    "price_data_available": False
                })
                enhanced_proposals.append(enhanced_proposal)
            
            # Rate limiting
            time.sleep(2)
        
        print(f"✅ Price impact analysis complete!")
        return enhanced_proposals

def integrate_price_data_with_existing_dataset(dataset_file: str, output_file: str):
    """Integrate price data with existing research dataset"""
    print(f"🔗 Integrating price data with {dataset_file}...")
    
    try:
        # Load existing dataset
        with open(dataset_file, "r", encoding="utf-8") as f:
            proposals = json.load(f)
        
        print(f"   Loaded {len(proposals)} proposals")
        
        # Initialize price analyzer
        analyzer = TokenPriceAnalyzer()
        
        # Analyze price impact
        enhanced_proposals = analyzer.batch_analyze_proposals(proposals)
        
        # Save enhanced dataset
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(enhanced_proposals, f, indent=2, ensure_ascii=False)
        
        # Save CSV version
        try:
            import pandas as pd
            df = pd.DataFrame(enhanced_proposals)
            df.to_csv(output_file.replace('.json', '.csv'), index=False, encoding="utf-8")
        except ImportError:
            pass
        
        print(f"✅ Enhanced dataset saved to {output_file}")
        
        # Summary statistics
        with_price_data = len([p for p in enhanced_proposals if p.get("price_data_available", False)])
        avg_price_impact = sum(p.get("overall_price_impact_pct", 0) for p in enhanced_proposals) / len(enhanced_proposals)
        
        print(f"📊 Price Integration Summary:")
        print(f"   Proposals with price data: {with_price_data}/{len(enhanced_proposals)}")
        print(f"   Average price impact: {avg_price_impact:.2f}%")
        
        return enhanced_proposals
        
    except Exception as e:
        print(f"❌ Price integration failed: {e}")
        return []

def main():
    """Test price data integration"""
    analyzer = TokenPriceAnalyzer()
    
    # Test with a sample proposal
    test_dao = "aave.eth"
    test_start = int(datetime(2024, 1, 1).timestamp())
    test_end = int(datetime(2024, 1, 7).timestamp())
    
    print(f"🧪 Testing price analysis for {test_dao}...")
    
    result = analyzer.analyze_proposal_price_impact(test_dao, test_start, test_end)
    print(f"Result: {json.dumps(result, indent=2)}")
    
    # Test integration with existing dataset
    print(f"\n🔗 Testing integration with existing dataset...")
    try:
        integrate_price_data_with_existing_dataset(
            "comprehensive_research_dataset.json",
            "price_enhanced_research_dataset.json"
        )
    except FileNotFoundError:
        print("   ⚠ No existing dataset found - run main collection first")

if __name__ == "__main__":
    main()
