# complete_price_data_analyzer.py - Analyze missing price data and get complete coverage
"""
Complete Price Data Analyzer to:
1. Identify which of the 150 activist proposals are missing price data
2. Use CoinGecko token list to find additional token mappings
3. Collect price data for all remaining proposals
"""

import pandas as pd
import os
import json
from typing import Dict, List, Set

class CompletePriceDataAnalyzer:
    """Analyzer to achieve 100% price data coverage"""
    
    def __init__(self):
        self.expanded_proposals_file = "immediate_expansion_data/expanded_activist_proposals_20250914_150333.csv"
        self.yahoo_price_dir = "yahoo_proposal_price_data"
        self.expanded_price_dir = "expanded_proposal_price_data"
        self.coingecko_file = "coingecko_tokens.csv"
        
        print(f"🔍 Complete Price Data Analyzer initialized")
        print(f"   Analyzing coverage for 150 activist proposals")
    
    def load_all_data(self):
        """Load all relevant data"""
        # Load expanded proposals
        self.proposals_df = pd.read_csv(self.expanded_proposals_file)
        print(f"   📥 Loaded {len(self.proposals_df)} activist proposals")
        
        # Load CoinGecko token list
        self.coingecko_df = pd.read_csv(self.coingecko_file, skiprows=1)
        print(f"   📥 Loaded {len(self.coingecko_df)} CoinGecko tokens")
        
        # Get completed proposals from both directories
        self.completed_proposals = self.get_completed_proposals()
        print(f"   ✅ Found {len(self.completed_proposals)} proposals with price data")
    
    def get_completed_proposals(self) -> Set[str]:
        """Get all proposal IDs that already have price data"""
        completed = set()
        
        # Check Yahoo price data directory
        if os.path.exists(self.yahoo_price_dir):
            for file in os.listdir(self.yahoo_price_dir):
                if file.endswith('.csv') and '_price_data.csv' in file:
                    # Extract proposal ID from filename
                    parts = file.replace('_price_data.csv', '').split('_', 1)
                    if len(parts) >= 2:
                        proposal_id = parts[1]
                        completed.add(proposal_id)
        
        # Check expanded price data directory
        if os.path.exists(self.expanded_price_dir):
            for file in os.listdir(self.expanded_price_dir):
                if file.endswith('.csv') and '_price_data.csv' in file:
                    # Extract proposal ID from filename
                    parts = file.replace('_price_data.csv', '').split('_', 1)
                    if len(parts) >= 2:
                        proposal_id = parts[1]
                        completed.add(proposal_id)
        
        return completed
    
    def analyze_missing_proposals(self):
        """Analyze which proposals are missing price data"""
        missing_proposals = []
        dao_coverage = {}
        
        for _, proposal in self.proposals_df.iterrows():
            proposal_id = proposal.get('proposal_id', proposal.get('id', ''))
            # Handle multiple DAO columns and NaN values
            dao = proposal.get('DAO', proposal.get('dao', ''))
            if pd.isna(dao) or dao == '':
                dao = proposal.get('dao', '')
            if pd.isna(dao) or dao == '':
                dao = 'unknown'
            
            # Track DAO coverage
            if dao not in dao_coverage:
                dao_coverage[dao] = {'total': 0, 'completed': 0, 'missing': 0}
            dao_coverage[dao]['total'] += 1
            
            if proposal_id not in self.completed_proposals:
                missing_proposals.append({
                    'proposal_id': proposal_id,
                    'dao': dao,
                    'title': proposal.get('title', proposal.get('Title', '')),
                    'activist_score': proposal.get('activist_score', 0)
                })
                dao_coverage[dao]['missing'] += 1
            else:
                dao_coverage[dao]['completed'] += 1
        
        print(f"\n📊 PRICE DATA COVERAGE ANALYSIS:")
        print(f"   📄 Total proposals: {len(self.proposals_df)}")
        print(f"   ✅ With price data: {len(self.completed_proposals)}")
        print(f"   ❌ Missing price data: {len(missing_proposals)}")
        print(f"   📈 Coverage rate: {len(self.completed_proposals)/len(self.proposals_df)*100:.1f}%")
        
        print(f"\n🏛️ DAO COVERAGE BREAKDOWN:")
        for dao, stats in sorted(dao_coverage.items(), key=lambda x: x[1]['missing'], reverse=True):
            coverage_pct = stats['completed'] / stats['total'] * 100 if stats['total'] > 0 else 0
            print(f"   {dao}: {stats['completed']}/{stats['total']} ({coverage_pct:.1f}%) - Missing: {stats['missing']}")
        
        return missing_proposals, dao_coverage
    
    def find_token_mappings(self, missing_proposals: List[Dict]) -> Dict:
        """Find CoinGecko token mappings for missing DAOs"""
        dao_tokens = {}
        
        # Extract unique DAOs from missing proposals
        missing_daos = list(set(p['dao'] for p in missing_proposals))
        
        print(f"\n🔍 SEARCHING FOR TOKEN MAPPINGS:")
        
        # Manual mappings for common DAO patterns
        dao_mapping_patterns = {
            'fei.eth': ['fei-usd', 'fei-protocol'],
            'cream-finance.eth': ['cream-2', 'cream'],
            'pickle.eth': ['pickle-finance'],
            'uma.eth': ['uma'],
            'ens.eth': ['ethereum-name-service'],
            'balancer.eth': ['balancer'],
            '1inch.eth': ['1inch'],
            'frax.eth': ['frax', 'frax-share'],
            'olympusdao.eth': ['olympus', 'olympus-v2'],
            'curve.eth': ['curve-dao-token'],
            'yearn': ['yearn-finance'],
            'colony.eth': ['colony'],
            'tokemak.eth': ['tokemak']
        }
        
        for dao in missing_daos:
            print(f"   🔍 Searching for {dao}...")
            
            # Check manual mappings first
            if dao in dao_mapping_patterns:
                for pattern in dao_mapping_patterns[dao]:
                    matches = self.coingecko_df[self.coingecko_df['Id (API id)'].str.contains(pattern, case=False, na=False)]
                    if not matches.empty:
                        token_info = matches.iloc[0]
                        dao_tokens[dao] = {
                            'coingecko_id': token_info['Id (API id)'],
                            'symbol': token_info['Symbol'],
                            'name': token_info['Name']
                        }
                        print(f"     ✅ Found: {token_info['Id (API id)']} ({token_info['Symbol']})")
                        break
            
            # If not found in manual mappings, search by DAO name
            if dao not in dao_tokens and dao != 'unknown' and not pd.isna(dao):
                # Try searching by DAO name parts
                dao_clean = str(dao).replace('.eth', '').replace('-', ' ')
                for part in dao_clean.split():
                    if len(part) > 2:  # Skip very short parts
                        matches = self.coingecko_df[
                            self.coingecko_df['Name'].str.contains(part, case=False, na=False) |
                            self.coingecko_df['Id (API id)'].str.contains(part, case=False, na=False)
                        ]
                        if not matches.empty:
                            token_info = matches.iloc[0]
                            dao_tokens[dao] = {
                                'coingecko_id': token_info['Id (API id)'],
                                'symbol': token_info['Symbol'],
                                'name': token_info['Name']
                            }
                            print(f"     ✅ Found: {token_info['Id (API id)']} ({token_info['Symbol']})")
                            break
            
            if dao not in dao_tokens:
                print(f"     ❌ No token found for {dao}")
        
        return dao_tokens
    
    def create_enhanced_token_mappings(self, dao_tokens: Dict) -> Dict:
        """Create enhanced token mappings with multiple data sources"""
        enhanced_mappings = {
            # Existing Yahoo Finance mappings
            "ens.eth": {
                "coingecko_id": "ethereum-name-service",
                "symbol": "ENS",
                "yahoo_symbol": "ENS-USD",
                "binance_symbol": "ENSUSDT"
            },
            "balancer.eth": {
                "coingecko_id": "balancer",
                "symbol": "BAL", 
                "yahoo_symbol": "BAL-USD",
                "binance_symbol": "BALUSDT"
            },
            "1inch.eth": {
                "coingecko_id": "1inch",
                "symbol": "1INCH",
                "yahoo_symbol": "1INCH-USD",
                "binance_symbol": "1INCHUSDT"
            },
            "frax.eth": {
                "coingecko_id": "frax",
                "symbol": "FRAX",
                "yahoo_symbol": "FRAX-USD",
                "binance_symbol": "FRAXUSDT"
            },
            "olympusdao.eth": {
                "coingecko_id": "olympus",
                "symbol": "OHM",
                "yahoo_symbol": "OHM-USD",
                "binance_symbol": "OHMUSDT"
            },
            "fei.eth": {
                "coingecko_id": "fei-usd",
                "symbol": "FEI",
                "yahoo_symbol": "FEI-USD",
                "binance_symbol": None
            },
            "cream-finance.eth": {
                "coingecko_id": "cream-2",
                "symbol": "CREAM",
                "yahoo_symbol": "CREAM-USD",
                "binance_symbol": "CREAMUSDT"
            },
            "pickle.eth": {
                "coingecko_id": "pickle-finance",
                "symbol": "PICKLE",
                "yahoo_symbol": "PICKLE-USD",
                "binance_symbol": None
            },
            "uma.eth": {
                "coingecko_id": "uma",
                "symbol": "UMA",
                "yahoo_symbol": "UMA-USD",
                "binance_symbol": "UMAUSDT"
            },
            "curve.eth": {
                "coingecko_id": "curve-dao-token",
                "symbol": "CRV",
                "yahoo_symbol": "CRV-USD",
                "binance_symbol": "CRVUSDT"
            },
            "yearn": {
                "coingecko_id": "yearn-finance",
                "symbol": "YFI",
                "yahoo_symbol": "YFI-USD",
                "binance_symbol": "YFIUSDT"
            }
        }
        
        # Add newly found tokens
        for dao, token_info in dao_tokens.items():
            if dao not in enhanced_mappings:
                enhanced_mappings[dao] = {
                    "coingecko_id": token_info['coingecko_id'],
                    "symbol": token_info['symbol'],
                    "yahoo_symbol": f"{token_info['symbol']}-USD",
                    "binance_symbol": f"{token_info['symbol']}USDT"
                }
        
        return enhanced_mappings
    
    def generate_completion_report(self, missing_proposals: List[Dict], dao_tokens: Dict):
        """Generate comprehensive completion report"""
        report = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "total_proposals": len(self.proposals_df),
            "completed_proposals": len(self.completed_proposals),
            "missing_proposals": len(missing_proposals),
            "coverage_percentage": len(self.completed_proposals) / len(self.proposals_df) * 100,
            "missing_by_dao": {},
            "token_mappings_found": len(dao_tokens),
            "potential_additional_coverage": 0
        }
        
        # Analyze missing by DAO
        for proposal in missing_proposals:
            dao = proposal['dao']
            if dao not in report["missing_by_dao"]:
                report["missing_by_dao"][dao] = []
            report["missing_by_dao"][dao].append({
                "proposal_id": proposal['proposal_id'],
                "title": proposal['title'],
                "activist_score": proposal['activist_score']
            })
        
        # Calculate potential additional coverage
        for dao, proposals in report["missing_by_dao"].items():
            if dao in dao_tokens:
                report["potential_additional_coverage"] += len(proposals)
        
        # Save report
        with open("price_data_completion_report.json", 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n📋 COMPLETION REPORT GENERATED:")
        print(f"   📄 Total proposals: {report['total_proposals']}")
        print(f"   ✅ Current coverage: {report['completed_proposals']} ({report['coverage_percentage']:.1f}%)")
        print(f"   🎯 Potential additional: {report['potential_additional_coverage']} proposals")
        print(f"   🔍 Token mappings found: {report['token_mappings_found']} DAOs")
        
        return report

def main():
    """Main execution function"""
    analyzer = CompletePriceDataAnalyzer()
    
    try:
        # Load all data
        analyzer.load_all_data()
        
        # Analyze missing proposals
        missing_proposals, dao_coverage = analyzer.analyze_missing_proposals()
        
        # Find token mappings for missing DAOs
        dao_tokens = analyzer.find_token_mappings(missing_proposals)
        
        # Create enhanced token mappings
        enhanced_mappings = analyzer.create_enhanced_token_mappings(dao_tokens)
        
        # Save enhanced mappings
        with open("enhanced_token_mappings.json", 'w') as f:
            json.dump(enhanced_mappings, f, indent=2)
        
        # Generate completion report
        report = analyzer.generate_completion_report(missing_proposals, dao_tokens)
        
        print(f"\n🎯 NEXT STEPS:")
        if missing_proposals:
            print(f"   1. Run enhanced price collector with new token mappings")
            print(f"   2. Target {len(missing_proposals)} remaining proposals")
            print(f"   3. Achieve {(len(analyzer.completed_proposals) + report['potential_additional_coverage']) / len(analyzer.proposals_df) * 100:.1f}% total coverage")
        else:
            print(f"   🎉 ALL PROPOSALS ALREADY HAVE PRICE DATA!")
        
        print(f"\n📁 Files generated:")
        print(f"   📊 enhanced_token_mappings.json")
        print(f"   📋 price_data_completion_report.json")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
