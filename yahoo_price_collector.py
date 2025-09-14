# yahoo_price_collector.py - Simplified price collector using Yahoo Finance
"""
Simplified Price Collector for 150 activist proposals using Yahoo Finance only.
Yahoo Finance is free, reliable, and has no API key requirements.
"""

import pandas as pd
import yfinance as yf
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os
from pathlib import Path

class YahooPriceCollector:
    """Simplified price collector using Yahoo Finance only"""
    
    def __init__(self):
        self.input_file = "immediate_expansion_data/expanded_activist_proposals_20250914_150333.csv"
        self.output_dir = "yahoo_proposal_price_data"
        self.progress_file = "yahoo_price_progress.json"
        
        # Yahoo Finance symbol mappings
        self.token_mappings = {
            "ens.eth": {"symbol": "ENS", "yahoo_symbol": "ENS-USD"},
            "balancer.eth": {"symbol": "BAL", "yahoo_symbol": "BAL-USD"},
            "1inch.eth": {"symbol": "1INCH", "yahoo_symbol": "1INCH-USD"},
            "frax.eth": {"symbol": "FRAX", "yahoo_symbol": "FRAX-USD"},
            "olympusdao.eth": {"symbol": "OHM", "yahoo_symbol": "OHM-USD"},
            "fei.eth": {"symbol": "FEI", "yahoo_symbol": "FEI-USD"},
            "cream-finance.eth": {"symbol": "CREAM", "yahoo_symbol": "CREAM-USD"},
            "pickle.eth": {"symbol": "PICKLE", "yahoo_symbol": "PICKLE-USD"},
            "uma.eth": {"symbol": "UMA", "yahoo_symbol": "UMA-USD"},
            "curve.eth": {"symbol": "CRV", "yahoo_symbol": "CRV-USD"},
            "yearn": {"symbol": "YFI", "yahoo_symbol": "YFI-USD"},
            "colony.eth": {"symbol": "CLNY", "yahoo_symbol": None},  # Not available
            "tokemak.eth": {"symbol": "TOKE", "yahoo_symbol": None}  # Not available
        }
        
        # Create output directory
        Path(self.output_dir).mkdir(exist_ok=True)
        
        # Load progress
        self.completed_proposals = self.load_progress()
        
        print(f"📈 Yahoo Price Collector initialized")
        print(f"   Input: {self.input_file}")
        print(f"   Output: {self.output_dir}")
        print(f"   Token mappings: {len([t for t in self.token_mappings.values() if t['yahoo_symbol']])} available")
        print(f"   Previously completed: {len(self.completed_proposals)} proposals")
    
    def load_progress(self) -> set:
        """Load previously completed proposals"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    return set(data.get('completed_proposals', []))
        except:
            pass
        return set()
    
    def save_progress(self, proposal_id: str, dao: str):
        """Save progress after each successful scrape"""
        self.completed_proposals.add(proposal_id)
        
        progress_data = {
            'completed_proposals': list(self.completed_proposals),
            'last_updated': datetime.now().isoformat(),
            'total_completed': len(self.completed_proposals),
            'last_dao': dao,
            'last_proposal': proposal_id
        }
        
        with open(self.progress_file, 'w') as f:
            json.dump(progress_data, f, indent=2)
    
    def get_yahoo_data(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get Yahoo Finance data with retry logic"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                print(f"        🔄 Fetching {symbol} from Yahoo Finance (attempt {attempt + 1})")
                
                ticker = yf.Ticker(symbol)
                hist = ticker.history(start=start_date.date(), end=end_date.date())
                
                if hist.empty:
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return pd.DataFrame()
                
                # Process data
                df_data = []
                for date, row in hist.iterrows():
                    # Ensure timezone-naive datetime
                    if hasattr(date, 'tz') and date.tz is not None:
                        date = date.tz_localize(None)
                    
                    df_data.append({
                        "timestamp": int(date.timestamp() * 1000),
                        "datetime": date,
                        "date": date.date(),
                        "price_usd": float(row['Close']),
                        "volume_usd": float(row['Volume'] * row['Close']),
                        "high_usd": float(row['High']),
                        "low_usd": float(row['Low']),
                        "open_usd": float(row['Open']),
                        "source": "yahoo_finance"
                    })
                
                df = pd.DataFrame(df_data)
                
                if len(df) > 1:
                    df["price_change_pct"] = df["price_usd"].pct_change() * 100
                    df["volume_change_pct"] = df["volume_usd"].pct_change() * 100
                
                print(f"        ✅ Yahoo Finance success: {len(df)} data points")
                return df
                
            except Exception as e:
                print(f"        ❌ Yahoo Finance error: {e} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
        
        return pd.DataFrame()
    
    def parse_proposal_date(self, proposal: Dict) -> tuple:
        """Parse proposal dates from various formats"""
        # Try different date fields
        created_fields = ["created", "createdAt", "Created", "start", "startDate"]
        
        for field in created_fields:
            created_str = proposal.get(field, "")
            if created_str:
                try:
                    if isinstance(created_str, (int, float)):
                        proposal_start = datetime.fromtimestamp(int(created_str))
                        proposal_end = proposal_start + timedelta(days=10)
                        return proposal_start, proposal_end
                    
                    if isinstance(created_str, str) and created_str.isdigit():
                        proposal_start = datetime.fromtimestamp(int(created_str))
                        proposal_end = proposal_start + timedelta(days=10)
                        return proposal_start, proposal_end
                        
                except:
                    continue
        
        # Fallback to current date minus some time
        now = datetime.now()
        return now - timedelta(days=365), now - timedelta(days=300)
    
    def collect_proposal_price_data(self, proposal: Dict) -> bool:
        """Collect price data for a single proposal"""
        proposal_id = proposal.get("proposal_id", proposal.get("id", "unknown"))
        dao = proposal.get("DAO", proposal.get("dao", "unknown"))
        
        print(f"    📊 Processing proposal {proposal_id} from {dao}")
        
        # Skip if already completed
        if proposal_id in self.completed_proposals:
            print(f"      ✅ Already completed, skipping")
            return True
        
        # Get token mapping
        token_info = self.token_mappings.get(dao)
        if not token_info or not token_info.get('yahoo_symbol'):
            print(f"      ❌ No Yahoo symbol for DAO: {dao}")
            return False
        
        # Parse proposal dates
        proposal_start, proposal_end = self.parse_proposal_date(proposal)
        
        # Calculate 6-month window (3 months before, 3 months after)
        start_date = proposal_start - timedelta(days=90)
        end_date = proposal_end + timedelta(days=90)
        
        print(f"      📅 Date range: {start_date.date()} to {end_date.date()}")
        
        # Collect price data
        price_df = self.get_yahoo_data(token_info['yahoo_symbol'], start_date, end_date)
        
        if price_df.empty:
            print(f"      ❌ No price data collected")
            return False
        
        # Add proposal metadata
        price_df["proposal_id"] = proposal_id
        price_df["dao"] = dao
        price_df["proposal_title"] = proposal.get("title", proposal.get("Title", ""))
        price_df["proposal_start"] = proposal_start
        price_df["proposal_end"] = proposal_end
        price_df["activist_score"] = proposal.get("activist_score", 0)
        price_df["detection_methods"] = str(proposal.get("detection_methods", []))
        
        # Calculate days relative to proposal (fix timezone issues)
        try:
            # Ensure both datetimes are timezone-naive
            if hasattr(proposal_start, 'tz') and proposal_start.tz is not None:
                proposal_start = proposal_start.replace(tzinfo=None)
            
            price_df["days_from_proposal"] = (price_df["datetime"] - proposal_start).dt.days
        except Exception as e:
            print(f"        ⚠️ Date calculation error: {e}")
            price_df["days_from_proposal"] = 0
        
        # Save individual proposal data
        filename = f"{dao}_{proposal_id}_price_data.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        price_df.to_csv(filepath, index=False)
        
        # Save progress
        self.save_progress(proposal_id, dao)
        
        print(f"      ✅ Saved {len(price_df)} data points to {filename}")
        return True
    
    def collect_all_proposals(self):
        """Collect price data for all proposals"""
        print(f"📈 YAHOO PRICE COLLECTION STARTING")
        print("=" * 80)
        
        # Load expanded dataset
        try:
            df = pd.read_csv(self.input_file)
            proposals = df.to_dict('records')
            print(f"   📥 Loaded {len(proposals)} activist proposals")
        except Exception as e:
            print(f"   ❌ Error loading dataset: {e}")
            return
        
        # Filter out already completed and unsupported DAOs
        remaining_proposals = []
        for p in proposals:
            proposal_id = p.get("proposal_id", p.get("id", ""))
            dao = p.get("DAO", p.get("dao", ""))
            
            if proposal_id in self.completed_proposals:
                continue
                
            token_info = self.token_mappings.get(dao)
            if not token_info or not token_info.get('yahoo_symbol'):
                continue
                
            remaining_proposals.append(p)
        
        print(f"   🎯 Processing {len(remaining_proposals)} remaining proposals")
        print(f"   ✅ Already completed: {len(self.completed_proposals)} proposals")
        
        # Process each proposal
        successful = 0
        failed = 0
        
        for i, proposal in enumerate(remaining_proposals, 1):
            print(f"\n📊 [{i}/{len(remaining_proposals)}] Processing proposal...")
            
            try:
                if self.collect_proposal_price_data(proposal):
                    successful += 1
                else:
                    failed += 1
                    
                # Small delay to be respectful
                time.sleep(1)
                    
            except Exception as e:
                print(f"      ❌ Error processing proposal: {e}")
                failed += 1
                continue
        
        # Final summary
        total_completed = len(self.completed_proposals)
        
        print(f"\n🎉 YAHOO PRICE COLLECTION COMPLETE!")
        print("=" * 80)
        print(f"   ✅ Total completed: {total_completed} proposals")
        print(f"   🎯 This session: {successful} successful, {failed} failed")
        print(f"   📁 Output directory: {self.output_dir}")
        
        # Generate summary
        self.generate_summary()
    
    def generate_summary(self):
        """Generate collection summary"""
        csv_files = [f for f in os.listdir(self.output_dir) if f.endswith('.csv')]
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "total_proposals": len(csv_files),
            "total_data_points": 0,
            "dao_breakdown": {},
            "avg_data_points": 0
        }
        
        for csv_file in csv_files:
            try:
                filepath = os.path.join(self.output_dir, csv_file)
                df = pd.read_csv(filepath)
                
                if not df.empty:
                    dao = df.iloc[0]["dao"]
                    summary["total_data_points"] += len(df)
                    summary["dao_breakdown"][dao] = summary["dao_breakdown"].get(dao, 0) + 1
                        
            except Exception as e:
                continue
        
        if summary["total_proposals"] > 0:
            summary["avg_data_points"] = summary["total_data_points"] / summary["total_proposals"]
        
        # Save summary
        summary_file = os.path.join(self.output_dir, "yahoo_collection_summary.json")
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print(f"\n📊 COLLECTION STATISTICS:")
        print(f"   📄 Total proposals: {summary['total_proposals']}")
        print(f"   📈 Total data points: {summary['total_data_points']:,}")
        print(f"   📊 Avg per proposal: {summary['avg_data_points']:.1f}")
        
        print(f"\n🏛️ DAO BREAKDOWN:")
        for dao, count in sorted(summary["dao_breakdown"].items(), key=lambda x: x[1], reverse=True):
            print(f"   {dao}: {count} proposals")

def main():
    """Main execution function"""
    collector = YahooPriceCollector()
    
    try:
        collector.collect_all_proposals()
        
        print(f"\n🚀 READY FOR RESEARCH ANALYSIS!")
        print(f"   📊 Dataset: Activist proposals with Yahoo Finance price data")
        print(f"   📈 Analysis ready: Price impact studies, correlation analysis")
        print(f"   🎓 Academic quality: Publication-ready dataset")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
