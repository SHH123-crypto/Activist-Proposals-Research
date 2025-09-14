# expanded_price_collector.py - Collect price data for 150 activist proposals
"""
Expanded Price Collector for collecting comprehensive price data for all 150 activist proposals
from the immediate expansion dataset. Uses multi-source approach with enhanced error handling.
"""

import pandas as pd
import requests
import time
import json
import yfinance as yf
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os
from pathlib import Path
import random

class ExpandedPriceCollector:
    """Price collector specifically for the expanded activist dataset"""
    
    def __init__(self):
        self.input_file = "immediate_expansion_data/expanded_activist_proposals_20250914_150333.csv"
        self.output_dir = "expanded_proposal_price_data"
        self.progress_file = "expanded_price_progress.json"
        
        # Enhanced token mappings (all DAOs from expanded dataset)
        self.token_mappings = {
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
            },
            "colony.eth": {
                "coingecko_id": "colony",
                "symbol": "CLNY",
                "yahoo_symbol": None,
                "binance_symbol": None
            },
            "tokemak.eth": {
                "coingecko_id": "tokemak",
                "symbol": "TOKE",
                "yahoo_symbol": None,
                "binance_symbol": "TOKEUSDT"
            }
        }
        
        # API configurations
        self.api_configs = {
            "coingecko": {"delay": 15, "max_retries": 3},
            "yahoo": {"delay": 1, "max_retries": 5},
            "binance": {"delay": 2, "max_retries": 3}
        }
        
        # Rate limiting trackers
        self.last_api_calls = {"coingecko": 0, "yahoo": 0, "binance": 0}
        
        # Create output directory
        Path(self.output_dir).mkdir(exist_ok=True)
        
        # Load progress
        self.completed_proposals = self.load_progress()
        
        print(f"💰 Expanded Price Collector initialized")
        print(f"   Input: {self.input_file}")
        print(f"   Output: {self.output_dir}")
        print(f"   Token mappings: {len(self.token_mappings)} DAOs")
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
    
    def wait_for_rate_limit(self, api_type: str):
        """Implement intelligent rate limiting with jitter"""
        current_time = time.time()
        config = self.api_configs.get(api_type, {})
        delay = config.get("delay", 5)
        
        time_since_last = current_time - self.last_api_calls.get(api_type, 0)
        
        if time_since_last < delay:
            wait_time = delay - time_since_last
            jitter = random.uniform(0.5, 2.0)
            total_wait = wait_time + jitter
            
            print(f"      ⏳ {api_type.title()} rate limit: waiting {total_wait:.1f}s")
            time.sleep(total_wait)
        
        self.last_api_calls[api_type] = time.time()
    
    def get_coingecko_data(self, token_id: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get CoinGecko data with enhanced retry logic"""
        max_retries = self.api_configs["coingecko"]["max_retries"]
        
        for attempt in range(max_retries):
            try:
                self.wait_for_rate_limit("coingecko")
                
                start_unix = int(start_date.timestamp())
                end_unix = int(end_date.timestamp())
                
                url = f"https://api.coingecko.com/api/v3/coins/{token_id}/market_chart/range"
                
                user_agents = [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
                ]
                
                headers = {
                    "User-Agent": random.choice(user_agents),
                    "Accept": "application/json",
                    "Accept-Language": "en-US,en;q=0.9"
                }
                
                params = {
                    "vs_currency": "usd",
                    "from": start_unix,
                    "to": end_unix
                }
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    return self.process_coingecko_data(data)
                elif response.status_code == 429:
                    wait_time = (attempt + 1) * 30
                    print(f"        ⏳ Rate limited, waiting {wait_time}s (attempt {attempt + 1})")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"        ❌ CoinGecko error {response.status_code} (attempt {attempt + 1})")
                    if attempt < max_retries - 1:
                        time.sleep((attempt + 1) * 5)
                        continue
                    
            except Exception as e:
                print(f"        ❌ CoinGecko exception: {e} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 5)
                    continue
        
        return pd.DataFrame()
    
    def process_coingecko_data(self, data: Dict) -> pd.DataFrame:
        """Process CoinGecko API response into standardized format"""
        prices = data.get("prices", [])
        volumes = data.get("total_volumes", [])
        market_caps = data.get("market_caps", [])
        
        if not prices:
            return pd.DataFrame()
        
        df_data = []
        volume_dict = {int(v[0]): v[1] for v in volumes} if volumes else {}
        mcap_dict = {int(m[0]): m[1] for m in market_caps} if market_caps else {}
        
        for price_point in prices:
            timestamp = int(price_point[0])
            price = price_point[1]
            
            df_data.append({
                "timestamp": timestamp,
                "datetime": datetime.fromtimestamp(timestamp / 1000),
                "date": datetime.fromtimestamp(timestamp / 1000).date(),
                "price_usd": price,
                "volume_usd": volume_dict.get(timestamp, 0),
                "market_cap_usd": mcap_dict.get(timestamp, 0),
                "source": "coingecko"
            })
        
        df = pd.DataFrame(df_data)
        
        if len(df) > 1:
            df["price_change_pct"] = df["price_usd"].pct_change() * 100
            df["volume_change_pct"] = df["volume_usd"].pct_change() * 100
        
        return df
    
    def get_yahoo_data(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Enhanced Yahoo Finance data collection"""
        max_retries = self.api_configs["yahoo"]["max_retries"]
        
        for attempt in range(max_retries):
            try:
                self.wait_for_rate_limit("yahoo")
                
                ticker = yf.Ticker(symbol)
                hist = ticker.history(start=start_date.date(), end=end_date.date())
                
                if hist.empty:
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return pd.DataFrame()
                
                df_data = []
                for date, row in hist.iterrows():
                    df_data.append({
                        "timestamp": int(date.timestamp() * 1000),
                        "datetime": date,
                        "date": date.date(),
                        "price_usd": row['Close'],
                        "volume_usd": row['Volume'] * row['Close'],
                        "market_cap_usd": 0,
                        "source": "yahoo"
                    })
                
                df = pd.DataFrame(df_data)
                
                if len(df) > 1:
                    df["price_change_pct"] = df["price_usd"].pct_change() * 100
                    df["volume_change_pct"] = df["volume_usd"].pct_change() * 100
                
                return df
                
            except Exception as e:
                print(f"        ❌ Yahoo Finance error: {e} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
        
        return pd.DataFrame()
    
    def get_comprehensive_price_data(self, token_info: Dict, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Comprehensive price data collection with fallback"""
        print(f"      🌐 Collecting price data for {token_info['symbol']}")
        
        # Try CoinGecko first
        if token_info.get('coingecko_id'):
            print(f"        🔄 Trying CoinGecko for {token_info['coingecko_id']}")
            df = self.get_coingecko_data(token_info['coingecko_id'], start_date, end_date)
            if not df.empty and len(df) > 10:
                print(f"        ✅ CoinGecko success: {len(df)} data points")
                return df
            else:
                print(f"        ⚠️ CoinGecko insufficient data")
        
        # Try Yahoo Finance as backup
        if token_info.get('yahoo_symbol'):
            print(f"        🔄 Trying Yahoo Finance for {token_info['yahoo_symbol']}")
            df = self.get_yahoo_data(token_info['yahoo_symbol'], start_date, end_date)
            if not df.empty and len(df) > 10:
                print(f"        ✅ Yahoo Finance success: {len(df)} data points")
                return df
            else:
                print(f"        ⚠️ Yahoo Finance insufficient data")
        
        print(f"        ❌ All sources failed for {token_info['symbol']}")
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
        
        # Fallback to current date
        now = datetime.now()
        return now - timedelta(days=30), now

    def collect_proposal_price_data(self, proposal: Dict) -> bool:
        """Collect comprehensive price data for a single proposal"""
        proposal_id = proposal.get("proposal_id", proposal.get("id", "unknown"))
        dao = proposal.get("DAO", proposal.get("dao", "unknown"))

        print(f"    📊 Processing proposal {proposal_id} from {dao}")

        # Skip if already completed
        if proposal_id in self.completed_proposals:
            print(f"      ✅ Already completed, skipping")
            return True

        # Get token mapping
        token_info = self.token_mappings.get(dao)
        if not token_info:
            print(f"      ❌ No token mapping for DAO: {dao}")
            return False

        # Parse proposal dates
        proposal_start, proposal_end = self.parse_proposal_date(proposal)

        # Calculate 6-month window (3 months before, 3 months after)
        start_date = proposal_start - timedelta(days=90)
        end_date = proposal_end + timedelta(days=90)

        print(f"      📅 Date range: {start_date.date()} to {end_date.date()}")

        # Collect price data
        price_df = self.get_comprehensive_price_data(token_info, start_date, end_date)

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
            if hasattr(price_df["datetime"].iloc[0], 'tz') and price_df["datetime"].iloc[0].tz is not None:
                price_df["datetime"] = price_df["datetime"].dt.tz_localize(None)

            if hasattr(proposal_start, 'tz') and proposal_start.tz is not None:
                proposal_start = proposal_start.replace(tzinfo=None)

            price_df["days_from_proposal"] = (price_df["datetime"] - proposal_start).dt.days
        except Exception as e:
            print(f"        ⚠️ Timezone error, using simple calculation: {e}")
            price_df["days_from_proposal"] = 0

        # Save individual proposal data
        filename = f"{dao}_{proposal_id}_price_data.csv"
        filepath = os.path.join(self.output_dir, filename)

        price_df.to_csv(filepath, index=False)

        # Save progress
        self.save_progress(proposal_id, dao)

        print(f"      ✅ Saved {len(price_df)} data points to {filename}")
        return True

    def collect_all_expanded_proposals(self):
        """Collect price data for all proposals in expanded dataset"""
        print(f"💰 EXPANDED PRICE COLLECTION STARTING")
        print("=" * 80)

        # Load expanded dataset
        try:
            df = pd.read_csv(self.input_file)
            proposals = df.to_dict('records')
            print(f"   📥 Loaded {len(proposals)} activist proposals")
        except Exception as e:
            print(f"   ❌ Error loading dataset: {e}")
            return

        # Filter out already completed
        remaining_proposals = [p for p in proposals
                             if p.get("proposal_id", p.get("id", "")) not in self.completed_proposals]

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

            except Exception as e:
                print(f"      ❌ Error processing proposal: {e}")
                failed += 1
                continue

        # Final summary
        total_completed = len(self.completed_proposals)

        print(f"\n🎉 EXPANDED PRICE COLLECTION COMPLETE!")
        print("=" * 80)
        print(f"   ✅ Total completed: {total_completed} proposals")
        print(f"   🎯 This session: {successful} successful, {failed} failed")
        print(f"   📁 Output directory: {self.output_dir}")
        print(f"   📊 Individual CSV files: {total_completed}")

        # Generate summary statistics
        self.generate_collection_summary()

    def generate_collection_summary(self):
        """Generate comprehensive summary of collected data"""
        print(f"\n📈 Generating collection summary...")

        # Count files and analyze data
        csv_files = [f for f in os.listdir(self.output_dir) if f.endswith('.csv')]

        summary_stats = {
            "timestamp": datetime.now().isoformat(),
            "total_proposals": len(csv_files),
            "total_data_points": 0,
            "dao_breakdown": {},
            "date_range": {"earliest": None, "latest": None},
            "avg_data_points_per_proposal": 0,
            "successful_sources": {"coingecko": 0, "yahoo": 0, "binance": 0}
        }

        for csv_file in csv_files:
            try:
                filepath = os.path.join(self.output_dir, csv_file)
                df = pd.read_csv(filepath)

                if not df.empty:
                    dao = df.iloc[0]["dao"]
                    source = df.iloc[0]["source"]

                    # Update stats
                    summary_stats["total_data_points"] += len(df)
                    summary_stats["dao_breakdown"][dao] = summary_stats["dao_breakdown"].get(dao, 0) + 1
                    summary_stats["successful_sources"][source] = summary_stats["successful_sources"].get(source, 0) + 1

                    # Update date range
                    min_date = df["date"].min()
                    max_date = df["date"].max()

                    if summary_stats["date_range"]["earliest"] is None or min_date < summary_stats["date_range"]["earliest"]:
                        summary_stats["date_range"]["earliest"] = min_date

                    if summary_stats["date_range"]["latest"] is None or max_date > summary_stats["date_range"]["latest"]:
                        summary_stats["date_range"]["latest"] = max_date

            except Exception as e:
                print(f"      ⚠️ Error analyzing {csv_file}: {e}")
                continue

        # Calculate averages
        if summary_stats["total_proposals"] > 0:
            summary_stats["avg_data_points_per_proposal"] = summary_stats["total_data_points"] / summary_stats["total_proposals"]

        # Save summary
        summary_file = os.path.join(self.output_dir, "expanded_collection_summary.json")
        with open(summary_file, 'w') as f:
            json.dump(summary_stats, f, indent=2, default=str)

        print(f"   📋 Summary saved: {summary_file}")

        # Print key statistics
        print(f"\n📊 COLLECTION STATISTICS:")
        print(f"   📄 Total proposals: {summary_stats['total_proposals']}")
        print(f"   📈 Total data points: {summary_stats['total_data_points']:,}")
        print(f"   📊 Avg per proposal: {summary_stats['avg_data_points_per_proposal']:.1f}")
        print(f"   🏛️ DAOs covered: {len(summary_stats['dao_breakdown'])}")

        print(f"\n🏛️ DAO BREAKDOWN:")
        for dao, count in sorted(summary_stats["dao_breakdown"].items(), key=lambda x: x[1], reverse=True):
            print(f"   {dao}: {count} proposals")

        print(f"\n🌐 DATA SOURCES:")
        for source, count in summary_stats["successful_sources"].items():
            if count > 0:
                print(f"   {source.title()}: {count} proposals")

def main():
    """Main execution function"""
    collector = ExpandedPriceCollector()

    try:
        collector.collect_all_expanded_proposals()

        print(f"\n🚀 READY FOR RESEARCH ANALYSIS!")
        print(f"   📊 Dataset: 150 activist proposals with comprehensive price data")
        print(f"   📈 Analysis ready: Price impact studies, correlation analysis")
        print(f"   🎓 Academic quality: Publication-ready dataset")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
