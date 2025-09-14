# ultimate_comprehensive_scraper.py - Maximum coverage with enhanced activist filtering
"""
Ultimate comprehensive scraper for maximum proposal coverage with:
- Enhanced activist proposal detection (beyond keywords)
- Multiple data source fallbacks
- Anti-bot detection measures
- Maximum DAO coverage
- Robust price data collection
"""

import requests
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
import os
from pathlib import Path
import yfinance as yf
import random
import re
from textblob import TextBlob
import numpy as np

class UltimateComprehensiveScraper:
    """Ultimate scraper for maximum proposal coverage with enhanced activist detection"""
    
    def __init__(self):
        self.output_dir = "ultimate_proposal_data"
        self.progress_file = "ultimate_progress.json"
        
        # Enhanced token mappings for ALL DAOs
        self.token_mappings = {
            # Major DeFi protocols
            "ens.eth": {
                "coingecko_id": "ethereum-name-service",
                "symbol": "ENS",
                "yahoo_symbol": "ENS-USD",
                "binance_symbol": "ENSUSDT",
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            },
            "balancer.eth": {
                "coingecko_id": "balancer",
                "symbol": "BAL", 
                "yahoo_symbol": "BAL-USD",
                "binance_symbol": "BALUSDT",
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            },
            "uniswap": {
                "coingecko_id": "uniswap",
                "symbol": "UNI",
                "yahoo_symbol": "UNI-USD", 
                "binance_symbol": "UNIUSDT",
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            },
            "1inch.eth": {
                "coingecko_id": "1inch",
                "symbol": "1INCH",
                "yahoo_symbol": "1INCH-USD",
                "binance_symbol": "1INCHUSDT",
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            },
            "frax.eth": {
                "coingecko_id": "frax",
                "symbol": "FRAX",
                "yahoo_symbol": "FRAX-USD",
                "binance_symbol": "FRAXUSDT",
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            },
            "olympusdao.eth": {
                "coingecko_id": "olympus",
                "symbol": "OHM",
                "yahoo_symbol": "OHM-USD",
                "binance_symbol": "OHMUSDT",
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            },
            "fei.eth": {
                "coingecko_id": "fei-usd",
                "symbol": "FEI",
                "yahoo_symbol": "FEI-USD",
                "binance_symbol": None,
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            },
            "cream-finance.eth": {
                "coingecko_id": "cream-2",
                "symbol": "CREAM",
                "yahoo_symbol": "CREAM-USD",
                "binance_symbol": "CREAMUSDT",
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            },
            "pickle.eth": {
                "coingecko_id": "pickle-finance",
                "symbol": "PICKLE",
                "yahoo_symbol": "PICKLE-USD",
                "binance_symbol": None,
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            },
            "uma.eth": {
                "coingecko_id": "uma",
                "symbol": "UMA",
                "yahoo_symbol": "UMA-USD",
                "binance_symbol": "UMAUSDT",
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            },
            "curve.eth": {
                "coingecko_id": "curve-dao-token",
                "symbol": "CRV",
                "yahoo_symbol": "CRV-USD",
                "binance_symbol": "CRVUSDT",
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            },
            "yearn": {
                "coingecko_id": "yearn-finance",
                "symbol": "YFI",
                "yahoo_symbol": "YFI-USD",
                "binance_symbol": "YFIUSDT",
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            },
            "colony.eth": {
                "coingecko_id": "colony",
                "symbol": "CLNY",
                "yahoo_symbol": None,
                "binance_symbol": None,
                "alternative_apis": ["coinmarketcap"]
            },
            "tokemak.eth": {
                "coingecko_id": "tokemak",
                "symbol": "TOKE",
                "yahoo_symbol": None,
                "binance_symbol": "TOKEUSDT",
                "alternative_apis": ["coinmarketcap", "cryptocompare"]
            }
        }
        
        # Enhanced activist detection patterns
        self.enhanced_activist_patterns = {
            # Governance structure changes
            "governance_change": [
                r"change.*governance", r"modify.*voting", r"alter.*constitution",
                r"amend.*charter", r"restructure.*dao", r"reform.*protocol",
                r"governance.*upgrade", r"voting.*mechanism", r"consensus.*change"
            ],
            
            # Financial/treasury activism
            "financial_activism": [
                r"treasury.*allocation", r"fund.*reallocation", r"budget.*revision",
                r"spending.*proposal", r"investment.*strategy", r"diversify.*treasury",
                r"liquidate.*position", r"sell.*tokens", r"buy.*back"
            ],
            
            # Protocol changes
            "protocol_activism": [
                r"protocol.*change", r"parameter.*adjustment", r"fee.*modification",
                r"reward.*restructure", r"emission.*change", r"tokenomics.*update",
                r"upgrade.*contract", r"migrate.*protocol"
            ],
            
            # Leadership/team changes
            "leadership_change": [
                r"remove.*team", r"replace.*lead", r"elect.*new", r"dismiss.*member",
                r"hire.*external", r"change.*management", r"new.*steward"
            ],
            
            # Emergency/urgent actions
            "emergency_action": [
                r"emergency.*proposal", r"urgent.*action", r"immediate.*response",
                r"crisis.*management", r"halt.*protocol", r"pause.*contract"
            ],
            
            # Community-driven initiatives
            "community_initiative": [
                r"community.*proposal", r"grassroots.*initiative", r"member.*driven",
                r"collective.*action", r"decentralized.*decision", r"bottom.*up"
            ]
        }
        
        # Sentiment analysis for activist detection
        self.activist_sentiment_indicators = [
            "should", "must", "need to", "propose", "change", "improve",
            "reform", "restructure", "demand", "require", "urgent", "critical"
        ]
        
        # API endpoints and configurations
        self.api_configs = {
            "coingecko": {
                "base_url": "https://api.coingecko.com/api/v3",
                "delay": 15,  # Increased delay
                "max_retries": 3
            },
            "yahoo": {
                "delay": 1,
                "max_retries": 5
            },
            "binance": {
                "base_url": "https://api.binance.com/api/v3",
                "delay": 2,
                "max_retries": 3
            }
        }
        
        # Rate limiting trackers
        self.last_api_calls = {
            "coingecko": 0,
            "yahoo": 0,
            "binance": 0
        }
        
        # Create output directory
        Path(self.output_dir).mkdir(exist_ok=True)
        
        # Load progress
        self.completed_proposals = self.load_progress()
        
        print(f"🚀 Ultimate Comprehensive Scraper initialized")
        print(f"   Output directory: {self.output_dir}")
        print(f"   Token mappings: {len(self.token_mappings)} DAOs")
        print(f"   Enhanced activist detection: {sum(len(patterns) for patterns in self.enhanced_activist_patterns.values())} patterns")
        print(f"   Previously completed: {len(self.completed_proposals)} proposals")
    
    def enhanced_activist_detection(self, proposal: Dict) -> Tuple[float, List[str], str]:
        """Enhanced activist proposal detection using multiple methods"""
        title = str(proposal.get("title", proposal.get("Title", ""))).lower()
        body = str(proposal.get("body", proposal.get("Body", ""))).lower()
        
        combined_text = f"{title} {body}"
        
        activist_score = 0.0
        detection_methods = []
        
        # Method 1: Enhanced pattern matching
        pattern_score = 0
        matched_categories = []
        
        for category, patterns in self.enhanced_activist_patterns.items():
            category_matches = 0
            for pattern in patterns:
                if re.search(pattern, combined_text, re.IGNORECASE):
                    category_matches += 1
            
            if category_matches > 0:
                pattern_score += min(category_matches * 0.15, 0.3)  # Cap per category
                matched_categories.append(category)
        
        activist_score += min(pattern_score, 0.6)  # Cap total pattern score
        
        if matched_categories:
            detection_methods.append(f"pattern_matching_{'+'.join(matched_categories)}")
        
        # Method 2: Sentiment analysis
        try:
            blob = TextBlob(combined_text)
            sentiment_score = 0
            
            # Check for activist sentiment indicators
            for indicator in self.activist_sentiment_indicators:
                if indicator in combined_text:
                    sentiment_score += 0.05
            
            # Polarity analysis (controversial proposals often have strong sentiment)
            if abs(blob.sentiment.polarity) > 0.3:
                sentiment_score += 0.1
            
            activist_score += min(sentiment_score, 0.25)
            
            if sentiment_score > 0:
                detection_methods.append("sentiment_analysis")
        
        except:
            pass
        
        # Method 3: Structural analysis
        structural_score = 0
        
        # Long proposals often indicate detailed activist initiatives
        if len(combined_text) > 1000:
            structural_score += 0.1
        
        # Multiple sections/bullet points indicate structured proposals
        if combined_text.count('\n') > 5 or combined_text.count('•') > 3:
            structural_score += 0.05
        
        # Proposals with numbers/percentages often involve parameter changes
        if re.search(r'\d+%|\$\d+|parameter|threshold|limit', combined_text):
            structural_score += 0.1
        
        activist_score += min(structural_score, 0.15)
        
        if structural_score > 0:
            detection_methods.append("structural_analysis")
        
        # Normalize score to 0-1 range
        activist_score = min(activist_score, 1.0)
        
        # Determine detection method summary
        if not detection_methods:
            method_summary = "no_detection"
        elif len(detection_methods) == 1:
            method_summary = detection_methods[0]
        else:
            method_summary = f"multi_method_{len(detection_methods)}"
        
        return activist_score, detection_methods, method_summary
    
    def load_progress(self) -> Set[str]:
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
            # Add random jitter to avoid synchronized requests
            jitter = random.uniform(0.5, 2.0)
            total_wait = wait_time + jitter
            
            print(f"      ⏳ {api_type.title()} rate limit: waiting {total_wait:.1f}s")
            time.sleep(total_wait)
        
        self.last_api_calls[api_type] = time.time()
    
    def get_coingecko_data_with_retry(self, token_id: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get CoinGecko data with enhanced retry logic"""
        max_retries = self.api_configs["coingecko"]["max_retries"]
        
        for attempt in range(max_retries):
            try:
                self.wait_for_rate_limit("coingecko")
                
                start_unix = int(start_date.timestamp())
                end_unix = int(end_date.timestamp())
                
                url = f"{self.api_configs['coingecko']['base_url']}/coins/{token_id}/market_chart/range"
                
                # Rotate user agents to avoid detection
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
                    # Rate limited - wait longer
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
        
        # Create DataFrame
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
        
        # Calculate additional metrics
        if len(df) > 1:
            df["price_change_pct"] = df["price_usd"].pct_change() * 100
            df["volume_change_pct"] = df["volume_usd"].pct_change() * 100
        
        return df

    def get_yahoo_data_enhanced(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Enhanced Yahoo Finance data collection with retry logic"""
        max_retries = self.api_configs["yahoo"]["max_retries"]

        for attempt in range(max_retries):
            try:
                self.wait_for_rate_limit("yahoo")

                print(f"        📊 Yahoo Finance attempt {attempt + 1} for {symbol}")

                ticker = yf.Ticker(symbol)
                hist = ticker.history(start=start_date.date(), end=end_date.date())

                if hist.empty:
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    return pd.DataFrame()

                # Convert to our format
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

                print(f"        ✅ Yahoo Finance: {len(df)} data points")
                return df

            except Exception as e:
                print(f"        ❌ Yahoo Finance error: {e} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue

        return pd.DataFrame()

    def get_binance_data_enhanced(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Enhanced Binance data collection with retry logic"""
        if not symbol:
            return pd.DataFrame()

        max_retries = self.api_configs["binance"]["max_retries"]

        for attempt in range(max_retries):
            try:
                self.wait_for_rate_limit("binance")

                print(f"        📈 Binance attempt {attempt + 1} for {symbol}")

                url = f"{self.api_configs['binance']['base_url']}/klines"
                params = {
                    "symbol": symbol,
                    "interval": "1d",
                    "startTime": int(start_date.timestamp() * 1000),
                    "endTime": int(end_date.timestamp() * 1000),
                    "limit": 1000
                }

                response = requests.get(url, params=params, timeout=30)

                if response.status_code == 200:
                    data = response.json()

                    if not data:
                        if attempt < max_retries - 1:
                            time.sleep(2 ** attempt)
                            continue
                        return pd.DataFrame()

                    # Process Binance data
                    df_data = []
                    for kline in data:
                        timestamp = int(kline[0])
                        close_price = float(kline[4])
                        volume = float(kline[5])

                        df_data.append({
                            "timestamp": timestamp,
                            "datetime": datetime.fromtimestamp(timestamp / 1000),
                            "date": datetime.fromtimestamp(timestamp / 1000).date(),
                            "price_usd": close_price,
                            "volume_usd": volume * close_price,
                            "market_cap_usd": 0,
                            "source": "binance"
                        })

                    df = pd.DataFrame(df_data)

                    if len(df) > 1:
                        df["price_change_pct"] = df["price_usd"].pct_change() * 100
                        df["volume_change_pct"] = df["volume_usd"].pct_change() * 100

                    print(f"        ✅ Binance: {len(df)} data points")
                    return df

                else:
                    print(f"        ❌ Binance error {response.status_code} (attempt {attempt + 1})")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue

            except Exception as e:
                print(f"        ❌ Binance error: {e} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue

        return pd.DataFrame()

    def get_comprehensive_price_data(self, token_info: Dict, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Comprehensive price data collection with all available sources"""
        print(f"      🌐 Comprehensive data collection for {token_info['symbol']}")

        # Define all available sources in priority order
        sources = []

        # Primary: CoinGecko (most comprehensive)
        if token_info.get('coingecko_id'):
            sources.append(('coingecko', token_info['coingecko_id'], self.get_coingecko_data_with_retry))

        # Secondary: Yahoo Finance (reliable for major tokens)
        if token_info.get('yahoo_symbol'):
            sources.append(('yahoo', token_info['yahoo_symbol'], self.get_yahoo_data_enhanced))

        # Tertiary: Binance (good for trading data)
        if token_info.get('binance_symbol'):
            sources.append(('binance', token_info['binance_symbol'], self.get_binance_data_enhanced))

        # Try each source with comprehensive error handling
        for source_type, identifier, get_data_func in sources:
            try:
                print(f"        🔄 Trying {source_type} for {identifier}")

                df = get_data_func(identifier, start_date, end_date)

                if not df.empty and len(df) > 10:  # Require minimum data points
                    print(f"        ✅ Success with {source_type}: {len(df)} data points")
                    return df
                else:
                    print(f"        ⚠️ {source_type} returned insufficient data ({len(df) if not df.empty else 0} points)")

            except Exception as e:
                print(f"        ❌ {source_type} failed with exception: {e}")
                continue

        print(f"        ❌ All sources failed for {token_info['symbol']}")
        return pd.DataFrame()

    def parse_proposal_date_enhanced(self, proposal: Dict) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Enhanced proposal date parsing with multiple fallback methods"""

        # Method 1: Try Unix timestamps
        start_unix = proposal.get("start_date", proposal.get("Start (Unix)", 0))
        end_unix = proposal.get("end_date", proposal.get("End (Unix)", 0))

        if start_unix and end_unix:
            try:
                proposal_start = datetime.fromtimestamp(int(start_unix))
                proposal_end = datetime.fromtimestamp(int(end_unix))
                return proposal_start, proposal_end
            except:
                pass

        # Method 2: Try created date with voting period estimation
        created_fields = ["created", "createdAt", "Created", "start", "startDate"]

        for field in created_fields:
            created_str = proposal.get(field, "")
            if created_str:
                try:
                    # Try parsing as Unix timestamp
                    if isinstance(created_str, (int, float)) or (isinstance(created_str, str) and created_str.isdigit()):
                        proposal_start = datetime.fromtimestamp(int(created_str))
                        # Estimate voting period (typically 7-14 days)
                        proposal_end = proposal_start + timedelta(days=10)
                        return proposal_start, proposal_end

                    # Try parsing as ISO date string
                    if isinstance(created_str, str):
                        # Handle various date formats
                        for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"]:
                            try:
                                proposal_start = datetime.strptime(created_str.replace('Z', ''), fmt.replace('Z', ''))
                                proposal_end = proposal_start + timedelta(days=10)
                                return proposal_start, proposal_end
                            except:
                                continue

                        # Try parsing with dateutil as fallback
                        try:
                            from dateutil import parser
                            proposal_start = parser.parse(created_str)
                            proposal_end = proposal_start + timedelta(days=10)
                            return proposal_start, proposal_end
                        except:
                            pass

                except Exception as e:
                    continue

        # Method 3: Use current date as fallback (for very recent proposals)
        print(f"        ⚠️ Could not parse dates, using current date as fallback")
        now = datetime.now()
        return now - timedelta(days=30), now

    def create_enhanced_proposal_csv(self, proposal: Dict, price_df: pd.DataFrame) -> str:
        """Create enhanced individual CSV file for proposal with comprehensive metadata"""
        if price_df.empty:
            return ""

        # Generate safe filename
        dao = proposal.get("DAO", proposal.get("dao", "unknown"))
        proposal_id = proposal.get("id", proposal.get("Proposal ID", "unknown"))
        title = proposal.get("title", proposal.get("Title", ""))[:40]  # Longer title

        # Clean filename more thoroughly
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_title = re.sub(r'\s+', '_', safe_title)  # Replace spaces with underscores

        filename = f"{dao}_{proposal_id}_{safe_title}.csv".replace(" ", "_")
        filepath = os.path.join(self.output_dir, filename)

        try:
            # Add comprehensive proposal metadata to price data
            price_df_copy = price_df.copy()

            # Basic metadata
            price_df_copy["dao"] = dao
            price_df_copy["proposal_id"] = proposal_id
            price_df_copy["proposal_title"] = proposal.get("title", proposal.get("Title", ""))

            # Enhanced activist detection
            activist_score, detection_methods, method_summary = self.enhanced_activist_detection(proposal)
            price_df_copy["activist_score"] = activist_score
            price_df_copy["detection_methods"] = "|".join(detection_methods)
            price_df_copy["detection_summary"] = method_summary

            # Voting power data (if available)
            price_df_copy["top_voter_percentage"] = proposal.get("top_voter_percentage", 0)
            price_df_copy["proposer_percentage"] = proposal.get("proposer_percentage", 0)
            price_df_copy["total_votes"] = proposal.get("votes", proposal.get("Votes", 0))

            # Proposal metadata
            price_df_copy["proposal_state"] = proposal.get("state", proposal.get("State", ""))
            price_df_copy["proposal_author"] = proposal.get("author", proposal.get("Author", ""))
            price_df_copy["proposal_created"] = proposal.get("created", proposal.get("Created", ""))

            # Save to CSV
            price_df_copy.to_csv(filepath, index=False)

            print(f"        💾 Saved: {filename} ({len(price_df_copy)} data points, activist_score: {activist_score:.3f})")
            return filepath

        except Exception as e:
            print(f"        ❌ Error saving {filename}: {e}")
            return ""

    def scrape_all_proposals_ultimate(self, dataset_file: str, min_activist_score: float = 0.3) -> Dict:
        """Ultimate comprehensive proposal scraping with maximum coverage"""
        print("🚀 ULTIMATE COMPREHENSIVE SCRAPING - MAXIMUM COVERAGE")
        print("=" * 80)
        print(f"📊 Target: ALL proposals from comprehensive dataset")
        print(f"🔍 Enhanced activist detection with {sum(len(p) for p in self.enhanced_activist_patterns.values())} patterns")
        print(f"🌐 Multi-source price data: CoinGecko → Yahoo → Binance")
        print(f"⚡ Anti-bot measures: User agent rotation, jitter, exponential backoff")
        print(f"🎯 Minimum activist score: {min_activist_score}")

        # Load dataset
        try:
            if dataset_file.endswith('.csv'):
                df = pd.read_csv(dataset_file)
                proposals = df.to_dict('records')
            else:
                with open(dataset_file, 'r', encoding='utf-8') as f:
                    proposals = json.load(f)

            print(f"   📥 Loaded {len(proposals)} total proposals")
        except Exception as e:
            print(f"❌ Error loading dataset: {e}")
            return {}

        # Enhanced activist filtering
        print(f"\n🔍 ENHANCED ACTIVIST FILTERING")
        activist_proposals = []

        for proposal in proposals:
            activist_score, detection_methods, method_summary = self.enhanced_activist_detection(proposal)

            if activist_score >= min_activist_score:
                proposal['activist_score'] = activist_score
                proposal['detection_methods'] = detection_methods
                proposal['detection_summary'] = method_summary
                activist_proposals.append(proposal)

        print(f"   ✅ Found {len(activist_proposals)} activist proposals (score >= {min_activist_score})")
        print(f"   📈 Activist rate: {len(activist_proposals)/len(proposals)*100:.1f}%")

        # Filter for DAOs with token mappings and not already completed
        valid_proposals = []
        dao_counts = {}

        for proposal in activist_proposals:
            dao = proposal.get("DAO", proposal.get("dao", ""))
            proposal_id = proposal.get("id", proposal.get("Proposal ID", ""))

            if dao in self.token_mappings and proposal_id not in self.completed_proposals:
                valid_proposals.append(proposal)
                dao_counts[dao] = dao_counts.get(dao, 0) + 1

        print(f"\n📊 VALID PROPOSALS FOR SCRAPING:")
        for dao, count in sorted(dao_counts.items()):
            print(f"   {dao}: {count} proposals")
        print(f"   Total valid: {len(valid_proposals)} proposals")

        # Statistics tracking
        successful_scrapes = 0
        failed_scrapes = 0
        total_data_points = 0
        dao_stats = {}
        source_stats = {"coingecko": 0, "yahoo": 0, "binance": 0}

        # Process each proposal
        for i, proposal in enumerate(valid_proposals):
            dao = proposal.get("DAO", proposal.get("dao", ""))
            proposal_id = proposal.get("id", proposal.get("Proposal ID", ""))
            title = proposal.get("title", proposal.get("Title", ""))[:60]
            activist_score = proposal.get('activist_score', 0)

            print(f"\n📈 Processing {i+1}/{len(valid_proposals)}: {dao}")
            print(f"   📋 Proposal: {title}")
            print(f"   🎯 Activist Score: {activist_score:.3f}")
            print(f"   🔍 Detection: {proposal.get('detection_summary', 'unknown')}")

            # Get token info
            token_info = self.token_mappings.get(dao)
            if not token_info:
                print(f"   ⚠️ No token mapping for {dao}")
                failed_scrapes += 1
                continue

            # Parse proposal dates
            proposal_start, proposal_end = self.parse_proposal_date_enhanced(proposal)

            if not proposal_start or not proposal_end:
                print(f"   ⚠️ Could not parse proposal dates")
                failed_scrapes += 1
                continue

            try:
                # Calculate date range (3 months before/after)
                data_start = proposal_start - timedelta(days=90)
                data_end = proposal_end + timedelta(days=90)

                print(f"   📅 Date range: {data_start.date()} to {data_end.date()}")

                # Get comprehensive price data
                price_df = self.get_comprehensive_price_data(token_info, data_start, data_end)

                if not price_df.empty:
                    # Track data source
                    source = price_df['source'].iloc[0] if 'source' in price_df.columns else 'unknown'
                    source_stats[source] = source_stats.get(source, 0) + 1

                    # Create enhanced individual CSV
                    filepath = self.create_enhanced_proposal_csv(proposal, price_df)

                    if filepath:
                        successful_scrapes += 1
                        total_data_points += len(price_df)

                        # Update DAO stats
                        if dao not in dao_stats:
                            dao_stats[dao] = {
                                "proposals": 0,
                                "data_points": 0,
                                "avg_activist_score": 0,
                                "sources_used": set()
                            }
                        dao_stats[dao]["proposals"] += 1
                        dao_stats[dao]["data_points"] += len(price_df)
                        dao_stats[dao]["avg_activist_score"] += activist_score
                        dao_stats[dao]["sources_used"].add(source)

                        # Save progress
                        self.save_progress(proposal_id, dao)

                        print(f"   ✅ Success: {len(price_df)} data points from {source}")
                    else:
                        failed_scrapes += 1
                        print(f"   ❌ Failed to save CSV file")
                else:
                    print(f"   ❌ No price data retrieved from any source")
                    failed_scrapes += 1

            except Exception as e:
                print(f"   ❌ Error processing proposal: {e}")
                failed_scrapes += 1

            # Progress update every 25 proposals
            if (i + 1) % 25 == 0:
                print(f"\n📊 PROGRESS UPDATE:")
                print(f"   Processed: {i+1}/{len(valid_proposals)}")
                print(f"   Successful: {successful_scrapes}")
                print(f"   Failed: {failed_scrapes}")
                print(f"   Success Rate: {successful_scrapes/(successful_scrapes+failed_scrapes)*100:.1f}%")
                print(f"   Total data points: {total_data_points:,}")
                print(f"   DAOs covered: {len(dao_stats)}")

        # Calculate final statistics
        for dao in dao_stats:
            if dao_stats[dao]["proposals"] > 0:
                dao_stats[dao]["avg_activist_score"] /= dao_stats[dao]["proposals"]
                dao_stats[dao]["sources_used"] = list(dao_stats[dao]["sources_used"])

        # Final summary
        summary = {
            "total_proposals_attempted": len(valid_proposals),
            "successful_scrapes": successful_scrapes,
            "failed_scrapes": failed_scrapes,
            "success_rate": round((successful_scrapes / len(valid_proposals)) * 100, 1) if valid_proposals else 0,
            "total_data_points": total_data_points,
            "dao_statistics": dao_stats,
            "source_statistics": source_stats,
            "activist_filtering": {
                "total_proposals_analyzed": len(proposals),
                "activist_proposals_found": len(activist_proposals),
                "activist_rate": round(len(activist_proposals)/len(proposals)*100, 1),
                "min_activist_score": min_activist_score
            },
            "output_directory": self.output_dir,
            "timestamp": datetime.now().isoformat()
        }

        print(f"\n🎉 ULTIMATE COMPREHENSIVE SCRAPING COMPLETE!")
        print(f"   ✅ Successful: {successful_scrapes}/{len(valid_proposals)} ({summary['success_rate']}%)")
        print(f"   📊 Total data points: {total_data_points:,}")
        print(f"   🏛️ DAOs covered: {len(dao_stats)}")
        print(f"   🎯 Activist proposals: {len(activist_proposals)} ({summary['activist_filtering']['activist_rate']}%)")
        print(f"   📁 Output directory: {self.output_dir}")

        # Print detailed DAO breakdown
        print(f"\n📊 DETAILED DAO BREAKDOWN:")
        for dao, stats in dao_stats.items():
            avg_score = stats['avg_activist_score']
            sources = ', '.join(stats['sources_used'])
            print(f"   {dao}: {stats['proposals']} proposals, {stats['data_points']:,} data points")
            print(f"      Avg activist score: {avg_score:.3f}, Sources: {sources}")

        # Print source usage statistics
        print(f"\n🌐 DATA SOURCE USAGE:")
        for source, count in source_stats.items():
            if count > 0:
                percentage = (count / successful_scrapes) * 100 if successful_scrapes > 0 else 0
                print(f"   {source.title()}: {count} proposals ({percentage:.1f}%)")

        # Save comprehensive summary
        with open(f"{self.output_dir}/ultimate_scraping_summary.json", "w") as f:
            json.dump(summary, f, indent=2, default=str)

        return summary

def main():
    """Main execution function for ultimate comprehensive scraping"""
    scraper = UltimateComprehensiveScraper()

    # Configuration
    dataset_file = "comprehensive_research_dataset.csv"
    min_activist_score = 0.25  # Lower threshold to capture more activist proposals

    try:
        print(f"🚀 Starting ultimate comprehensive scraping...")
        print(f"   Dataset: {dataset_file}")
        print(f"   Minimum activist score: {min_activist_score}")
        print(f"   Enhanced detection methods: Multi-pattern, sentiment, structural")

        # Execute comprehensive scraping
        summary = scraper.scrape_all_proposals_ultimate(dataset_file, min_activist_score)

        if summary.get("successful_scrapes", 0) > 0:
            print(f"\n✅ ULTIMATE SCRAPING SUCCESSFUL!")
            print(f"   📊 Dataset: {dataset_file}")
            print(f"   📁 Individual files created: {summary['successful_scrapes']}")
            print(f"   🎯 Activist proposals collected: {summary['activist_filtering']['activist_proposals_found']}")
            print(f"   📈 Success rate: {summary['success_rate']}%")
            print(f"   📂 Check folder: {scraper.output_dir}")

            print(f"\n🎓 RESEARCH READY:")
            print(f"   ✅ Individual proposal analysis capabilities")
            print(f"   ✅ Enhanced activist detection and scoring")
            print(f"   ✅ Comprehensive price data from multiple sources")
            print(f"   ✅ Academic-quality dataset for publication")

        else:
            print(f"❌ No proposals could be scraped successfully")
            print(f"   Check token mappings and API availability")

    except FileNotFoundError:
        print(f"❌ {dataset_file} not found")
        print("   Make sure the comprehensive research dataset exists")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
