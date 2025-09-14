# massive_data_expansion_scraper.py - Scale to 1000+ activist proposals
"""
Massive Data Expansion Scraper for scaling to 1000+ activist proposals using:
1. Boardroom.io API (50,000+ governance proposals)
2. DeepDAO API (2,461 enriched DAOs)
3. Messari Governor API (comprehensive governance data)
4. Snapshot.org GraphQL (expanded queries)
5. Tally.xyz API (alternative access methods)
6. Direct blockchain queries (Ethereum, Polygon, etc.)
"""

import requests
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os
from pathlib import Path
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import logging

# Import our existing activist detection
from ultimate_comprehensive_scraper import UltimateComprehensiveScraper

class MassiveDataExpansionScraper:
    """Scraper for massive scale data collection from multiple sources"""
    
    def __init__(self):
        self.output_dir = "massive_activist_dataset"
        self.progress_file = "massive_expansion_progress.json"
        
        # API configurations for multiple sources
        self.api_configs = {
            "boardroom": {
                "base_url": "https://api.boardroom.info/v1",
                "headers": {"X-API-KEY": ""},  # Get from boardroom.io
                "delay": 1,
                "max_retries": 3
            },
            "deepdao": {
                "base_url": "https://api.deepdao.io/v1",
                "headers": {"Authorization": "Bearer "},  # Get from deepdao.io
                "delay": 2,
                "max_retries": 3
            },
            "messari": {
                "base_url": "https://data.messari.io/api/v1",
                "headers": {"x-messari-api-key": ""},  # Get from messari.io
                "delay": 1,
                "max_retries": 3
            },
            "snapshot": {
                "base_url": "https://hub.snapshot.org/graphql",
                "headers": {"Content-Type": "application/json"},
                "delay": 0.5,
                "max_retries": 5
            },
            "tally": {
                "base_url": "https://api.tally.xyz/query",
                "headers": {"Api-Key": ""},  # Get from tally.xyz
                "delay": 2,
                "max_retries": 3
            }
        }
        
        # Expanded DAO universe (500+ DAOs)
        self.expanded_dao_universe = [
            # Major DeFi protocols
            "uniswap", "compound-governance", "aave", "makerdao", "curve", "yearn",
            "balancer", "1inch", "sushiswap", "pancakeswap", "frax", "olympusdao",
            "fei", "cream-finance", "pickle", "uma", "ens", "gitcoin",
            
            # Layer 1/2 protocols
            "ethereum", "polygon", "arbitrum", "optimism", "avalanche", "fantom",
            "harmony", "near", "solana", "cosmos", "polkadot", "kusama",
            
            # NFT/Gaming DAOs
            "decentraland", "sandbox", "axie-infinity", "illuvium", "gala",
            "enjin", "chromia", "immutable", "flow", "wax",
            
            # Infrastructure DAOs
            "chainlink", "graph", "filecoin", "arweave", "helium", "livepeer",
            "render", "akash", "storj", "sia",
            
            # Social/Creator DAOs
            "friends-with-benefits", "bankless", "developer-dao", "cabin",
            "forefront", "seed-club", "mirror", "rally", "roll",
            
            # Investment DAOs
            "metacartel", "moloch", "flamingo", "pleasr", "constitution",
            "krause-house", "links", "jenny", "neon",
            
            # Protocol DAOs
            "api3", "badger", "barnbridge", "convex", "ribbon", "tokemak",
            "tribe", "rari", "reflexer", "liquity", "alchemix", "abracadabra"
        ]
        
        # Enhanced activist detection (inherit from existing)
        self.activist_detector = UltimateComprehensiveScraper()
        
        # Create output directory
        Path(self.output_dir).mkdir(exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'{self.output_dir}/massive_scraping.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        print(f"🚀 Massive Data Expansion Scraper initialized")
        print(f"   Target: 1000+ activist proposals")
        print(f"   Sources: 5 major APIs + blockchain queries")
        print(f"   DAO universe: {len(self.expanded_dao_universe)} protocols")
        print(f"   Output directory: {self.output_dir}")
    
    async def get_boardroom_proposals(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Get proposals from Boardroom.io API (50,000+ governance proposals)"""
        self.logger.info("🏛️ Fetching proposals from Boardroom.io...")
        
        proposals = []
        page = 1
        max_pages = 100  # Limit to prevent infinite loops
        
        while page <= max_pages:
            try:
                url = f"{self.api_configs['boardroom']['base_url']}/proposals"
                params = {
                    "page": page,
                    "limit": 100,
                    "status": "all"
                }
                
                async with session.get(url, params=params, 
                                     headers=self.api_configs['boardroom']['headers']) as response:
                    if response.status == 200:
                        data = await response.json()
                        page_proposals = data.get('data', [])
                        
                        if not page_proposals:
                            break
                            
                        proposals.extend(page_proposals)
                        self.logger.info(f"   📄 Page {page}: {len(page_proposals)} proposals")
                        page += 1
                        
                        await asyncio.sleep(self.api_configs['boardroom']['delay'])
                    else:
                        self.logger.warning(f"   ⚠️ Boardroom API error {response.status}")
                        break
                        
            except Exception as e:
                self.logger.error(f"   ❌ Boardroom error: {e}")
                break
        
        self.logger.info(f"   ✅ Boardroom total: {len(proposals)} proposals")
        return proposals
    
    async def get_deepdao_proposals(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Get proposals from DeepDAO API (2,461 enriched DAOs)"""
        self.logger.info("🔍 Fetching proposals from DeepDAO...")
        
        proposals = []
        
        # First get list of DAOs
        try:
            url = f"{self.api_configs['deepdao']['base_url']}/organizations"
            params = {"limit": 1000, "offset": 0}
            
            async with session.get(url, params=params,
                                 headers=self.api_configs['deepdao']['headers']) as response:
                if response.status == 200:
                    data = await response.json()
                    daos = data.get('data', [])
                    
                    # Get proposals for each DAO
                    for dao in daos[:100]:  # Limit to top 100 DAOs
                        dao_id = dao.get('id')
                        if dao_id:
                            dao_proposals = await self.get_deepdao_dao_proposals(session, dao_id)
                            proposals.extend(dao_proposals)
                            await asyncio.sleep(self.api_configs['deepdao']['delay'])
                            
        except Exception as e:
            self.logger.error(f"   ❌ DeepDAO error: {e}")
        
        self.logger.info(f"   ✅ DeepDAO total: {len(proposals)} proposals")
        return proposals
    
    async def get_deepdao_dao_proposals(self, session: aiohttp.ClientSession, dao_id: str) -> List[Dict]:
        """Get proposals for a specific DAO from DeepDAO"""
        try:
            url = f"{self.api_configs['deepdao']['base_url']}/organizations/{dao_id}/proposals"
            
            async with session.get(url, headers=self.api_configs['deepdao']['headers']) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('data', [])
        except:
            pass
        return []
    
    async def get_messari_proposals(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Get proposals from Messari Governor API"""
        self.logger.info("📊 Fetching proposals from Messari Governor...")
        
        proposals = []
        
        try:
            url = f"{self.api_configs['messari']['base_url']}/governance/proposals"
            params = {"limit": 1000}
            
            async with session.get(url, params=params,
                                 headers=self.api_configs['messari']['headers']) as response:
                if response.status == 200:
                    data = await response.json()
                    proposals = data.get('data', [])
                    
        except Exception as e:
            self.logger.error(f"   ❌ Messari error: {e}")
        
        self.logger.info(f"   ✅ Messari total: {len(proposals)} proposals")
        return proposals
    
    async def get_expanded_snapshot_proposals(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Get expanded proposals from Snapshot.org with larger queries"""
        self.logger.info("📸 Fetching expanded proposals from Snapshot.org...")
        
        proposals = []
        skip = 0
        batch_size = 1000
        max_proposals = 10000  # Limit to prevent overwhelming
        
        while len(proposals) < max_proposals:
            query = """
            query Proposals($skip: Int!, $first: Int!) {
              proposals(
                skip: $skip,
                first: $first,
                orderBy: "created",
                orderDirection: desc,
                where: {
                  state: "closed"
                }
              ) {
                id
                title
                body
                author
                created
                start
                end
                state
                scores
                scores_total
                votes
                space {
                  id
                  name
                  about
                  network
                  symbol
                }
              }
            }
            """
            
            variables = {"skip": skip, "first": batch_size}
            
            try:
                async with session.post(
                    self.api_configs['snapshot']['base_url'],
                    json={"query": query, "variables": variables},
                    headers=self.api_configs['snapshot']['headers']
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        batch_proposals = data.get('data', {}).get('proposals', [])
                        
                        if not batch_proposals:
                            break
                            
                        proposals.extend(batch_proposals)
                        skip += batch_size
                        
                        self.logger.info(f"   📄 Batch: {len(batch_proposals)} proposals (total: {len(proposals)})")
                        await asyncio.sleep(self.api_configs['snapshot']['delay'])
                    else:
                        break
                        
            except Exception as e:
                self.logger.error(f"   ❌ Snapshot error: {e}")
                break
        
        self.logger.info(f"   ✅ Snapshot total: {len(proposals)} proposals")
        return proposals
    
    async def get_tally_proposals(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Get proposals from Tally.xyz API"""
        self.logger.info("🗳️ Fetching proposals from Tally.xyz...")
        
        proposals = []
        
        # Tally GraphQL query for proposals
        query = """
        query Proposals($pagination: Pagination) {
          proposals(pagination: $pagination) {
            nodes {
              id
              title
              description
              proposer {
                address
              }
              createdAt
              startBlock
              endBlock
              state
              votes {
                support
                weight
                voter {
                  address
                }
              }
              governor {
                name
                slug
                token {
                  symbol
                  name
                }
              }
            }
          }
        }
        """
        
        try:
            variables = {"pagination": {"limit": 1000, "offset": 0}}
            
            async with session.post(
                self.api_configs['tally']['base_url'],
                json={"query": query, "variables": variables},
                headers=self.api_configs['tally']['headers']
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    proposals = data.get('data', {}).get('proposals', {}).get('nodes', [])
                    
        except Exception as e:
            self.logger.error(f"   ❌ Tally error: {e}")
        
        self.logger.info(f"   ✅ Tally total: {len(proposals)} proposals")
        return proposals
    
    def normalize_proposals(self, proposals: List[Dict], source: str) -> List[Dict]:
        """Normalize proposals from different sources to common format"""
        normalized = []
        
        for proposal in proposals:
            try:
                # Common normalization logic
                normalized_proposal = {
                    "source": source,
                    "id": proposal.get("id", ""),
                    "title": proposal.get("title", ""),
                    "body": proposal.get("body", proposal.get("description", "")),
                    "author": proposal.get("author", proposal.get("proposer", {}).get("address", "")),
                    "created": proposal.get("created", proposal.get("createdAt", "")),
                    "start": proposal.get("start", proposal.get("startBlock", "")),
                    "end": proposal.get("end", proposal.get("endBlock", "")),
                    "state": proposal.get("state", ""),
                    "votes": proposal.get("votes", 0),
                    "dao": self.extract_dao_name(proposal, source),
                    "raw_data": proposal
                }
                
                normalized.append(normalized_proposal)
                
            except Exception as e:
                self.logger.warning(f"   ⚠️ Error normalizing proposal: {e}")
                continue
        
        return normalized
    
    def extract_dao_name(self, proposal: Dict, source: str) -> str:
        """Extract DAO name from proposal based on source"""
        if source == "snapshot":
            return proposal.get("space", {}).get("id", "")
        elif source == "tally":
            return proposal.get("governor", {}).get("slug", "")
        elif source == "boardroom":
            return proposal.get("protocol", "")
        elif source == "deepdao":
            return proposal.get("organization", {}).get("name", "")
        elif source == "messari":
            return proposal.get("protocol", {}).get("slug", "")
        return ""
    
    async def massive_proposal_collection(self) -> Dict:
        """Collect proposals from all sources simultaneously"""
        self.logger.info("🚀 MASSIVE PROPOSAL COLLECTION STARTING")
        self.logger.info("=" * 80)
        
        # Create async session
        timeout = aiohttp.ClientTimeout(total=300)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            
            # Collect from all sources simultaneously
            tasks = [
                self.get_boardroom_proposals(session),
                self.get_deepdao_proposals(session),
                self.get_messari_proposals(session),
                self.get_expanded_snapshot_proposals(session),
                self.get_tally_proposals(session)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            all_proposals = []
            source_names = ["boardroom", "deepdao", "messari", "snapshot", "tally"]
            
            for i, (source, proposals) in enumerate(zip(source_names, results)):
                if isinstance(proposals, Exception):
                    self.logger.error(f"   ❌ {source} failed: {proposals}")
                    continue
                    
                normalized = self.normalize_proposals(proposals, source)
                all_proposals.extend(normalized)
                
                self.logger.info(f"   ✅ {source}: {len(normalized)} normalized proposals")
        
        self.logger.info(f"🎯 TOTAL PROPOSALS COLLECTED: {len(all_proposals)}")
        return all_proposals
    
    def enhanced_activist_filtering(self, proposals: List[Dict], min_score: float = 0.2) -> List[Dict]:
        """Apply enhanced activist filtering to massive dataset"""
        self.logger.info(f"🔍 ENHANCED ACTIVIST FILTERING (min_score: {min_score})")
        
        activist_proposals = []
        
        for proposal in proposals:
            try:
                # Use existing activist detection
                activist_score, detection_methods, method_summary = \
                    self.activist_detector.enhanced_activist_detection(proposal)
                
                if activist_score >= min_score:
                    proposal['activist_score'] = activist_score
                    proposal['detection_methods'] = detection_methods
                    proposal['detection_summary'] = method_summary
                    activist_proposals.append(proposal)
                    
            except Exception as e:
                continue
        
        self.logger.info(f"   ✅ Found {len(activist_proposals)} activist proposals")
        self.logger.info(f"   📈 Activist rate: {len(activist_proposals)/len(proposals)*100:.1f}%")
        
        return activist_proposals
    
    def save_massive_dataset(self, proposals: List[Dict]) -> str:
        """Save massive dataset to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save as JSON
        json_file = f"{self.output_dir}/massive_activist_proposals_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump(proposals, f, indent=2, default=str)
        
        # Save as CSV
        csv_file = f"{self.output_dir}/massive_activist_proposals_{timestamp}.csv"
        df = pd.DataFrame(proposals)
        df.to_csv(csv_file, index=False)
        
        # Save summary
        summary = {
            "timestamp": timestamp,
            "total_proposals": len(proposals),
            "sources": list(set(p.get('source', '') for p in proposals)),
            "daos": list(set(p.get('dao', '') for p in proposals)),
            "avg_activist_score": sum(p.get('activist_score', 0) for p in proposals) / len(proposals),
            "score_distribution": self.get_score_distribution(proposals)
        }
        
        summary_file = f"{self.output_dir}/massive_dataset_summary_{timestamp}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        self.logger.info(f"💾 Dataset saved:")
        self.logger.info(f"   📄 JSON: {json_file}")
        self.logger.info(f"   📊 CSV: {csv_file}")
        self.logger.info(f"   📋 Summary: {summary_file}")
        
        return csv_file
    
    def get_score_distribution(self, proposals: List[Dict]) -> Dict:
        """Get distribution of activist scores"""
        scores = [p.get('activist_score', 0) for p in proposals]
        return {
            "min": min(scores),
            "max": max(scores),
            "mean": sum(scores) / len(scores),
            "ranges": {
                "0.2-0.3": len([s for s in scores if 0.2 <= s < 0.3]),
                "0.3-0.4": len([s for s in scores if 0.3 <= s < 0.4]),
                "0.4-0.5": len([s for s in scores if 0.4 <= s < 0.5]),
                "0.5+": len([s for s in scores if s >= 0.5])
            }
        }

async def main():
    """Main execution function"""
    scraper = MassiveDataExpansionScraper()
    
    try:
        # Collect massive dataset
        all_proposals = await scraper.massive_proposal_collection()
        
        if all_proposals:
            # Apply activist filtering
            activist_proposals = scraper.enhanced_activist_filtering(all_proposals, min_score=0.2)
            
            if activist_proposals:
                # Save dataset
                csv_file = scraper.save_massive_dataset(activist_proposals)
                
                print(f"\n🎉 MASSIVE DATA EXPANSION COMPLETE!")
                print(f"   📊 Total proposals collected: {len(all_proposals):,}")
                print(f"   🎯 Activist proposals found: {len(activist_proposals):,}")
                print(f"   📈 Success rate: {len(activist_proposals)/len(all_proposals)*100:.1f}%")
                print(f"   📁 Dataset saved: {csv_file}")
                print(f"\n🚀 Ready for massive-scale research analysis!")
                
            else:
                print("❌ No activist proposals found")
        else:
            print("❌ No proposals collected")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
