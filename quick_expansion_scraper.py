# quick_expansion_scraper.py - Immediate 10x expansion using free sources
"""
Quick Expansion Scraper to immediately scale from 27 to 200-300 activist proposals using:
1. Expanded Snapshot.org queries (free)
2. Boardroom.io free tier
3. More comprehensive DAO coverage
4. Enhanced activist detection with lower threshold
"""

import requests
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os
from pathlib import Path

# Import existing activist detection
from ultimate_comprehensive_scraper import UltimateComprehensiveScraper

class QuickExpansionScraper:
    """Quick scraper for immediate 10x expansion using free sources"""
    
    def __init__(self):
        self.output_dir = "quick_expansion_data"
        self.progress_file = "quick_expansion_progress.json"
        
        # Expanded DAO list (100+ major DAOs)
        self.expanded_daos = [
            # Major DeFi (existing)
            "uniswap", "compound-governance", "aave", "makerdao", "curve", "yearn",
            "balancer", "1inch", "sushiswap", "frax", "olympusdao", "fei", 
            "cream-finance", "pickle", "uma", "ens", "gitcoin",
            
            # Additional major protocols
            "pancakeswap", "trader-joe", "platypus-finance", "benqi", "avalanche",
            "polygon", "arbitrum", "optimism", "fantom", "harmony", "near",
            "solana", "cosmos", "polkadot", "kusama", "chainlink", "graph",
            "filecoin", "arweave", "helium", "livepeer", "render", "akash",
            
            # NFT/Gaming DAOs
            "decentraland", "sandbox", "axie-infinity", "illuvium", "gala",
            "enjin", "immutable", "flow", "wax", "treasure", "merit-circle",
            
            # Social/Creator DAOs
            "friends-with-benefits", "bankless", "developer-dao", "cabin",
            "forefront", "seed-club", "mirror", "rally", "roll", "coordinape",
            
            # Investment DAOs
            "metacartel", "moloch", "flamingo", "pleasr", "constitution",
            "krause-house", "links", "jenny", "neon", "whale",
            
            # Protocol DAOs
            "api3", "badger", "barnbridge", "convex", "ribbon", "tokemak",
            "tribe", "rari", "reflexer", "liquity", "alchemix", "abracadabra",
            "spell", "wonderland", "olympus", "klima", "redacted", "dopex"
        ]
        
        # Enhanced activist detection
        self.activist_detector = UltimateComprehensiveScraper()
        
        # Create output directory
        Path(self.output_dir).mkdir(exist_ok=True)
        
        print(f"⚡ Quick Expansion Scraper initialized")
        print(f"   Target: 200-300 activist proposals (10x expansion)")
        print(f"   Sources: Snapshot.org (expanded) + Boardroom.io (free)")
        print(f"   DAO coverage: {len(self.expanded_daos)} protocols")
        print(f"   Output directory: {self.output_dir}")
    
    def get_expanded_snapshot_proposals(self, max_proposals: int = 10000) -> List[Dict]:
        """Get massively expanded proposals from Snapshot.org"""
        print(f"📸 Fetching expanded proposals from Snapshot.org...")
        
        all_proposals = []
        skip = 0
        batch_size = 1000
        
        while len(all_proposals) < max_proposals:
            query = """
            query Proposals($skip: Int!, $first: Int!) {
              proposals(
                skip: $skip,
                first: $first,
                orderBy: "created",
                orderDirection: desc,
                where: {
                  state_in: ["closed", "active"]
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
                  members
                }
                strategies {
                  name
                  params
                }
              }
            }
            """
            
            variables = {"skip": skip, "first": batch_size}
            
            try:
                response = requests.post(
                    "https://hub.snapshot.org/graphql",
                    json={"query": query, "variables": variables},
                    headers={"Content-Type": "application/json"},
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    batch_proposals = data.get('data', {}).get('proposals', [])
                    
                    if not batch_proposals:
                        break
                    
                    all_proposals.extend(batch_proposals)
                    skip += batch_size
                    
                    print(f"   📄 Batch {skip//batch_size}: {len(batch_proposals)} proposals (total: {len(all_proposals)})")
                    time.sleep(0.5)  # Rate limiting
                else:
                    print(f"   ❌ Snapshot API error: {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"   ❌ Snapshot error: {e}")
                break
        
        print(f"   ✅ Snapshot total: {len(all_proposals)} proposals")
        return all_proposals
    
    def get_boardroom_free_proposals(self) -> List[Dict]:
        """Get proposals from Boardroom.io free tier"""
        print(f"🏛️ Fetching proposals from Boardroom.io (free tier)...")
        
        proposals = []
        
        # Boardroom free tier endpoints
        try:
            # Get protocols first
            protocols_url = "https://api.boardroom.info/v1/protocols"
            response = requests.get(protocols_url, timeout=30)
            
            if response.status_code == 200:
                protocols = response.json().get('data', [])
                print(f"   📋 Found {len(protocols)} protocols")
                
                # Get proposals for each protocol (limited by free tier)
                for protocol in protocols[:20]:  # Limit to avoid rate limits
                    protocol_id = protocol.get('cname', '')
                    if protocol_id:
                        protocol_proposals = self.get_boardroom_protocol_proposals(protocol_id)
                        proposals.extend(protocol_proposals)
                        time.sleep(2)  # Rate limiting for free tier
                        
        except Exception as e:
            print(f"   ❌ Boardroom error: {e}")
        
        print(f"   ✅ Boardroom total: {len(proposals)} proposals")
        return proposals
    
    def get_boardroom_protocol_proposals(self, protocol_id: str) -> List[Dict]:
        """Get proposals for a specific protocol from Boardroom"""
        try:
            url = f"https://api.boardroom.info/v1/protocols/{protocol_id}/proposals"
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                proposals = data.get('data', [])
                print(f"     📄 {protocol_id}: {len(proposals)} proposals")
                return proposals
        except:
            pass
        return []
    
    def normalize_snapshot_proposals(self, proposals: List[Dict]) -> List[Dict]:
        """Normalize Snapshot proposals to common format"""
        normalized = []
        
        for proposal in proposals:
            try:
                normalized_proposal = {
                    "source": "snapshot",
                    "id": proposal.get("id", ""),
                    "title": proposal.get("title", ""),
                    "body": proposal.get("body", ""),
                    "author": proposal.get("author", ""),
                    "created": proposal.get("created", ""),
                    "start": proposal.get("start", ""),
                    "end": proposal.get("end", ""),
                    "state": proposal.get("state", ""),
                    "votes": proposal.get("votes", 0),
                    "scores_total": proposal.get("scores_total", 0),
                    "dao": proposal.get("space", {}).get("id", ""),
                    "dao_name": proposal.get("space", {}).get("name", ""),
                    "dao_symbol": proposal.get("space", {}).get("symbol", ""),
                    "dao_members": proposal.get("space", {}).get("members", []),
                    "raw_data": proposal
                }
                normalized.append(normalized_proposal)
            except:
                continue
        
        return normalized
    
    def normalize_boardroom_proposals(self, proposals: List[Dict]) -> List[Dict]:
        """Normalize Boardroom proposals to common format"""
        normalized = []
        
        for proposal in proposals:
            try:
                normalized_proposal = {
                    "source": "boardroom",
                    "id": proposal.get("refId", ""),
                    "title": proposal.get("title", ""),
                    "body": proposal.get("content", ""),
                    "author": proposal.get("proposer", ""),
                    "created": proposal.get("startTime", ""),
                    "start": proposal.get("startTime", ""),
                    "end": proposal.get("endTime", ""),
                    "state": proposal.get("currentState", ""),
                    "votes": proposal.get("totalVotes", 0),
                    "dao": proposal.get("protocol", ""),
                    "raw_data": proposal
                }
                normalized.append(normalized_proposal)
            except:
                continue
        
        return normalized
    
    def enhanced_activist_filtering_relaxed(self, proposals: List[Dict], min_score: float = 0.15) -> List[Dict]:
        """Apply enhanced activist filtering with relaxed threshold for more proposals"""
        print(f"🔍 Enhanced activist filtering (relaxed threshold: {min_score})...")
        
        activist_proposals = []
        
        for proposal in proposals:
            try:
                # Use existing activist detection with lower threshold
                activist_score, detection_methods, method_summary = \
                    self.activist_detector.enhanced_activist_detection(proposal)
                
                if activist_score >= min_score:
                    proposal['activist_score'] = activist_score
                    proposal['detection_methods'] = detection_methods
                    proposal['detection_summary'] = method_summary
                    activist_proposals.append(proposal)
                    
            except Exception as e:
                continue
        
        print(f"   ✅ Found {len(activist_proposals)} activist proposals")
        print(f"   📈 Activist rate: {len(activist_proposals)/len(proposals)*100:.1f}%")
        
        return activist_proposals
    
    def filter_by_dao_relevance(self, proposals: List[Dict]) -> List[Dict]:
        """Filter proposals by DAO relevance to our expanded list"""
        relevant_proposals = []
        
        for proposal in proposals:
            dao = proposal.get('dao', '').lower()
            if any(target_dao in dao for target_dao in self.expanded_daos):
                relevant_proposals.append(proposal)
        
        print(f"   🎯 Filtered to {len(relevant_proposals)} relevant DAO proposals")
        return relevant_proposals
    
    def quick_expansion_collection(self) -> List[Dict]:
        """Collect expanded dataset from free sources"""
        print("⚡ QUICK EXPANSION COLLECTION STARTING")
        print("=" * 60)
        
        all_proposals = []
        
        # 1. Get expanded Snapshot proposals
        snapshot_proposals = self.get_expanded_snapshot_proposals(max_proposals=15000)
        normalized_snapshot = self.normalize_snapshot_proposals(snapshot_proposals)
        all_proposals.extend(normalized_snapshot)
        
        # 2. Get Boardroom free tier proposals
        boardroom_proposals = self.get_boardroom_free_proposals()
        normalized_boardroom = self.normalize_boardroom_proposals(boardroom_proposals)
        all_proposals.extend(normalized_boardroom)
        
        print(f"\n📊 COLLECTION SUMMARY:")
        print(f"   Snapshot: {len(normalized_snapshot)} proposals")
        print(f"   Boardroom: {len(normalized_boardroom)} proposals")
        print(f"   Total: {len(all_proposals)} proposals")
        
        # 3. Filter by DAO relevance
        relevant_proposals = self.filter_by_dao_relevance(all_proposals)
        
        # 4. Apply enhanced activist filtering
        activist_proposals = self.enhanced_activist_filtering_relaxed(relevant_proposals, min_score=0.15)
        
        return activist_proposals
    
    def save_quick_expansion_dataset(self, proposals: List[Dict]) -> str:
        """Save quick expansion dataset"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save as JSON
        json_file = f"{self.output_dir}/quick_expansion_activist_proposals_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump(proposals, f, indent=2, default=str)
        
        # Save as CSV
        csv_file = f"{self.output_dir}/quick_expansion_activist_proposals_{timestamp}.csv"
        df = pd.DataFrame(proposals)
        df.to_csv(csv_file, index=False)
        
        # Save summary
        summary = {
            "timestamp": timestamp,
            "total_proposals": len(proposals),
            "sources": list(set(p.get('source', '') for p in proposals)),
            "unique_daos": len(set(p.get('dao', '') for p in proposals)),
            "avg_activist_score": sum(p.get('activist_score', 0) for p in proposals) / len(proposals) if proposals else 0,
            "score_ranges": {
                "0.15-0.25": len([p for p in proposals if 0.15 <= p.get('activist_score', 0) < 0.25]),
                "0.25-0.35": len([p for p in proposals if 0.25 <= p.get('activist_score', 0) < 0.35]),
                "0.35-0.45": len([p for p in proposals if 0.35 <= p.get('activist_score', 0) < 0.45]),
                "0.45+": len([p for p in proposals if p.get('activist_score', 0) >= 0.45])
            }
        }
        
        summary_file = f"{self.output_dir}/quick_expansion_summary_{timestamp}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print(f"\n💾 Quick expansion dataset saved:")
        print(f"   📄 JSON: {json_file}")
        print(f"   📊 CSV: {csv_file}")
        print(f"   📋 Summary: {summary_file}")
        
        return csv_file

def main():
    """Main execution function for quick expansion"""
    scraper = QuickExpansionScraper()
    
    try:
        # Collect expanded dataset
        activist_proposals = scraper.quick_expansion_collection()
        
        if activist_proposals:
            # Save dataset
            csv_file = scraper.save_quick_expansion_dataset(activist_proposals)
            
            print(f"\n🎉 QUICK EXPANSION COMPLETE!")
            print(f"   🎯 Activist proposals found: {len(activist_proposals)}")
            print(f"   📈 Expansion factor: {len(activist_proposals)/27:.1f}x (from 27 to {len(activist_proposals)})")
            print(f"   📁 Dataset saved: {csv_file}")
            print(f"\n⚡ Ready for immediate research analysis!")
            print(f"   Next step: Run price data collection on expanded dataset")
            
        else:
            print("❌ No activist proposals found")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
