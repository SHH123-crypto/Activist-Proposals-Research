# alternative_sources.py - Alternative DAO data sources (NO API KEY NEEDED)
"""
Alternative sources for DAO proposal data that are faster than Tally.xyz scraping:
1. Snapshot.org - Decentralized governance platform
2. Commonwealth.im - Governance discussion platform  
3. Direct blockchain queries via Alchemy/Infura
4. Boardroom.info - Governance aggregator
"""

import asyncio
import aiohttp
import json
from typing import List, Dict

class SnapshotClient:
    """Snapshot.org GraphQL API client - NO API KEY NEEDED"""
    
    def __init__(self):
        self.base_url = "https://hub.snapshot.org/graphql"
        self.headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    
    async def get_dao_proposals(self, space_id: str, limit: int = 50) -> List[Dict]:
        """Get proposals from Snapshot.org"""
        
        query = """
        query Proposals($space: String!, $first: Int!) {
            proposals(
                first: $first,
                where: { space: $space },
                orderBy: "created",
                orderDirection: desc
            ) {
                id
                title
                body
                start
                end
                state
                author
                choices
                scores
                scores_total
                votes
                space {
                    id
                    name
                }
            }
        }
        """
        
        variables = {
            "space": space_id,
            "first": limit
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    self.base_url,
                    headers=self.headers,
                    json={"query": query, "variables": variables},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    
                    if response.status == 200:
                        data = await response.json()
                        if "data" in data and "proposals" in data["data"]:
                            proposals = data["data"]["proposals"]
                            print(f"✓ Snapshot: Found {len(proposals)} proposals for {space_id}")
                            return self._format_proposals(proposals, space_id)
                        else:
                            print(f"⚠ Snapshot: No proposals for {space_id}")
                            return []
                    else:
                        print(f"✗ Snapshot Error {response.status} for {space_id}")
                        return []
                        
            except Exception as e:
                print(f"✗ Snapshot Exception for {space_id}: {str(e)}")
                return []
    
    def _format_proposals(self, proposals: List[Dict], space_id: str) -> List[Dict]:
        """Format Snapshot proposals to match expected structure"""
        formatted = []
        
        for proposal in proposals:
            formatted_proposal = {
                "id": proposal.get("id", ""),
                "title": proposal.get("title", ""),
                "description": proposal.get("body", "")[:500],  # Truncate
                "link": f"https://snapshot.org/#/{space_id}/proposal/{proposal.get('id', '')}",
                "state": proposal.get("state", "unknown").lower(),
                "createdAt": str(proposal.get("start", "")),
                "proposer": proposal.get("author", ""),
                "dao": space_id,
                "source": "snapshot.org"
            }
            formatted.append(formatted_proposal)
            
        return formatted

async def get_alternative_dao_data():
    """Get DAO data from alternative sources - FAST and NO API KEY"""
    
    print("🔄 Using alternative DAO data sources...")
    
    snapshot = SnapshotClient()
    all_proposals = []
    
    # Popular DAO spaces on Snapshot
    dao_spaces = [
        "arbitrumfoundation.eth",
        "opcollective.eth", 
        "uniswapgovernance.eth",
        "aave.eth",
        "compound-governance.eth",
        "ens.eth",
        "gitcoindao.eth",
        "balancer.eth"
    ]
    
    print(f"📡 Fetching from {len(dao_spaces)} DAO spaces...")
    
    # Fetch proposals from each space
    tasks = []
    for space in dao_spaces:
        task = snapshot.get_dao_proposals(space, limit=20)  # 20 per DAO
        tasks.append(task)
    
    # Run all requests concurrently for speed
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for result in results:
        if isinstance(result, list):
            all_proposals.extend(result)
        else:
            print(f"⚠ Error in one of the requests: {result}")
    
    print(f"📊 Total proposals from alternative sources: {len(all_proposals)}")
    
    # Save results
    with open("alternative_dao_proposals.json", "w", encoding="utf-8") as f:
        json.dump(all_proposals, f, indent=2, ensure_ascii=False)
    
    print("💾 Saved to alternative_dao_proposals.json")
    return all_proposals

async def main():
    """Main function for alternative sources"""
    proposals = await get_alternative_dao_data()
    
    # Quick analysis
    if proposals:
        sources = set(p.get("source", "unknown") for p in proposals)
        daos = set(p.get("dao", "unknown") for p in proposals)
        
        print(f"\n📈 Analysis:")
        print(f"   Sources: {', '.join(sources)}")
        print(f"   DAOs: {len(daos)} unique DAOs")
        print(f"   Avg proposals per DAO: {len(proposals) / len(daos):.1f}")
    
    return proposals

if __name__ == "__main__":
    asyncio.run(main())
