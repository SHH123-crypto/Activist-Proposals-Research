# additional_sources.py - Even more DAO data sources
"""
Additional DAO proposal sources for maximum data coverage:
1. Mirror.xyz - Decentralized publishing platform with DAO content
2. Discourse forums - Many DAOs use Discourse for governance
3. GitHub - Some DAOs track proposals in GitHub issues/discussions
4. Aragon - DAO governance platform
5. DAOstack - Governance framework
6. Colony - DAO management platform
7. Moloch DAO variants
8. Direct contract queries via Alchemy/Infura
"""

import asyncio
import aiohttp
import json
import re
from typing import List, Dict
from datetime import datetime

class MirrorClient:
    """Mirror.xyz API client for DAO-related content"""
    
    def __init__(self):
        self.base_url = "https://mirror.xyz/api"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json"
        }
    
    async def get_dao_content(self, limit: int = 50) -> List[Dict]:
        """Get DAO-related content from Mirror.xyz"""
        # Search for DAO-related content
        search_terms = ["DAO", "governance", "proposal", "vote", "treasury"]
        all_content = []
        
        for term in search_terms[:2]:  # Limit to avoid rate limits
            url = f"{self.base_url}/search"
            params = {"q": term, "limit": limit // len(search_terms)}
            
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url, headers=self.headers, params=params, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "entries" in data:
                                entries = data["entries"]
                                dao_entries = [e for e in entries if self._is_dao_related(e)]
                                all_content.extend(dao_entries)
                                print(f"✓ Mirror: Found {len(dao_entries)} DAO entries for '{term}'")
                        else:
                            print(f"✗ Mirror Error {response.status} for '{term}'")
                except Exception as e:
                    print(f"✗ Mirror Exception for '{term}': {str(e)}")
        
        return self._format_content(all_content)
    
    def _is_dao_related(self, entry: Dict) -> bool:
        """Check if Mirror entry is DAO-related"""
        title = entry.get("title", "").lower()
        body = entry.get("body", "").lower()
        text = title + " " + body
        
        dao_keywords = [
            "dao", "governance", "proposal", "vote", "treasury", "protocol",
            "community", "delegate", "token", "defi", "decentralized"
        ]
        return any(keyword in text for keyword in dao_keywords)
    
    def _format_content(self, entries: List[Dict]) -> List[Dict]:
        """Format Mirror entries to proposal format"""
        formatted = []
        for entry in entries:
            formatted_proposal = {
                "id": f"mirror_{entry.get('digest', '')}",
                "title": entry.get("title", ""),
                "description": entry.get("body", "")[:500],
                "link": f"https://mirror.xyz/{entry.get('author', {}).get('address', '')}/{entry.get('digest', '')}",
                "state": "published",
                "createdAt": entry.get("timestamp", ""),
                "proposer": entry.get("author", {}).get("address", ""),
                "dao": "mirror_community",
                "source": "mirror.xyz"
            }
            formatted.append(formatted_proposal)
        return formatted

class DiscourseClient:
    """Generic Discourse forum client for DAO governance forums"""
    
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json"
        }
    
    async def get_forum_proposals(self, forum_url: str, dao_name: str, limit: int = 20) -> List[Dict]:
        """Get proposals from Discourse-based governance forums"""
        url = f"{forum_url}/latest.json"
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=self.headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if "topic_list" in data and "topics" in data["topic_list"]:
                            topics = data["topic_list"]["topics"][:limit]
                            governance_topics = [t for t in topics if self._is_governance_topic(t)]
                            print(f"✓ {dao_name} Forum: Found {len(governance_topics)} governance topics")
                            return self._format_proposals(governance_topics, forum_url, dao_name)
                    else:
                        print(f"✗ {dao_name} Forum Error {response.status}")
                        return []
            except Exception as e:
                print(f"✗ {dao_name} Forum Exception: {str(e)}")
                return []
    
    def _is_governance_topic(self, topic: Dict) -> bool:
        """Check if topic is governance-related"""
        title = topic.get("title", "").lower()
        governance_keywords = [
            "proposal", "vote", "governance", "treasury", "funding", "grant",
            "protocol", "upgrade", "dao", "community", "delegate", "rfc"
        ]
        return any(keyword in title for keyword in governance_keywords)
    
    def _format_proposals(self, topics: List[Dict], forum_url: str, dao_name: str) -> List[Dict]:
        """Format Discourse topics to proposal format"""
        formatted = []
        for topic in topics:
            formatted_proposal = {
                "id": f"{dao_name}_forum_{topic.get('id', '')}",
                "title": topic.get("title", ""),
                "description": topic.get("excerpt", "")[:500],
                "link": f"{forum_url}/t/{topic.get('slug', '')}/{topic.get('id', '')}",
                "state": "active" if not topic.get("closed") else "closed",
                "createdAt": topic.get("created_at", ""),
                "proposer": f"user_{topic.get('posters', [{}])[0].get('user_id', '')}",
                "dao": dao_name,
                "source": f"{dao_name}_forum"
            }
            formatted.append(formatted_proposal)
        return formatted

class AragonClient:
    """Aragon DAO governance client"""
    
    def __init__(self):
        self.base_url = "https://api.aragon.org"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json"
        }
    
    async def get_aragon_proposals(self, limit: int = 30) -> List[Dict]:
        """Get proposals from Aragon DAOs"""
        # Aragon API endpoints for governance data
        url = f"{self.base_url}/v1/daos"
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=self.headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if "daos" in data:
                            daos = data["daos"][:10]  # Limit to first 10 DAOs
                            all_proposals = []
                            
                            for dao in daos:
                                dao_proposals = await self._get_dao_proposals(session, dao, limit // 10)
                                all_proposals.extend(dao_proposals)
                            
                            print(f"✓ Aragon: Found {len(all_proposals)} proposals from {len(daos)} DAOs")
                            return all_proposals
                    else:
                        print(f"✗ Aragon Error {response.status}")
                        return []
            except Exception as e:
                print(f"✗ Aragon Exception: {str(e)}")
                return []
    
    async def _get_dao_proposals(self, session: aiohttp.ClientSession, dao: Dict, limit: int) -> List[Dict]:
        """Get proposals for a specific Aragon DAO"""
        dao_id = dao.get("id", "")
        url = f"{self.base_url}/v1/daos/{dao_id}/proposals"
        
        try:
            async with session.get(url, headers=self.headers, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    if "proposals" in data:
                        proposals = data["proposals"][:limit]
                        return self._format_proposals(proposals, dao.get("name", dao_id))
        except Exception:
            pass
        return []
    
    def _format_proposals(self, proposals: List[Dict], dao_name: str) -> List[Dict]:
        """Format Aragon proposals"""
        formatted = []
        for proposal in proposals:
            formatted_proposal = {
                "id": f"aragon_{proposal.get('id', '')}",
                "title": proposal.get("metadata", {}).get("title", ""),
                "description": proposal.get("metadata", {}).get("summary", "")[:500],
                "link": f"https://app.aragon.org/#/{dao_name}/proposal/{proposal.get('id', '')}",
                "state": proposal.get("status", "unknown").lower(),
                "createdAt": proposal.get("creationDate", ""),
                "proposer": proposal.get("creator", ""),
                "dao": dao_name,
                "source": "aragon.org"
            }
            formatted.append(formatted_proposal)
        return formatted

async def get_additional_dao_sources() -> List[Dict]:
    """Get DAO proposals from additional sources"""
    print("🔍 Fetching from ADDITIONAL DAO sources...")
    
    # Initialize clients
    mirror = MirrorClient()
    discourse = DiscourseClient()
    aragon = AragonClient()
    
    all_proposals = []
    
    # 1. Mirror.xyz content
    print("\n📡 Additional Source 1: Mirror.xyz")
    mirror_content = await mirror.get_dao_content(30)
    all_proposals.extend(mirror_content)
    
    # 2. Various DAO Discourse forums
    print("\n📡 Additional Source 2: DAO Forums")
    dao_forums = [
        ("https://forum.makerdao.com", "makerdao"),
        ("https://forum.yearn.finance", "yearn"),
        ("https://forum.1inch.io", "1inch"),
        ("https://forum.sushi.com", "sushi"),
        ("https://research.lido.fi", "lido")
    ]
    
    forum_tasks = []
    for forum_url, dao_name in dao_forums:
        task = discourse.get_forum_proposals(forum_url, dao_name, 10)
        forum_tasks.append(task)
    
    forum_results = await asyncio.gather(*forum_tasks, return_exceptions=True)
    for result in forum_results:
        if isinstance(result, list):
            all_proposals.extend(result)
    
    # 3. Aragon DAOs
    print("\n📡 Additional Source 3: Aragon DAOs")
    aragon_proposals = await aragon.get_aragon_proposals(30)
    all_proposals.extend(aragon_proposals)
    
    print(f"\n📊 Additional sources collected: {len(all_proposals)} proposals")
    
    # Analyze sources
    sources = {}
    for proposal in all_proposals:
        source = proposal.get("source", "unknown")
        sources[source] = sources.get(source, 0) + 1
    
    print(f"   Additional sources breakdown:")
    for source, count in sources.items():
        print(f"     {source}: {count} proposals")
    
    return all_proposals

if __name__ == "__main__":
    asyncio.run(get_additional_dao_sources())
