# enhance_existing_dataset.py - Enhance existing dataset with voting power & price data
"""
Enhance the existing comprehensive dataset with:
- Detailed voting power analysis
- Top voter information and percentages
- Proposer voting power
- DAO descriptions
- Price correlation data
- Governance share analysis
"""

import json
import requests
import time
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime

class DatasetEnhancer:
    """Enhance existing dataset with voting power and price data"""
    
    def __init__(self):
        self.enhanced_data = []
        
        # Token mappings for price analysis
        self.token_mappings = {
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
            "uma.eth": "uma"
        }
    
    def fetch_detailed_votes(self, proposal_id: str) -> List[Dict]:
        """Fetch detailed voting data for a proposal"""
        url = "https://hub.snapshot.org/graphql"
        query = """
        query Votes($proposal_id: String!) {
          votes(first: 1000, where:{proposal:$proposal_id}, orderBy: "vp", orderDirection: desc) { 
            voter vp choice created reason
          }
        }
        """
        
        try:
            response = requests.post(
                url, 
                json={"query": query, "variables": {"proposal_id": proposal_id}},
                timeout=15
            )
            if response.status_code == 200:
                data = response.json()
                if "data" in data and "votes" in data["data"]:
                    return data["data"]["votes"]
        except Exception as e:
            print(f"⚠ Error fetching votes for {proposal_id}: {e}")
        
        return []
    
    def analyze_voting_power_dynamics(self, votes: List[Dict], proposal: Dict) -> Dict:
        """Analyze detailed voting power dynamics"""
        if not votes:
            return {
                "total_voters": 0,
                "total_voting_power": 0,
                "top_voter_address": "",
                "top_voter_vp": 0,
                "top_voter_percentage": 0,
                "top_voter_choice": None,
                "proposer_vp": 0,
                "proposer_percentage": 0,
                "proposer_choice": None,
                "top_10_concentration": 0,
                "voting_power_gini": 0,
                "governance_share_analysis": {}
            }
        
        # Sort votes by voting power
        sorted_votes = sorted(votes, key=lambda x: x["vp"], reverse=True)
        total_vp = sum(v["vp"] for v in votes)
        
        # Top voter analysis
        top_voter = sorted_votes[0]
        top_voter_vp = top_voter["vp"]
        top_voter_percentage = (top_voter_vp / total_vp * 100) if total_vp > 0 else 0
        
        # Top 10 concentration
        top_10_vp = sum(v["vp"] for v in sorted_votes[:10])
        top_10_concentration = (top_10_vp / total_vp * 100) if total_vp > 0 else 0
        
        # Proposer analysis
        proposer = proposal.get("author", proposal.get("Proposer", ""))
        proposer_vote = next((v for v in votes if v["voter"] == proposer), {})
        proposer_vp = proposer_vote.get("vp", 0)
        proposer_percentage = (proposer_vp / total_vp * 100) if total_vp > 0 else 0
        
        # Governance share analysis
        governance_shares = self.calculate_governance_shares(sorted_votes, total_vp)
        
        # Gini coefficient for voting power distribution
        gini = self.calculate_gini_coefficient([v["vp"] for v in votes])
        
        return {
            "total_voters": len(votes),
            "total_voting_power": total_vp,
            "top_voter_address": top_voter["voter"],
            "top_voter_vp": top_voter_vp,
            "top_voter_percentage": round(top_voter_percentage, 3),
            "top_voter_choice": top_voter.get("choice"),
            "proposer_address": proposer,
            "proposer_vp": proposer_vp,
            "proposer_percentage": round(proposer_percentage, 3),
            "proposer_choice": proposer_vote.get("choice"),
            "top_10_concentration": round(top_10_concentration, 3),
            "voting_power_gini": round(gini, 3),
            "governance_share_analysis": governance_shares
        }
    
    def calculate_governance_shares(self, sorted_votes: List[Dict], total_vp: float) -> Dict:
        """Calculate detailed governance share analysis"""
        if not sorted_votes or total_vp == 0:
            return {}
        
        # Define governance share tiers
        tiers = {
            "whale_voters": [],  # >10% voting power
            "large_voters": [],  # 1-10% voting power  
            "medium_voters": [], # 0.1-1% voting power
            "small_voters": []   # <0.1% voting power
        }
        
        for vote in sorted_votes:
            percentage = (vote["vp"] / total_vp) * 100
            
            if percentage > 10:
                tiers["whale_voters"].append(vote)
            elif percentage > 1:
                tiers["large_voters"].append(vote)
            elif percentage > 0.1:
                tiers["medium_voters"].append(vote)
            else:
                tiers["small_voters"].append(vote)
        
        # Calculate tier statistics
        tier_stats = {}
        for tier_name, tier_votes in tiers.items():
            if tier_votes:
                tier_vp = sum(v["vp"] for v in tier_votes)
                tier_stats[tier_name] = {
                    "count": len(tier_votes),
                    "total_vp": tier_vp,
                    "percentage_of_total": round((tier_vp / total_vp) * 100, 2),
                    "avg_vp_per_voter": round(tier_vp / len(tier_votes), 2)
                }
            else:
                tier_stats[tier_name] = {
                    "count": 0,
                    "total_vp": 0,
                    "percentage_of_total": 0,
                    "avg_vp_per_voter": 0
                }
        
        return tier_stats
    
    def calculate_gini_coefficient(self, values: List[float]) -> float:
        """Calculate Gini coefficient for voting power distribution"""
        if not values or len(values) < 2:
            return 0.0
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        cumsum = sum(sorted_values)
        
        if cumsum == 0:
            return 0.0
        
        return (2 * sum((i + 1) * val for i, val in enumerate(sorted_values))) / (n * cumsum) - (n + 1) / n
    
    def get_dao_description(self, dao_name: str) -> str:
        """Get basic DAO description"""
        descriptions = {
            "ens.eth": "Ethereum Name Service (ENS) - Decentralized domain name system for Ethereum addresses and resources",
            "balancer.eth": "Balancer Protocol - Automated portfolio manager and decentralized exchange with programmable liquidity",
            "curve.eth": "Curve Finance - Decentralized exchange optimized for stablecoin and similar asset trading",
            "yearn": "Yearn Finance - Yield optimization protocol aggregating DeFi lending and trading strategies",
            "1inch.eth": "1inch Network - Decentralized exchange aggregator optimizing trades across multiple DEXs",
            "frax.eth": "Frax Protocol - Fractional-algorithmic stablecoin system with governance token",
            "olympusdao.eth": "OlympusDAO - Decentralized reserve currency protocol with bonding and staking mechanisms",
            "fei.eth": "Fei Protocol - Decentralized stablecoin with protocol-controlled value and direct incentives",
            "cream-finance.eth": "Cream Finance - Decentralized lending protocol enabling borrowing and lending of crypto assets",
            "pickle.eth": "Pickle Finance - Yield farming protocol that compounds rewards from other DeFi protocols",
            "uma.eth": "UMA Protocol - Decentralized oracle and synthetic asset platform for financial contracts"
        }
        
        return descriptions.get(dao_name, f"DAO governance space: {dao_name}")
    
    def enhance_dataset(self, input_file: str, output_file: str) -> List[Dict]:
        """Enhance existing dataset with voting power and governance data"""
        print(f"🔧 Enhancing dataset: {input_file}")
        
        # Load existing dataset
        try:
            if input_file.endswith('.csv'):
                df = pd.read_csv(input_file)
                data = df.to_dict('records')
            else:
                with open(input_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            
            print(f"   Loaded {len(data)} proposals")
        except Exception as e:
            print(f"❌ Error loading {input_file}: {e}")
            return []
        
        enhanced_count = 0
        
        for i, proposal in enumerate(data):
            print(f"   Enhancing {i+1}/{len(data)}: {proposal.get('DAO', proposal.get('dao', 'unknown'))}")
            
            # Get proposal ID
            proposal_id = proposal.get("Proposal ID", proposal.get("id", ""))
            dao_name = proposal.get("DAO", proposal.get("dao", ""))
            
            if not proposal_id:
                # Copy original proposal without enhancement
                self.enhanced_data.append(proposal)
                continue
            
            # Fetch detailed voting data
            votes = self.fetch_detailed_votes(proposal_id)
            
            # Analyze voting power dynamics
            voting_analysis = self.analyze_voting_power_dynamics(votes, proposal)
            
            # Get DAO description
            dao_description = self.get_dao_description(dao_name)
            
            # Get token symbol for price analysis
            token_symbol = self.token_mappings.get(dao_name, "")
            
            # Create enhanced proposal record
            enhanced_proposal = proposal.copy()
            enhanced_proposal.update({
                # DAO information
                "dao_description": dao_description,
                "token_symbol": token_symbol,
                
                # Voting power analysis
                **voting_analysis,
                
                # Research metadata
                "enhancement_date": datetime.now().isoformat(),
                "has_detailed_voting_data": len(votes) > 0,
                "research_ready": len(votes) > 10  # Minimum threshold for analysis
            })
            
            self.enhanced_data.append(enhanced_proposal)
            enhanced_count += 1
            
            # Rate limiting
            time.sleep(0.1)
        
        print(f"   ✅ Enhanced {enhanced_count} proposals with voting data")
        
        # Save enhanced dataset
        self.save_enhanced_dataset(output_file)
        
        return self.enhanced_data
    
    def save_enhanced_dataset(self, filename: str):
        """Save enhanced dataset"""
        print(f"💾 Saving enhanced dataset...")
        
        # JSON format
        with open(f"{filename}.json", "w", encoding="utf-8") as f:
            json.dump(self.enhanced_data, f, indent=2, ensure_ascii=False)
        
        # CSV format
        try:
            df = pd.DataFrame(self.enhanced_data)
            df.to_csv(f"{filename}.csv", index=False, encoding="utf-8")
            print(f"✅ Enhanced dataset saved:")
            print(f"   📄 {filename}.json ({len(self.enhanced_data)} proposals)")
            print(f"   📄 {filename}.csv")
        except Exception as e:
            print(f"⚠ CSV export error: {e}")
            print(f"✅ Enhanced dataset saved:")
            print(f"   📄 {filename}.json ({len(self.enhanced_data)} proposals)")
        
        # Generate enhancement summary
        self.generate_enhancement_summary(filename)
    
    def generate_enhancement_summary(self, filename: str):
        """Generate summary of enhancements"""
        if not self.enhanced_data:
            return
        
        research_ready = len([p for p in self.enhanced_data if p.get("research_ready", False)])
        with_voting_data = len([p for p in self.enhanced_data if p.get("has_detailed_voting_data", False)])
        
        # Voting power statistics
        voting_powers = [p.get("top_voter_percentage", 0) for p in self.enhanced_data if p.get("top_voter_percentage")]
        avg_top_voter_power = sum(voting_powers) / len(voting_powers) if voting_powers else 0
        
        # Governance concentration
        concentrations = [p.get("top_10_concentration", 0) for p in self.enhanced_data if p.get("top_10_concentration")]
        avg_concentration = sum(concentrations) / len(concentrations) if concentrations else 0
        
        summary = {
            "enhancement_summary": {
                "total_proposals": len(self.enhanced_data),
                "research_ready_proposals": research_ready,
                "proposals_with_voting_data": with_voting_data,
                "enhancement_success_rate": round((with_voting_data / len(self.enhanced_data)) * 100, 1)
            },
            "voting_power_insights": {
                "avg_top_voter_power_pct": round(avg_top_voter_power, 2),
                "avg_top_10_concentration_pct": round(avg_concentration, 2),
                "proposals_with_whale_voters": len([p for p in self.enhanced_data 
                                                  if p.get("top_voter_percentage", 0) > 10])
            },
            "research_applications": [
                "Voting power concentration analysis",
                "Governance share distribution studies", 
                "Proposal outcome correlation with voting power",
                "DAO governance mechanism comparison",
                "Token price impact analysis (with price data integration)"
            ]
        }
        
        with open(f"{filename}_enhancement_summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        print(f"   📄 {filename}_enhancement_summary.json")
        print(f"📊 Enhancement Summary:")
        print(f"   Research-ready proposals: {research_ready}/{len(self.enhanced_data)}")
        print(f"   Average top voter power: {avg_top_voter_power:.1f}%")
        print(f"   Average top-10 concentration: {avg_concentration:.1f}%")

def main():
    """Main enhancement function"""
    enhancer = DatasetEnhancer()
    
    # Try to enhance existing datasets
    datasets_to_enhance = [
        ("comprehensive_research_dataset.json", "voting_enhanced_research_dataset"),
        ("fast_major_daos_dataset.json", "voting_enhanced_major_daos_dataset")
    ]
    
    for input_file, output_base in datasets_to_enhance:
        try:
            print(f"\n🔧 Attempting to enhance {input_file}...")
            enhanced_data = enhancer.enhance_dataset(input_file, output_base)
            
            if enhanced_data:
                print(f"✅ Successfully enhanced {input_file}")
                print(f"   Output: {output_base}.json/.csv")
                break
            
        except FileNotFoundError:
            print(f"   ⚠ {input_file} not found")
            continue
        except Exception as e:
            print(f"   ❌ Enhancement failed: {e}")
            continue
    else:
        print("❌ No suitable dataset found for enhancement")
        print("   Run data collection first to generate base datasets")

if __name__ == "__main__":
    main()
