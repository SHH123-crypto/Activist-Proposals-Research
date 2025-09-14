# immediate_expansion_scraper.py - Immediate expansion using existing data + lower thresholds
"""
Immediate Expansion Scraper to get more activist proposals by:
1. Lowering activist detection threshold from 0.25 to 0.15
2. Re-analyzing existing comprehensive dataset with relaxed criteria
3. Adding more nuanced detection patterns
4. Expanding to all 681 proposals in comprehensive dataset
"""

import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Tuple
import os
from pathlib import Path
import re
from textblob import TextBlob

class ImmediateExpansionScraper:
    """Immediate expansion using existing data with relaxed criteria"""
    
    def __init__(self):
        self.output_dir = "immediate_expansion_data"
        Path(self.output_dir).mkdir(exist_ok=True)
        
        # Enhanced activist patterns with more inclusive criteria
        self.relaxed_activist_patterns = {
            # Governance changes (more inclusive)
            "governance_change": [
                r"change.*governance", r"modify.*voting", r"alter.*constitution",
                r"amend.*charter", r"restructure.*dao", r"reform.*protocol",
                r"governance.*upgrade", r"voting.*mechanism", r"consensus.*change",
                r"update.*governance", r"improve.*voting", r"enhance.*governance"
            ],
            
            # Financial/treasury (expanded)
            "financial_activism": [
                r"treasury.*allocation", r"fund.*reallocation", r"budget.*revision",
                r"spending.*proposal", r"investment.*strategy", r"diversify.*treasury",
                r"liquidate.*position", r"sell.*tokens", r"buy.*back",
                r"treasury.*management", r"fund.*distribution", r"allocate.*funds",
                r"financial.*strategy", r"revenue.*sharing", r"profit.*distribution"
            ],
            
            # Protocol changes (expanded)
            "protocol_activism": [
                r"protocol.*change", r"parameter.*adjustment", r"fee.*modification",
                r"reward.*restructure", r"emission.*change", r"tokenomics.*update",
                r"upgrade.*contract", r"migrate.*protocol", r"update.*parameters",
                r"adjust.*fees", r"modify.*rewards", r"change.*emissions"
            ],
            
            # Leadership/team (expanded)
            "leadership_change": [
                r"remove.*team", r"replace.*lead", r"elect.*new", r"dismiss.*member",
                r"hire.*external", r"change.*management", r"new.*steward",
                r"appoint.*lead", r"select.*team", r"choose.*representative"
            ],
            
            # Emergency/urgent (expanded)
            "emergency_action": [
                r"emergency.*proposal", r"urgent.*action", r"immediate.*response",
                r"crisis.*management", r"halt.*protocol", r"pause.*contract",
                r"urgent.*fix", r"emergency.*update", r"critical.*change"
            ],
            
            # Community initiatives (expanded)
            "community_initiative": [
                r"community.*proposal", r"grassroots.*initiative", r"member.*driven",
                r"collective.*action", r"decentralized.*decision", r"bottom.*up",
                r"community.*vote", r"member.*proposal", r"user.*initiative"
            ],
            
            # New categories for more coverage
            "strategic_change": [
                r"strategic.*shift", r"direction.*change", r"roadmap.*update",
                r"vision.*change", r"mission.*update", r"strategy.*revision"
            ],
            
            "partnership_activism": [
                r"partnership.*proposal", r"collaboration.*initiative", r"alliance.*formation",
                r"integration.*proposal", r"merger.*discussion", r"acquisition.*proposal"
            ],
            
            "technical_activism": [
                r"technical.*upgrade", r"infrastructure.*change", r"architecture.*update",
                r"security.*enhancement", r"performance.*improvement", r"scalability.*solution"
            ]
        }
        
        # Relaxed sentiment indicators
        self.relaxed_sentiment_indicators = [
            "should", "must", "need to", "propose", "change", "improve",
            "reform", "restructure", "demand", "require", "urgent", "critical",
            "important", "necessary", "recommend", "suggest", "update", "modify",
            "enhance", "optimize", "revise", "adjust", "implement", "introduce"
        ]
        
        print(f"⚡ Immediate Expansion Scraper initialized")
        print(f"   Strategy: Relaxed criteria + comprehensive dataset re-analysis")
        print(f"   Target: 100-200 activist proposals (4-8x expansion)")
        print(f"   Enhanced patterns: {sum(len(patterns) for patterns in self.relaxed_activist_patterns.values())}")
        print(f"   Output directory: {self.output_dir}")
    
    def relaxed_activist_detection(self, proposal: Dict) -> Tuple[float, List[str], str]:
        """Relaxed activist proposal detection with lower thresholds"""
        title = str(proposal.get("title", proposal.get("Title", ""))).lower()
        body = str(proposal.get("body", proposal.get("Body", ""))).lower()
        
        combined_text = f"{title} {body}"
        
        activist_score = 0.0
        detection_methods = []
        
        # Method 1: Enhanced pattern matching (more generous scoring)
        pattern_score = 0
        matched_categories = []
        
        for category, patterns in self.relaxed_activist_patterns.items():
            category_matches = 0
            for pattern in patterns:
                if re.search(pattern, combined_text, re.IGNORECASE):
                    category_matches += 1
            
            if category_matches > 0:
                # More generous scoring
                pattern_score += min(category_matches * 0.1, 0.25)  # Lower threshold per match
                matched_categories.append(category)
        
        activist_score += min(pattern_score, 0.5)  # Lower cap
        
        if matched_categories:
            detection_methods.append(f"pattern_matching_{'+'.join(matched_categories)}")
        
        # Method 2: Relaxed sentiment analysis
        try:
            blob = TextBlob(combined_text)
            sentiment_score = 0
            
            # Check for relaxed sentiment indicators
            for indicator in self.relaxed_sentiment_indicators:
                if indicator in combined_text:
                    sentiment_score += 0.03  # Lower per-indicator score
            
            # More generous polarity analysis
            if abs(blob.sentiment.polarity) > 0.2:  # Lower threshold
                sentiment_score += 0.08
            
            activist_score += min(sentiment_score, 0.2)  # Lower cap
            
            if sentiment_score > 0:
                detection_methods.append("sentiment_analysis")
        
        except:
            pass
        
        # Method 3: Relaxed structural analysis
        structural_score = 0
        
        # Lower thresholds for structural indicators
        if len(combined_text) > 500:  # Lower threshold
            structural_score += 0.05
        
        if combined_text.count('\n') > 3 or combined_text.count('•') > 2:  # Lower threshold
            structural_score += 0.03
        
        # More generous parameter detection
        if re.search(r'\d+%|\$\d+|parameter|threshold|limit|change|update', combined_text):
            structural_score += 0.07
        
        activist_score += min(structural_score, 0.1)
        
        if structural_score > 0:
            detection_methods.append("structural_analysis")
        
        # Method 4: Title-based activism detection (new)
        title_score = 0
        title_keywords = ["proposal", "change", "update", "modify", "improve", "reform", "restructure"]
        
        for keyword in title_keywords:
            if keyword in title:
                title_score += 0.02
        
        activist_score += min(title_score, 0.1)
        
        if title_score > 0:
            detection_methods.append("title_analysis")
        
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
    
    def analyze_comprehensive_dataset(self, min_score: float = 0.15) -> List[Dict]:
        """Re-analyze comprehensive dataset with relaxed criteria"""
        print(f"📊 Re-analyzing comprehensive dataset with relaxed criteria...")
        print(f"   Minimum activist score: {min_score}")
        
        # Load comprehensive dataset
        df = pd.read_csv('comprehensive_research_dataset.csv')
        proposals = df.to_dict('records')
        
        print(f"   📥 Loaded {len(proposals)} total proposals")
        
        activist_proposals = []
        
        for proposal in proposals:
            try:
                # Apply relaxed activist detection
                activist_score, detection_methods, method_summary = \
                    self.relaxed_activist_detection(proposal)
                
                if activist_score >= min_score:
                    proposal['activist_score'] = activist_score
                    proposal['detection_methods'] = detection_methods
                    proposal['detection_summary'] = method_summary
                    activist_proposals.append(proposal)
                    
            except Exception as e:
                continue
        
        print(f"   ✅ Found {len(activist_proposals)} activist proposals")
        print(f"   📈 Activist rate: {len(activist_proposals)/len(proposals)*100:.1f}%")
        print(f"   🚀 Expansion factor: {len(activist_proposals)/27:.1f}x (from 27 to {len(activist_proposals)})")
        
        return activist_proposals
    
    def analyze_score_distribution(self, proposals: List[Dict]) -> Dict:
        """Analyze the distribution of activist scores"""
        scores = [p.get('activist_score', 0) for p in proposals]
        
        distribution = {
            "total_proposals": len(proposals),
            "score_stats": {
                "min": min(scores) if scores else 0,
                "max": max(scores) if scores else 0,
                "mean": sum(scores) / len(scores) if scores else 0,
                "median": sorted(scores)[len(scores)//2] if scores else 0
            },
            "score_ranges": {
                "0.15-0.20": len([s for s in scores if 0.15 <= s < 0.20]),
                "0.20-0.25": len([s for s in scores if 0.20 <= s < 0.25]),
                "0.25-0.30": len([s for s in scores if 0.25 <= s < 0.30]),
                "0.30-0.35": len([s for s in scores if 0.30 <= s < 0.35]),
                "0.35-0.40": len([s for s in scores if 0.35 <= s < 0.40]),
                "0.40+": len([s for s in scores if s >= 0.40])
            },
            "detection_methods": {}
        }
        
        # Analyze detection methods
        for proposal in proposals:
            methods = proposal.get('detection_methods', [])
            for method in methods:
                distribution["detection_methods"][method] = \
                    distribution["detection_methods"].get(method, 0) + 1
        
        return distribution
    
    def analyze_dao_coverage(self, proposals: List[Dict]) -> Dict:
        """Analyze DAO coverage in expanded dataset"""
        dao_stats = {}
        
        for proposal in proposals:
            dao = proposal.get('DAO', proposal.get('dao', 'unknown'))
            if dao not in dao_stats:
                dao_stats[dao] = {
                    "proposals": 0,
                    "avg_activist_score": 0,
                    "score_sum": 0
                }
            
            dao_stats[dao]["proposals"] += 1
            dao_stats[dao]["score_sum"] += proposal.get('activist_score', 0)
        
        # Calculate averages
        for dao in dao_stats:
            if dao_stats[dao]["proposals"] > 0:
                dao_stats[dao]["avg_activist_score"] = \
                    dao_stats[dao]["score_sum"] / dao_stats[dao]["proposals"]
        
        return dao_stats
    
    def save_expanded_dataset(self, proposals: List[Dict]) -> str:
        """Save expanded dataset with comprehensive analysis"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save main dataset
        csv_file = f"{self.output_dir}/expanded_activist_proposals_{timestamp}.csv"
        df = pd.DataFrame(proposals)
        df.to_csv(csv_file, index=False)
        
        json_file = f"{self.output_dir}/expanded_activist_proposals_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump(proposals, f, indent=2, default=str)
        
        # Generate comprehensive analysis
        score_distribution = self.analyze_score_distribution(proposals)
        dao_coverage = self.analyze_dao_coverage(proposals)
        
        analysis = {
            "timestamp": timestamp,
            "expansion_summary": {
                "original_proposals": 27,
                "expanded_proposals": len(proposals),
                "expansion_factor": len(proposals) / 27 if len(proposals) > 0 else 0,
                "min_activist_score": 0.15,
                "detection_method": "relaxed_criteria"
            },
            "score_distribution": score_distribution,
            "dao_coverage": dao_coverage,
            "top_daos_by_proposals": sorted(
                dao_coverage.items(), 
                key=lambda x: x[1]["proposals"], 
                reverse=True
            )[:10],
            "top_daos_by_activist_score": sorted(
                dao_coverage.items(), 
                key=lambda x: x[1]["avg_activist_score"], 
                reverse=True
            )[:10]
        }
        
        analysis_file = f"{self.output_dir}/expansion_analysis_{timestamp}.json"
        with open(analysis_file, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        
        print(f"\n💾 Expanded dataset saved:")
        print(f"   📊 CSV: {csv_file}")
        print(f"   📄 JSON: {json_file}")
        print(f"   📈 Analysis: {analysis_file}")
        
        return csv_file

def main():
    """Main execution function"""
    scraper = ImmediateExpansionScraper()
    
    try:
        # Analyze comprehensive dataset with relaxed criteria
        activist_proposals = scraper.analyze_comprehensive_dataset(min_score=0.15)
        
        if activist_proposals:
            # Save expanded dataset
            csv_file = scraper.save_expanded_dataset(activist_proposals)
            
            # Print detailed results
            dao_coverage = scraper.analyze_dao_coverage(activist_proposals)
            
            print(f"\n🎉 IMMEDIATE EXPANSION COMPLETE!")
            print(f"   🎯 Activist proposals found: {len(activist_proposals)}")
            print(f"   🚀 Expansion factor: {len(activist_proposals)/27:.1f}x")
            print(f"   🏛️ DAOs covered: {len(dao_coverage)}")
            print(f"   📁 Dataset saved: {csv_file}")
            
            print(f"\n📊 TOP DAOs BY ACTIVIST PROPOSALS:")
            sorted_daos = sorted(dao_coverage.items(), key=lambda x: x[1]["proposals"], reverse=True)
            for dao, stats in sorted_daos[:10]:
                print(f"   {dao}: {stats['proposals']} proposals (avg score: {stats['avg_activist_score']:.3f})")
            
            print(f"\n⚡ Ready for price data collection on expanded dataset!")
            
        else:
            print("❌ No activist proposals found")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
