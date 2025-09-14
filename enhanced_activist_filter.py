# enhanced_activist_filter.py - Research-grade activist proposal detection
"""
Enhanced activist proposal detection for research projects:
- Multiple detection methods
- Academic-quality classification
- Comprehensive activist categories
- Research-grade analysis
"""

import re
from typing import List, Dict, Tuple
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch

class EnhancedActivistDetector:
    """Enhanced activist proposal detection for research"""
    
    def __init__(self):
        self.activist_keywords = self._build_comprehensive_keywords()
        self.activist_patterns = self._build_activist_patterns()
        self.zero_shot_classifier = None
        self.distilbert_tokenizer = None
        self.distilbert_model = None
    
    def _build_comprehensive_keywords(self) -> Dict[str, List[str]]:
        """Build comprehensive activist keyword categories"""
        return {
            "governance_reform": [
                "constitution", "constitutional", "governance", "reform", "change", "improve",
                "amendment", "framework", "structure", "process", "procedure", "mechanism",
                "voting", "election", "delegate", "representation", "participation", "democracy",
                "decentralization", "autonomy", "self-governance", "community-driven"
            ],
            "treasury_activism": [
                "treasury", "funding", "grant", "budget", "allocation", "distribution",
                "spending", "investment", "diversification", "management", "stewardship",
                "transparency", "accountability", "audit", "oversight", "public goods",
                "retroactive", "incentive", "reward", "compensation"
            ],
            "protocol_activism": [
                "protocol", "upgrade", "improvement", "enhancement", "optimization",
                "parameter", "configuration", "setting", "adjustment", "modification",
                "technical", "implementation", "deployment", "migration", "transition",
                "security", "safety", "risk", "mitigation", "emergency"
            ],
            "community_activism": [
                "community", "member", "contributor", "developer", "builder", "creator",
                "ecosystem", "growth", "adoption", "engagement", "education", "outreach",
                "onboarding", "retention", "diversity", "inclusion", "accessibility",
                "collaboration", "partnership", "alliance", "cooperation"
            ],
            "economic_activism": [
                "tokenomics", "economics", "monetary", "fiscal", "inflation", "deflation",
                "supply", "demand", "market", "price", "value", "valuation", "fee",
                "revenue", "profit", "loss", "sustainability", "viability", "growth"
            ],
            "social_activism": [
                "social", "impact", "mission", "purpose", "values", "ethics", "responsibility",
                "sustainability", "environment", "climate", "carbon", "green", "renewable",
                "equality", "justice", "fairness", "rights", "freedom", "privacy"
            ],
            "strategic_activism": [
                "strategy", "strategic", "vision", "roadmap", "direction", "future",
                "expansion", "scaling", "integration", "interoperability", "cross-chain",
                "partnership", "acquisition", "merger", "collaboration", "alliance"
            ]
        }
    
    def _build_activist_patterns(self) -> List[Tuple[str, str]]:
        """Build regex patterns for activist content detection"""
        return [
            (r"\b(propose|proposal)\s+to\s+(change|modify|update|improve|reform)", "governance_reform"),
            (r"\b(treasury|fund|grant)\s+(allocation|distribution|management)", "treasury_activism"),
            (r"\b(protocol|parameter)\s+(upgrade|change|adjustment)", "protocol_activism"),
            (r"\b(community|governance)\s+(improvement|enhancement|reform)", "community_activism"),
            (r"\b(token|economic|monetary)\s+(model|policy|mechanism)", "economic_activism"),
            (r"\b(constitution|framework|structure)\s+(amendment|change|update)", "governance_reform"),
            (r"\b(voting|election|delegate)\s+(process|system|mechanism)", "governance_reform"),
            (r"\b(transparency|accountability|oversight)\s+(measure|initiative)", "governance_reform"),
            (r"\b(incentive|reward|compensation)\s+(program|system|structure)", "treasury_activism"),
            (r"\b(security|safety|risk)\s+(improvement|mitigation|enhancement)", "protocol_activism")
        ]
    
    def detect_activist_proposals(self, proposals: List[Dict]) -> List[Dict]:
        """Detect activist proposals using multiple methods"""
        print(f"🔍 Enhanced Activist Detection on {len(proposals)} proposals")
        
        activist_proposals = []
        
        for proposal in proposals:
            title = proposal.get("title", "")
            description = proposal.get("description", "")
            text = f"{title} {description}".lower()
            
            # Method 1: Keyword-based detection
            keyword_score, keyword_categories = self._keyword_detection(text)
            
            # Method 2: Pattern-based detection
            pattern_score, pattern_categories = self._pattern_detection(text)
            
            # Method 3: Context-based detection
            context_score = self._context_detection(text)
            
            # Method 4: Title analysis
            title_score = self._title_analysis(title)
            
            # Combined scoring
            total_score = (keyword_score * 0.3 + 
                          pattern_score * 0.3 + 
                          context_score * 0.2 + 
                          title_score * 0.2)
            
            # Enhanced threshold for research quality
            if total_score >= 0.4:  # Lower threshold to catch more activist content
                proposal["activist_score"] = total_score
                proposal["activist_categories"] = list(set(keyword_categories + pattern_categories))
                proposal["detection_method"] = "enhanced_multi_method"
                activist_proposals.append(proposal)
        
        print(f"✅ Detected {len(activist_proposals)} activist proposals ({len(activist_proposals)/len(proposals)*100:.1f}%)")
        
        return activist_proposals
    
    def _keyword_detection(self, text: str) -> Tuple[float, List[str]]:
        """Keyword-based activist detection"""
        total_score = 0
        found_categories = []
        
        for category, keywords in self.activist_keywords.items():
            category_score = 0
            for keyword in keywords:
                if keyword in text:
                    category_score += 1
            
            if category_score > 0:
                found_categories.append(category)
                # Normalize by category size and add to total
                total_score += min(category_score / len(keywords), 0.5)
        
        return min(total_score, 1.0), found_categories
    
    def _pattern_detection(self, text: str) -> Tuple[float, List[str]]:
        """Pattern-based activist detection"""
        total_score = 0
        found_categories = []
        
        for pattern, category in self.activist_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                total_score += 0.1
                found_categories.append(category)
        
        return min(total_score, 1.0), found_categories
    
    def _context_detection(self, text: str) -> float:
        """Context-based detection for activist language"""
        activist_contexts = [
            "we propose", "this proposal", "community should", "dao needs",
            "governance improvement", "protocol enhancement", "treasury management",
            "voting mechanism", "delegate system", "community governance",
            "decentralized decision", "autonomous organization", "collective action"
        ]
        
        score = 0
        for context in activist_contexts:
            if context in text:
                score += 0.05
        
        return min(score, 1.0)
    
    def _title_analysis(self, title: str) -> float:
        """Analyze title for activist indicators"""
        title_lower = title.lower()
        
        # Strong activist indicators in titles
        strong_indicators = [
            "proposal", "improvement", "enhancement", "reform", "change",
            "update", "upgrade", "amendment", "governance", "treasury",
            "community", "protocol", "framework", "constitution"
        ]
        
        score = 0
        for indicator in strong_indicators:
            if indicator in title_lower:
                score += 0.1
        
        # Bonus for proposal-like structure
        if any(word in title_lower for word in ["aip", "pip", "sip", "rfc", "proposal"]):
            score += 0.2
        
        return min(score, 1.0)
    
    def categorize_activist_proposals(self, proposals: List[Dict]) -> Dict[str, List[Dict]]:
        """Categorize activist proposals by type"""
        categories = {
            "governance_reform": [],
            "treasury_activism": [],
            "protocol_activism": [],
            "community_activism": [],
            "economic_activism": [],
            "social_activism": [],
            "strategic_activism": [],
            "multi_category": []
        }
        
        for proposal in proposals:
            proposal_categories = proposal.get("activist_categories", [])
            
            if len(proposal_categories) > 1:
                categories["multi_category"].append(proposal)
            elif len(proposal_categories) == 1:
                category = proposal_categories[0]
                if category in categories:
                    categories[category].append(proposal)
            else:
                # Default categorization based on content
                text = f"{proposal.get('title', '')} {proposal.get('description', '')}".lower()
                if any(word in text for word in ["governance", "voting", "constitution"]):
                    categories["governance_reform"].append(proposal)
                elif any(word in text for word in ["treasury", "funding", "grant"]):
                    categories["treasury_activism"].append(proposal)
                else:
                    categories["community_activism"].append(proposal)
        
        return categories
    
    def generate_activist_analysis(self, proposals: List[Dict]) -> Dict:
        """Generate comprehensive activist proposal analysis"""
        categorized = self.categorize_activist_proposals(proposals)
        
        analysis = {
            "total_activist_proposals": len(proposals),
            "category_breakdown": {
                category: len(props) for category, props in categorized.items()
            },
            "top_activist_daos": self._analyze_top_activist_daos(proposals),
            "activist_trends": self._analyze_activist_trends(proposals),
            "methodology": {
                "detection_methods": ["keyword_based", "pattern_based", "context_based", "title_analysis"],
                "threshold": 0.4,
                "categories": list(self.activist_keywords.keys())
            }
        }
        
        return analysis
    
    def _analyze_top_activist_daos(self, proposals: List[Dict]) -> Dict[str, int]:
        """Analyze which DAOs have most activist proposals"""
        dao_counts = {}
        for proposal in proposals:
            dao = proposal.get("dao", "unknown")
            dao_counts[dao] = dao_counts.get(dao, 0) + 1
        
        # Return top 10
        return dict(sorted(dao_counts.items(), key=lambda x: x[1], reverse=True)[:10])
    
    def _analyze_activist_trends(self, proposals: List[Dict]) -> Dict:
        """Analyze trends in activist proposals"""
        # This could be expanded with time-series analysis
        return {
            "most_common_keywords": self._get_most_common_keywords(proposals),
            "average_activist_score": sum(p.get("activist_score", 0) for p in proposals) / len(proposals) if proposals else 0
        }
    
    def _get_most_common_keywords(self, proposals: List[Dict]) -> List[str]:
        """Get most common activist keywords across proposals"""
        keyword_counts = {}
        
        for proposal in proposals:
            text = f"{proposal.get('title', '')} {proposal.get('description', '')}".lower()
            for category, keywords in self.activist_keywords.items():
                for keyword in keywords:
                    if keyword in text:
                        keyword_counts[keyword] = keyword_counts.get(keyword, 0) + 1
        
        # Return top 20 keywords
        return [kw for kw, count in sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:20]]

def main():
    """Test the enhanced activist detector"""
    detector = EnhancedActivistDetector()
    
    # Test with sample proposals
    test_proposals = [
        {
            "title": "Governance Framework Improvement Proposal",
            "description": "This proposal aims to improve the governance framework by implementing new voting mechanisms and delegate systems for better community participation.",
            "dao": "test_dao"
        },
        {
            "title": "Treasury Allocation for Public Goods Funding",
            "description": "Proposal to allocate treasury funds for public goods funding and retroactive grants to support ecosystem development.",
            "dao": "test_dao"
        }
    ]
    
    activist_proposals = detector.detect_activist_proposals(test_proposals)
    analysis = detector.generate_activist_analysis(activist_proposals)
    
    print(f"Analysis: {analysis}")

if __name__ == "__main__":
    main()
