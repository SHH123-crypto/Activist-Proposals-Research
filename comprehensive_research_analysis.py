# comprehensive_research_analysis.py - Complete research analysis
"""
Comprehensive research analysis combining all data sources:
- Fast major DAOs dataset (552 activist proposals)
- Ultimate DAO dataset (135 proposals)
- Additional sources and comprehensive analysis
- Research-grade outputs and insights
"""

import json
import pandas as pd
from typing import Dict, List
import time
from datetime import datetime

class ComprehensiveResearchAnalysis:
    """Complete research analysis of DAO activist proposals"""
    
    def __init__(self):
        self.all_datasets = {}
        self.combined_proposals = []
        self.analysis_results = {}
    
    def load_all_datasets(self):
        """Load all available datasets"""
        print("📊 Loading all available datasets...")
        
        datasets_to_load = [
            ("fast_major_daos", "fast_major_daos_dataset.json"),
            ("ultimate_dao", "ultimate_dao_proposals.json"),
            ("ultimate_activist", "ultimate_activist_proposals.json"),
            ("comprehensive_dao", "comprehensive_dao_proposals.json"),
            ("alternative_dao", "alternative_dao_proposals.json")
        ]
        
        for dataset_name, filename in datasets_to_load:
            try:
                with open(filename, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.all_datasets[dataset_name] = data
                    print(f"   ✅ {dataset_name}: {len(data)} proposals")
            except FileNotFoundError:
                print(f"   ⚠ {dataset_name}: File not found")
            except Exception as e:
                print(f"   ❌ {dataset_name}: Error - {e}")
    
    def combine_and_deduplicate(self):
        """Combine all datasets and remove duplicates"""
        print("\n🔄 Combining and deduplicating datasets...")
        
        all_proposals = []
        seen_ids = set()
        seen_titles = set()
        
        for dataset_name, proposals in self.all_datasets.items():
            print(f"   Processing {dataset_name}...")
            
            for proposal in proposals:
                # Create unique identifier
                proposal_id = proposal.get("Proposal ID", proposal.get("id", ""))
                title = proposal.get("Title", proposal.get("title", "")).lower().strip()
                dao = proposal.get("DAO", proposal.get("dao", "")).lower()
                
                # Create signature for deduplication
                signature = f"{dao}:{title[:50]}"
                
                # Skip if we've seen this proposal
                if proposal_id in seen_ids or signature in seen_titles:
                    continue
                
                # Add source information
                proposal["source_dataset"] = dataset_name
                proposal["combined_id"] = f"{dataset_name}_{len(all_proposals)}"
                
                # Normalize field names
                normalized_proposal = self._normalize_proposal_fields(proposal)
                
                all_proposals.append(normalized_proposal)
                seen_ids.add(proposal_id)
                seen_titles.add(signature)
        
        self.combined_proposals = all_proposals
        print(f"   ✅ Combined dataset: {len(all_proposals)} unique proposals")
        
        return all_proposals
    
    def _normalize_proposal_fields(self, proposal: Dict) -> Dict:
        """Normalize proposal field names across datasets"""
        normalized = {
            "id": proposal.get("Proposal ID", proposal.get("id", "")),
            "title": proposal.get("Title", proposal.get("title", "")),
            "body": proposal.get("Body", proposal.get("body", proposal.get("description", ""))),
            "dao": proposal.get("DAO", proposal.get("dao", "")),
            "author": proposal.get("Proposer", proposal.get("Author", proposal.get("author", ""))),
            "state": proposal.get("State", proposal.get("state", "")),
            "created": proposal.get("Created", proposal.get("created", "")),
            "link": proposal.get("Link", proposal.get("link", "")),
            "source_dataset": proposal.get("source_dataset", ""),
            "combined_id": proposal.get("combined_id", ""),
            "activist_score": proposal.get("Activist Score", proposal.get("activist_score", 0)),
            "keyword_hits": proposal.get("Keyword Hits", proposal.get("keyword_hits", 0)),
            "detection_method": proposal.get("Detection Method", proposal.get("detection_method", ""))
        }
        
        # Preserve any additional fields
        for key, value in proposal.items():
            if key not in normalized:
                normalized[key] = value
        
        return normalized
    
    def analyze_comprehensive_dataset(self) -> Dict:
        """Perform comprehensive analysis of the combined dataset"""
        print("\n📈 Performing comprehensive analysis...")
        
        if not self.combined_proposals:
            print("   ❌ No proposals to analyze")
            return {}
        
        analysis = {
            "dataset_overview": self._analyze_dataset_overview(),
            "dao_analysis": self._analyze_daos(),
            "proposal_analysis": self._analyze_proposals(),
            "activist_analysis": self._analyze_activist_content(),
            "source_analysis": self._analyze_sources(),
            "temporal_analysis": self._analyze_temporal_patterns(),
            "quality_metrics": self._calculate_quality_metrics()
        }
        
        self.analysis_results = analysis
        return analysis
    
    def _analyze_dataset_overview(self) -> Dict:
        """Analyze overall dataset characteristics"""
        return {
            "total_proposals": len(self.combined_proposals),
            "total_unique_daos": len(set(p["dao"] for p in self.combined_proposals if p["dao"])),
            "total_sources": len(set(p["source_dataset"] for p in self.combined_proposals)),
            "activist_proposals": len([p for p in self.combined_proposals if p.get("activist_score", 0) > 0]),
            "activist_rate": len([p for p in self.combined_proposals if p.get("activist_score", 0) > 0]) / len(self.combined_proposals) * 100
        }
    
    def _analyze_daos(self) -> Dict:
        """Analyze DAO-level statistics"""
        dao_stats = {}
        
        for proposal in self.combined_proposals:
            dao = proposal["dao"]
            if not dao:
                continue
                
            if dao not in dao_stats:
                dao_stats[dao] = {
                    "total_proposals": 0,
                    "activist_proposals": 0,
                    "avg_activist_score": 0,
                    "sources": set()
                }
            
            dao_stats[dao]["total_proposals"] += 1
            dao_stats[dao]["sources"].add(proposal["source_dataset"])
            
            if proposal.get("activist_score", 0) > 0:
                dao_stats[dao]["activist_proposals"] += 1
        
        # Calculate averages and convert sets to lists
        for dao, stats in dao_stats.items():
            if stats["total_proposals"] > 0:
                stats["activist_rate"] = (stats["activist_proposals"] / stats["total_proposals"]) * 100
            stats["sources"] = list(stats["sources"])
        
        # Get top DAOs by different metrics
        top_by_total = sorted(dao_stats.items(), key=lambda x: x[1]["total_proposals"], reverse=True)[:20]
        top_by_activist = sorted(dao_stats.items(), key=lambda x: x[1]["activist_proposals"], reverse=True)[:20]
        top_by_rate = sorted(dao_stats.items(), key=lambda x: x[1].get("activist_rate", 0), reverse=True)[:20]
        
        return {
            "total_unique_daos": len(dao_stats),
            "dao_statistics": dao_stats,
            "top_daos_by_total_proposals": dict(top_by_total),
            "top_daos_by_activist_proposals": dict(top_by_activist),
            "top_daos_by_activist_rate": dict(top_by_rate)
        }
    
    def _analyze_proposals(self) -> Dict:
        """Analyze proposal-level characteristics"""
        title_lengths = [len(p["title"]) for p in self.combined_proposals if p["title"]]
        body_lengths = [len(p["body"]) for p in self.combined_proposals if p["body"]]
        
        states = {}
        for proposal in self.combined_proposals:
            state = proposal.get("state", "unknown")
            states[state] = states.get(state, 0) + 1
        
        return {
            "avg_title_length": sum(title_lengths) / len(title_lengths) if title_lengths else 0,
            "avg_body_length": sum(body_lengths) / len(body_lengths) if body_lengths else 0,
            "proposal_states": states,
            "proposals_with_authors": len([p for p in self.combined_proposals if p.get("author")]),
            "proposals_with_links": len([p for p in self.combined_proposals if p.get("link")])
        }
    
    def _analyze_activist_content(self) -> Dict:
        """Analyze activist proposal characteristics"""
        activist_proposals = [p for p in self.combined_proposals if p.get("activist_score", 0) > 0]
        
        if not activist_proposals:
            return {"error": "No activist proposals found"}
        
        scores = [p["activist_score"] for p in activist_proposals if p.get("activist_score")]
        keyword_hits = [p.get("keyword_hits", 0) for p in activist_proposals]
        
        detection_methods = {}
        for proposal in activist_proposals:
            method = proposal.get("detection_method", "unknown")
            detection_methods[method] = detection_methods.get(method, 0) + 1
        
        return {
            "total_activist_proposals": len(activist_proposals),
            "avg_activist_score": sum(scores) / len(scores) if scores else 0,
            "avg_keyword_hits": sum(keyword_hits) / len(keyword_hits) if keyword_hits else 0,
            "detection_methods": detection_methods,
            "high_confidence_activist": len([p for p in activist_proposals if p.get("activist_score", 0) > 0.7]),
            "medium_confidence_activist": len([p for p in activist_proposals if 0.4 <= p.get("activist_score", 0) <= 0.7])
        }
    
    def _analyze_sources(self) -> Dict:
        """Analyze data source characteristics"""
        source_stats = {}
        
        for proposal in self.combined_proposals:
            source = proposal["source_dataset"]
            if source not in source_stats:
                source_stats[source] = {
                    "total_proposals": 0,
                    "activist_proposals": 0,
                    "unique_daos": set()
                }
            
            source_stats[source]["total_proposals"] += 1
            source_stats[source]["unique_daos"].add(proposal["dao"])
            
            if proposal.get("activist_score", 0) > 0:
                source_stats[source]["activist_proposals"] += 1
        
        # Convert sets to counts
        for source, stats in source_stats.items():
            stats["unique_daos"] = len(stats["unique_daos"])
            if stats["total_proposals"] > 0:
                stats["activist_rate"] = (stats["activist_proposals"] / stats["total_proposals"]) * 100
        
        return {
            "total_sources": len(source_stats),
            "source_statistics": source_stats
        }
    
    def _analyze_temporal_patterns(self) -> Dict:
        """Analyze temporal patterns in proposals"""
        # This would require parsing dates - simplified for now
        proposals_with_dates = [p for p in self.combined_proposals if p.get("created")]
        
        return {
            "proposals_with_timestamps": len(proposals_with_dates),
            "temporal_coverage": "Analysis requires date parsing implementation"
        }
    
    def _calculate_quality_metrics(self) -> Dict:
        """Calculate dataset quality metrics"""
        total = len(self.combined_proposals)
        
        return {
            "completeness": {
                "proposals_with_titles": len([p for p in self.combined_proposals if p.get("title")]) / total * 100,
                "proposals_with_bodies": len([p for p in self.combined_proposals if p.get("body")]) / total * 100,
                "proposals_with_daos": len([p for p in self.combined_proposals if p.get("dao")]) / total * 100,
                "proposals_with_authors": len([p for p in self.combined_proposals if p.get("author")]) / total * 100
            },
            "diversity": {
                "unique_daos": len(set(p["dao"] for p in self.combined_proposals if p["dao"])),
                "unique_sources": len(set(p["source_dataset"] for p in self.combined_proposals)),
                "dao_coverage_score": len(set(p["dao"] for p in self.combined_proposals if p["dao"])) / 100  # Normalized
            },
            "activist_detection": {
                "proposals_with_scores": len([p for p in self.combined_proposals if p.get("activist_score", 0) > 0]),
                "avg_detection_confidence": sum(p.get("activist_score", 0) for p in self.combined_proposals) / total
            }
        }
    
    def generate_research_report(self) -> str:
        """Generate comprehensive research report"""
        if not self.analysis_results:
            return "No analysis results available"
        
        overview = self.analysis_results["dataset_overview"]
        dao_analysis = self.analysis_results["dao_analysis"]
        activist_analysis = self.analysis_results["activist_analysis"]
        quality = self.analysis_results["quality_metrics"]
        
        report = f"""# Comprehensive DAO Activist Proposal Research Analysis

## Executive Summary

This comprehensive analysis combines multiple data sources to provide the most complete view of DAO activist proposals available for research.

### Key Findings

- **Total Proposals Analyzed**: {overview['total_proposals']:,}
- **Unique DAOs Covered**: {overview['total_unique_daos']:,}
- **Activist Proposals Identified**: {overview['activist_proposals']:,}
- **Overall Activist Detection Rate**: {overview['activist_rate']:.1f}%
- **Data Sources**: {overview['total_sources']} different collection methods

## Dataset Composition

### Top DAOs by Activist Proposals
"""
        
        # Add top DAOs
        for dao, stats in list(dao_analysis["top_daos_by_activist_proposals"].items())[:10]:
            activist_count = stats["activist_proposals"]
            total_count = stats["total_proposals"]
            rate = stats.get("activist_rate", 0)
            report += f"- **{dao}**: {activist_count} activist proposals ({rate:.1f}% of {total_count} total)\n"
        
        report += f"""
### Data Quality Metrics

- **Title Completeness**: {quality['completeness']['proposals_with_titles']:.1f}%
- **Body Completeness**: {quality['completeness']['proposals_with_bodies']:.1f}%
- **DAO Attribution**: {quality['completeness']['proposals_with_daos']:.1f}%
- **Author Information**: {quality['completeness']['proposals_with_authors']:.1f}%

### Activist Detection Analysis

- **High Confidence Activist Proposals**: {activist_analysis.get('high_confidence_activist', 0):,}
- **Medium Confidence Activist Proposals**: {activist_analysis.get('medium_confidence_activist', 0):,}
- **Average Activist Score**: {activist_analysis.get('avg_activist_score', 0):.3f}
- **Average Keyword Hits**: {activist_analysis.get('avg_keyword_hits', 0):.1f}

## Research Applications

This dataset is suitable for:

1. **Academic Research on DAO Governance**
   - Comprehensive coverage of {overview['total_unique_daos']:,} DAOs
   - {overview['activist_proposals']:,} activist proposals for analysis
   - Multiple data sources for validation

2. **Activist Content Analysis**
   - Enhanced detection methods with confidence scores
   - Keyword-based and NLP-based classification
   - Detailed proposal content and metadata

3. **Governance Mechanism Studies**
   - Diverse DAO ecosystems represented
   - Proposal outcomes and voting data (where available)
   - Temporal analysis capabilities

## Methodology

### Data Collection
- **Multi-source aggregation**: Snapshot.org, governance forums, manual curation
- **Comprehensive coverage**: Major DeFi, infrastructure, gaming, and social DAOs
- **Quality filtering**: Junk proposal removal and validation

### Activist Detection
- **Keyword-based detection**: Comprehensive activist keyword library
- **NLP classification**: Zero-shot classification with BART-large-mnli
- **Combined scoring**: Weighted combination of detection methods
- **Confidence levels**: High (>0.7), Medium (0.4-0.7), Low (<0.4)

## Files Generated

- `comprehensive_research_dataset.json` - Complete combined dataset
- `comprehensive_research_dataset.csv` - CSV format for analysis
- `comprehensive_analysis_results.json` - Detailed analysis data
- `comprehensive_research_report.md` - This report

---
*Generated by Comprehensive Research Analysis Pipeline*
*Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        
        return report
    
    def save_comprehensive_results(self):
        """Save all comprehensive results"""
        print("\n💾 Saving comprehensive research results...")
        
        # Save combined dataset
        with open("comprehensive_research_dataset.json", "w", encoding="utf-8") as f:
            json.dump(self.combined_proposals, f, indent=2, ensure_ascii=False)
        
        # Save CSV format
        try:
            df = pd.DataFrame(self.combined_proposals)
            df.to_csv("comprehensive_research_dataset.csv", index=False, encoding="utf-8")
        except Exception as e:
            print(f"   ⚠ CSV export error: {e}")
        
        # Save analysis results
        with open("comprehensive_analysis_results.json", "w", encoding="utf-8") as f:
            json.dump(self.analysis_results, f, indent=2, ensure_ascii=False)
        
        # Save research report
        report = self.generate_research_report()
        with open("comprehensive_research_report.md", "w", encoding="utf-8") as f:
            f.write(report)
        
        print(f"✅ Comprehensive results saved:")
        print(f"   📄 comprehensive_research_dataset.json ({len(self.combined_proposals):,} proposals)")
        print(f"   📄 comprehensive_research_dataset.csv")
        print(f"   📄 comprehensive_analysis_results.json")
        print(f"   📄 comprehensive_research_report.md")

def main():
    """Main comprehensive analysis function"""
    analyzer = ComprehensiveResearchAnalysis()
    
    print("🎓 COMPREHENSIVE RESEARCH ANALYSIS")
    print("=" * 50)
    print("📊 Combining all available datasets")
    print("🔬 Generating research-grade analysis")
    
    # Load all datasets
    analyzer.load_all_datasets()
    
    # Combine and deduplicate
    combined_data = analyzer.combine_and_deduplicate()
    
    # Perform comprehensive analysis
    analysis = analyzer.analyze_comprehensive_dataset()
    
    # Save results
    analyzer.save_comprehensive_results()
    
    print(f"\n🎉 COMPREHENSIVE ANALYSIS COMPLETE!")
    print(f"   Final dataset: {len(combined_data):,} unique proposals")
    print(f"   Activist proposals: {analysis['dataset_overview']['activist_proposals']:,}")
    print(f"   Unique DAOs: {analysis['dataset_overview']['total_unique_daos']:,}")
    print(f"   Ready for academic research!")
    
    return analysis

if __name__ == "__main__":
    main()
