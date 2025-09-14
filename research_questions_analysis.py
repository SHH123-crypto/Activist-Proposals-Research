# research_questions_analysis.py - Analysis for specific research questions
"""
Comprehensive analysis for research questions:
1. Do activist governance proposals in DAOs affect the price of the cryptocurrency associated with the DAO?
2. Does having a larger share of the DAO correlate to proposals going the top voter's way?

Statistical analysis, correlation studies, and research insights.
"""

import pandas as pd
import numpy as np
import json
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from datetime import datetime

class ResearchQuestionsAnalyzer:
    """Analyze specific research questions about activist proposals and voting power"""
    
    def __init__(self, dataset_file: str):
        self.dataset_file = dataset_file
        self.df = None
        self.analysis_results = {}
        
    def load_and_prepare_data(self):
        """Load and prepare data for analysis"""
        print("📊 Loading and preparing research dataset...")
        
        try:
            if self.dataset_file.endswith('.csv'):
                self.df = pd.read_csv(self.dataset_file)
            else:
                with open(self.dataset_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.df = pd.DataFrame(data)
            
            print(f"   Loaded {len(self.df)} proposals")
            
            # Data cleaning and preparation
            self.df = self._clean_and_prepare_data(self.df)
            
            print(f"   Prepared {len(self.df)} proposals for analysis")
            return True
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
    
    def _clean_and_prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare data for analysis"""
        
        # Convert numeric columns
        numeric_columns = [
            'activist_score', 'top_voter_percentage', 'proposer_percentage',
            'total_voting_power', 'top_10_concentration', 'voting_power_gini',
            'overall_price_impact_pct', 'total_voters'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Create binary outcome variable
        if 'outcome' in df.columns:
            df['proposal_passed'] = df['outcome'].apply(
                lambda x: 1 if str(x).lower() in ['passed', 'success', 'executed'] else 0
            )
        
        # Create voting power categories
        if 'top_voter_percentage' in df.columns:
            df['voting_power_category'] = pd.cut(
                df['top_voter_percentage'],
                bins=[0, 5, 15, 30, 100],
                labels=['Low (0-5%)', 'Medium (5-15%)', 'High (15-30%)', 'Very High (30%+)']
            )
        
        # Create activist strength categories
        if 'activist_score' in df.columns:
            df['activist_strength'] = pd.cut(
                df['activist_score'],
                bins=[0, 0.4, 0.7, 1.0],
                labels=['Low', 'Medium', 'High']
            )
        
        # Filter for valid data
        df = df.dropna(subset=['dao_space', 'title'])
        
        return df
    
    def analyze_research_question_1(self) -> Dict:
        """RQ1: Do activist governance proposals affect token prices?"""
        print("\n🔬 Research Question 1: Activist Proposals → Token Price Impact")
        print("-" * 60)
        
        if 'overall_price_impact_pct' not in self.df.columns:
            return {"error": "Price impact data not available"}
        
        # Filter for proposals with price data
        price_data = self.df[self.df['price_data_available'] == True].copy()
        
        if len(price_data) == 0:
            return {"error": "No proposals with price data available"}
        
        print(f"   Analyzing {len(price_data)} proposals with price data")
        
        # Basic statistics
        activist_proposals = price_data[price_data['activist_score'] > 0.4]
        non_activist_proposals = price_data[price_data['activist_score'] <= 0.4]
        
        results = {
            "sample_size": len(price_data),
            "activist_proposals": len(activist_proposals),
            "non_activist_proposals": len(non_activist_proposals),
            
            "price_impact_stats": {
                "all_proposals": {
                    "mean": price_data['overall_price_impact_pct'].mean(),
                    "median": price_data['overall_price_impact_pct'].median(),
                    "std": price_data['overall_price_impact_pct'].std()
                },
                "activist_proposals": {
                    "mean": activist_proposals['overall_price_impact_pct'].mean(),
                    "median": activist_proposals['overall_price_impact_pct'].median(),
                    "std": activist_proposals['overall_price_impact_pct'].std()
                },
                "non_activist_proposals": {
                    "mean": non_activist_proposals['overall_price_impact_pct'].mean(),
                    "median": non_activist_proposals['overall_price_impact_pct'].median(),
                    "std": non_activist_proposals['overall_price_impact_pct'].std()
                }
            }
        }
        
        # Statistical tests
        if len(activist_proposals) > 5 and len(non_activist_proposals) > 5:
            # T-test for difference in means
            t_stat, t_pvalue = stats.ttest_ind(
                activist_proposals['overall_price_impact_pct'].dropna(),
                non_activist_proposals['overall_price_impact_pct'].dropna()
            )
            
            # Mann-Whitney U test (non-parametric)
            u_stat, u_pvalue = stats.mannwhitneyu(
                activist_proposals['overall_price_impact_pct'].dropna(),
                non_activist_proposals['overall_price_impact_pct'].dropna(),
                alternative='two-sided'
            )
            
            results["statistical_tests"] = {
                "t_test": {"statistic": t_stat, "p_value": t_pvalue},
                "mann_whitney_u": {"statistic": u_stat, "p_value": u_pvalue}
            }
        
        # Correlation analysis
        correlation_vars = ['activist_score', 'top_voter_percentage', 'total_voters', 'voting_power_gini']
        correlations = {}
        
        for var in correlation_vars:
            if var in price_data.columns:
                corr, p_val = stats.pearsonr(
                    price_data[var].dropna(),
                    price_data.loc[price_data[var].notna(), 'overall_price_impact_pct']
                )
                correlations[var] = {"correlation": corr, "p_value": p_val}
        
        results["correlations"] = correlations
        
        # Effect size analysis
        if len(activist_proposals) > 0 and len(non_activist_proposals) > 0:
            # Cohen's d
            pooled_std = np.sqrt(
                ((len(activist_proposals) - 1) * activist_proposals['overall_price_impact_pct'].var() +
                 (len(non_activist_proposals) - 1) * non_activist_proposals['overall_price_impact_pct'].var()) /
                (len(activist_proposals) + len(non_activist_proposals) - 2)
            )
            
            cohens_d = (activist_proposals['overall_price_impact_pct'].mean() - 
                       non_activist_proposals['overall_price_impact_pct'].mean()) / pooled_std
            
            results["effect_size"] = {"cohens_d": cohens_d}
        
        print(f"   ✅ Analysis complete")
        return results
    
    def analyze_research_question_2(self) -> Dict:
        """RQ2: Does voting power correlate with proposal outcomes?"""
        print("\n🔬 Research Question 2: Voting Power → Proposal Outcomes")
        print("-" * 60)
        
        if 'top_voter_percentage' not in self.df.columns or 'proposal_passed' not in self.df.columns:
            return {"error": "Voting power or outcome data not available"}
        
        # Filter for proposals with voting data
        voting_data = self.df[
            (self.df['top_voter_percentage'].notna()) & 
            (self.df['proposal_passed'].notna())
        ].copy()
        
        if len(voting_data) == 0:
            return {"error": "No proposals with voting data available"}
        
        print(f"   Analyzing {len(voting_data)} proposals with voting data")
        
        results = {
            "sample_size": len(voting_data),
            "passed_proposals": voting_data['proposal_passed'].sum(),
            "failed_proposals": len(voting_data) - voting_data['proposal_passed'].sum()
        }
        
        # Voting power distribution analysis
        passed = voting_data[voting_data['proposal_passed'] == 1]
        failed = voting_data[voting_data['proposal_passed'] == 0]
        
        results["voting_power_stats"] = {
            "passed_proposals": {
                "mean_top_voter_power": passed['top_voter_percentage'].mean(),
                "median_top_voter_power": passed['top_voter_percentage'].median(),
                "mean_proposer_power": passed['proposer_percentage'].mean() if 'proposer_percentage' in passed.columns else None
            },
            "failed_proposals": {
                "mean_top_voter_power": failed['top_voter_percentage'].mean(),
                "median_top_voter_power": failed['top_voter_percentage'].median(),
                "mean_proposer_power": failed['proposer_percentage'].mean() if 'proposer_percentage' in failed.columns else None
            }
        }
        
        # Statistical tests for voting power differences
        if len(passed) > 5 and len(failed) > 5:
            t_stat, t_pvalue = stats.ttest_ind(
                passed['top_voter_percentage'].dropna(),
                failed['top_voter_percentage'].dropna()
            )
            
            results["voting_power_difference_test"] = {
                "t_statistic": t_stat,
                "p_value": t_pvalue
            }
        
        # Logistic regression analysis (simplified)
        try:
            from sklearn.linear_model import LogisticRegression
            from sklearn.metrics import classification_report, roc_auc_score
            
            # Prepare features
            features = ['top_voter_percentage']
            if 'proposer_percentage' in voting_data.columns:
                features.append('proposer_percentage')
            if 'voting_power_gini' in voting_data.columns:
                features.append('voting_power_gini')
            
            X = voting_data[features].dropna()
            y = voting_data.loc[X.index, 'proposal_passed']
            
            if len(X) > 10:
                model = LogisticRegression()
                model.fit(X, y)
                
                predictions = model.predict(X)
                probabilities = model.predict_proba(X)[:, 1]
                
                results["logistic_regression"] = {
                    "features": features,
                    "coefficients": dict(zip(features, model.coef_[0])),
                    "accuracy": (predictions == y).mean(),
                    "auc_score": roc_auc_score(y, probabilities)
                }
        except ImportError:
            results["logistic_regression"] = {"error": "sklearn not available"}
        
        # Alignment analysis
        if 'top_voter_aligned_with_outcome' in voting_data.columns:
            alignment_rate = voting_data['top_voter_aligned_with_outcome'].mean()
            results["top_voter_alignment_rate"] = alignment_rate
        
        # Voting power concentration analysis
        if 'top_10_concentration' in voting_data.columns:
            high_concentration = voting_data[voting_data['top_10_concentration'] > 50]
            low_concentration = voting_data[voting_data['top_10_concentration'] <= 50]
            
            if len(high_concentration) > 0 and len(low_concentration) > 0:
                results["concentration_analysis"] = {
                    "high_concentration_pass_rate": high_concentration['proposal_passed'].mean(),
                    "low_concentration_pass_rate": low_concentration['proposal_passed'].mean()
                }
        
        print(f"   ✅ Analysis complete")
        return results
    
    def generate_comprehensive_analysis(self) -> Dict:
        """Generate comprehensive analysis for both research questions"""
        print("🎓 COMPREHENSIVE RESEARCH ANALYSIS")
        print("=" * 60)
        
        if not self.load_and_prepare_data():
            return {"error": "Failed to load data"}
        
        # Analyze both research questions
        rq1_results = self.analyze_research_question_1()
        rq2_results = self.analyze_research_question_2()
        
        # Combined analysis
        combined_results = {
            "dataset_overview": {
                "total_proposals": len(self.df),
                "activist_proposals": len(self.df[self.df['activist_score'] > 0.4]) if 'activist_score' in self.df.columns else 0,
                "proposals_with_price_data": len(self.df[self.df.get('price_data_available', False) == True]),
                "proposals_with_voting_data": len(self.df[self.df['top_voter_percentage'].notna()]) if 'top_voter_percentage' in self.df.columns else 0
            },
            "research_question_1": rq1_results,
            "research_question_2": rq2_results,
            "analysis_date": datetime.now().isoformat()
        }
        
        self.analysis_results = combined_results
        return combined_results
    
    def save_analysis_results(self, filename: str = "research_analysis_results"):
        """Save analysis results"""
        print(f"\n💾 Saving analysis results...")
        
        # Save JSON results
        with open(f"{filename}.json", "w", encoding="utf-8") as f:
            json.dump(self.analysis_results, f, indent=2, ensure_ascii=False)
        
        # Generate summary report
        report = self.generate_analysis_report()
        with open(f"{filename}_report.md", "w", encoding="utf-8") as f:
            f.write(report)
        
        print(f"✅ Analysis results saved:")
        print(f"   📄 {filename}.json")
        print(f"   📄 {filename}_report.md")
    
    def generate_analysis_report(self) -> str:
        """Generate comprehensive analysis report"""
        if not self.analysis_results:
            return "No analysis results available"
        
        overview = self.analysis_results.get("dataset_overview", {})
        rq1 = self.analysis_results.get("research_question_1", {})
        rq2 = self.analysis_results.get("research_question_2", {})
        
        report = f"""# Research Analysis Report: DAO Activist Proposals

## Dataset Overview

- **Total Proposals**: {overview.get('total_proposals', 0):,}
- **Activist Proposals**: {overview.get('activist_proposals', 0):,}
- **Proposals with Price Data**: {overview.get('proposals_with_price_data', 0):,}
- **Proposals with Voting Data**: {overview.get('proposals_with_voting_data', 0):,}

## Research Question 1: Do activist governance proposals affect token prices?

### Key Findings

"""
        
        if "price_impact_stats" in rq1:
            stats = rq1["price_impact_stats"]
            report += f"""
- **Sample Size**: {rq1.get('sample_size', 0)} proposals with price data
- **Activist Proposals**: {rq1.get('activist_proposals', 0)}
- **Non-Activist Proposals**: {rq1.get('non_activist_proposals', 0)}

### Price Impact Statistics

**All Proposals**:
- Mean Impact: {stats['all_proposals']['mean']:.3f}%
- Median Impact: {stats['all_proposals']['median']:.3f}%

**Activist Proposals**:
- Mean Impact: {stats['activist_proposals']['mean']:.3f}%
- Median Impact: {stats['activist_proposals']['median']:.3f}%

**Non-Activist Proposals**:
- Mean Impact: {stats['non_activist_proposals']['mean']:.3f}%
- Median Impact: {stats['non_activist_proposals']['median']:.3f}%
"""
        
        if "statistical_tests" in rq1:
            tests = rq1["statistical_tests"]
            report += f"""
### Statistical Significance

- **T-test p-value**: {tests['t_test']['p_value']:.6f}
- **Mann-Whitney U p-value**: {tests['mann_whitney_u']['p_value']:.6f}
"""
        
        report += f"""
## Research Question 2: Does voting power correlate with proposal outcomes?

### Key Findings

"""
        
        if "voting_power_stats" in rq2:
            stats = rq2["voting_power_stats"]
            report += f"""
- **Sample Size**: {rq2.get('sample_size', 0)} proposals with voting data
- **Passed Proposals**: {rq2.get('passed_proposals', 0)}
- **Failed Proposals**: {rq2.get('failed_proposals', 0)}

### Voting Power Analysis

**Passed Proposals**:
- Mean Top Voter Power: {stats['passed_proposals']['mean_top_voter_power']:.2f}%
- Median Top Voter Power: {stats['passed_proposals']['median_top_voter_power']:.2f}%

**Failed Proposals**:
- Mean Top Voter Power: {stats['failed_proposals']['mean_top_voter_power']:.2f}%
- Median Top Voter Power: {stats['failed_proposals']['median_top_voter_power']:.2f}%
"""
        
        if "top_voter_alignment_rate" in rq2:
            report += f"""
### Alignment Analysis

- **Top Voter Alignment Rate**: {rq2['top_voter_alignment_rate']:.1%}
"""
        
        report += f"""
## Conclusions

Based on the analysis of {overview.get('total_proposals', 0):,} DAO proposals:

1. **Price Impact**: {'Significant' if rq1.get('statistical_tests', {}).get('t_test', {}).get('p_value', 1) < 0.05 else 'No significant'} difference in price impact between activist and non-activist proposals.

2. **Voting Power**: {'Strong' if rq2.get('top_voter_alignment_rate', 0) > 0.7 else 'Moderate' if rq2.get('top_voter_alignment_rate', 0) > 0.5 else 'Weak'} correlation between voting power and proposal outcomes.

---
*Analysis generated on {self.analysis_results.get('analysis_date', 'Unknown')}*
"""
        
        return report

def main():
    """Main analysis function"""
    # Try to analyze existing dataset
    dataset_files = [
        "comprehensive_research_dataset.csv",
        "enhanced_research_dataset.csv",
        "fast_major_daos_dataset.csv"
    ]
    
    for dataset_file in dataset_files:
        try:
            print(f"🔍 Attempting to analyze {dataset_file}...")
            analyzer = ResearchQuestionsAnalyzer(dataset_file)
            results = analyzer.generate_comprehensive_analysis()
            analyzer.save_analysis_results("research_analysis_results")
            
            print(f"\n🎉 RESEARCH ANALYSIS COMPLETE!")
            print(f"   Dataset: {dataset_file}")
            print(f"   Results saved to research_analysis_results.json")
            break
            
        except FileNotFoundError:
            print(f"   ⚠ {dataset_file} not found")
            continue
    else:
        print("❌ No suitable dataset found. Run data collection first.")

if __name__ == "__main__":
    main()
