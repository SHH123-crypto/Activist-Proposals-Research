# individual_proposal_analyzer.py - Analyze individual proposal price impact
"""
Analyze individual proposal price sheets for detailed insights:
- Load individual proposal price data
- Perform statistical analysis
- Generate visualizations
- Create detailed reports
- Compare proposals across DAOs
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os

class IndividualProposalAnalyzer:
    """Analyze individual proposal price impact data"""
    
    def __init__(self, price_data_dir: str = "proposal_price_data"):
        self.price_data_dir = price_data_dir
        self.analysis_output_dir = "individual_analysis_reports"
        
        # Create output directory
        Path(self.analysis_output_dir).mkdir(exist_ok=True)
        
        # Set up plotting style
        plt.style.use('default')
        
        print(f"🔬 Individual Proposal Analyzer initialized")
        print(f"   Price data directory: {price_data_dir}")
        print(f"   Analysis output: {self.analysis_output_dir}")
    
    def load_proposal_data(self, filename: str) -> pd.DataFrame:
        """Load individual proposal price data"""
        filepath = os.path.join(self.price_data_dir, filename)
        
        try:
            df = pd.read_csv(filepath)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception as e:
            print(f"❌ Error loading {filename}: {e}")
            return pd.DataFrame()
    
    def analyze_single_proposal(self, filename: str) -> Dict:
        """Perform comprehensive analysis of a single proposal"""
        print(f"\n🔍 Analyzing: {filename}")
        
        df = self.load_proposal_data(filename)
        if df.empty:
            return {"error": "No data loaded"}
        
        # Extract proposal info
        proposal_info = {
            "dao": df["dao"].iloc[0] if "dao" in df.columns else "",
            "proposal_id": df["proposal_id"].iloc[0] if "proposal_id" in df.columns else "",
            "title": df["proposal_title"].iloc[0] if "proposal_title" in df.columns else "",
            "activist_score": df["activist_score"].iloc[0] if "activist_score" in df.columns else 0,
            "top_voter_percentage": df["top_voter_percentage"].iloc[0] if "top_voter_percentage" in df.columns else 0
        }
        
        # Get proposal dates
        proposal_start = pd.to_datetime(df["proposal_start"].iloc[0]) if "proposal_start" in df.columns else None
        proposal_end = pd.to_datetime(df["proposal_end"].iloc[0]) if "proposal_end" in df.columns else None
        
        # Define analysis periods
        if proposal_start and proposal_end:
            pre_period = df[df["datetime"] < proposal_start]
            during_period = df[(df["datetime"] >= proposal_start) & (df["datetime"] <= proposal_end)]
            post_period = df[df["datetime"] > proposal_end]
        else:
            # Fallback: split data into thirds
            total_days = len(df)
            pre_period = df.iloc[:total_days//3]
            during_period = df.iloc[total_days//3:2*total_days//3]
            post_period = df.iloc[2*total_days//3:]
        
        # Calculate period statistics
        analysis = {
            "proposal_info": proposal_info,
            "data_summary": {
                "total_data_points": len(df),
                "date_range": {
                    "start": df["datetime"].min().isoformat(),
                    "end": df["datetime"].max().isoformat(),
                    "duration_days": (df["datetime"].max() - df["datetime"].min()).days
                }
            },
            "period_analysis": {
                "pre_proposal": self._calculate_period_stats(pre_period),
                "during_proposal": self._calculate_period_stats(during_period),
                "post_proposal": self._calculate_period_stats(post_period)
            },
            "price_impact_analysis": self._analyze_price_impact(pre_period, during_period, post_period),
            "volatility_analysis": self._analyze_volatility(df, proposal_start, proposal_end),
            "volume_analysis": self._analyze_volume(df, proposal_start, proposal_end)
        }
        
        return analysis
    
    def _calculate_period_stats(self, df: pd.DataFrame) -> Dict:
        """Calculate statistics for a time period"""
        if df.empty:
            return {"data_points": 0, "avg_price": 0, "price_change": 0, "volatility": 0}
        
        return {
            "data_points": len(df),
            "avg_price": round(df["price_usd"].mean(), 6),
            "start_price": round(df["price_usd"].iloc[0], 6),
            "end_price": round(df["price_usd"].iloc[-1], 6),
            "min_price": round(df["price_usd"].min(), 6),
            "max_price": round(df["price_usd"].max(), 6),
            "price_change_pct": round(((df["price_usd"].iloc[-1] - df["price_usd"].iloc[0]) / df["price_usd"].iloc[0] * 100), 3) if df["price_usd"].iloc[0] > 0 else 0,
            "volatility": round(df["price_usd"].std(), 6),
            "avg_volume": round(df["volume_usd"].mean(), 2) if "volume_usd" in df.columns else 0
        }
    
    def _analyze_price_impact(self, pre_df: pd.DataFrame, during_df: pd.DataFrame, post_df: pd.DataFrame) -> Dict:
        """Analyze price impact across periods"""
        impact_analysis = {}
        
        if not pre_df.empty and not post_df.empty:
            pre_avg = pre_df["price_usd"].mean()
            post_avg = post_df["price_usd"].mean()
            
            impact_analysis["pre_to_post_change_pct"] = round(((post_avg - pre_avg) / pre_avg * 100), 3) if pre_avg > 0 else 0
            impact_analysis["pre_avg_price"] = round(pre_avg, 6)
            impact_analysis["post_avg_price"] = round(post_avg, 6)
        
        if not pre_df.empty and not during_df.empty:
            pre_avg = pre_df["price_usd"].mean()
            during_avg = during_df["price_usd"].mean()
            
            impact_analysis["pre_to_during_change_pct"] = round(((during_avg - pre_avg) / pre_avg * 100), 3) if pre_avg > 0 else 0
        
        if not during_df.empty and not post_df.empty:
            during_avg = during_df["price_usd"].mean()
            post_avg = post_df["price_usd"].mean()
            
            impact_analysis["during_to_post_change_pct"] = round(((post_avg - during_avg) / during_avg * 100), 3) if during_avg > 0 else 0
        
        return impact_analysis
    
    def _analyze_volatility(self, df: pd.DataFrame, proposal_start, proposal_end) -> Dict:
        """Analyze price volatility patterns"""
        volatility_analysis = {}
        
        if "price_change_pct" in df.columns:
            volatility_analysis["overall_volatility"] = round(df["price_change_pct"].std(), 3)
            volatility_analysis["max_daily_gain"] = round(df["price_change_pct"].max(), 3)
            volatility_analysis["max_daily_loss"] = round(df["price_change_pct"].min(), 3)
            
            # Volatility around proposal dates
            if proposal_start and proposal_end:
                proposal_period = df[(df["datetime"] >= proposal_start) & (df["datetime"] <= proposal_end)]
                if not proposal_period.empty:
                    volatility_analysis["proposal_period_volatility"] = round(proposal_period["price_change_pct"].std(), 3)
        
        return volatility_analysis
    
    def _analyze_volume(self, df: pd.DataFrame, proposal_start, proposal_end) -> Dict:
        """Analyze trading volume patterns"""
        volume_analysis = {}
        
        if "volume_usd" in df.columns:
            volume_analysis["avg_volume"] = round(df["volume_usd"].mean(), 2)
            volume_analysis["max_volume"] = round(df["volume_usd"].max(), 2)
            volume_analysis["volume_volatility"] = round(df["volume_usd"].std(), 2)
            
            # Volume around proposal dates
            if proposal_start and proposal_end:
                proposal_period = df[(df["datetime"] >= proposal_start) & (df["datetime"] <= proposal_end)]
                if not proposal_period.empty:
                    volume_analysis["proposal_period_avg_volume"] = round(proposal_period["volume_usd"].mean(), 2)
        
        return volume_analysis
    
    def create_proposal_visualization(self, filename: str, analysis: Dict) -> str:
        """Create visualization for individual proposal"""
        df = self.load_proposal_data(filename)
        if df.empty:
            return ""
        
        proposal_info = analysis["proposal_info"]
        dao = proposal_info["dao"]
        title = proposal_info["title"][:50]
        
        # Create figure with subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle(f'{dao}: {title}', fontsize=14, fontweight='bold')
        
        # Plot 1: Price over time
        axes[0, 0].plot(df["datetime"], df["price_usd"], linewidth=1.5, color='blue')
        axes[0, 0].set_title('Price Over Time')
        axes[0, 0].set_ylabel('Price (USD)')
        axes[0, 0].tick_params(axis='x', rotation=45)
        
        # Add proposal period shading if available
        if "proposal_start" in df.columns and "proposal_end" in df.columns:
            proposal_start = pd.to_datetime(df["proposal_start"].iloc[0])
            proposal_end = pd.to_datetime(df["proposal_end"].iloc[0])
            axes[0, 0].axvspan(proposal_start, proposal_end, alpha=0.3, color='red', label='Proposal Period')
            axes[0, 0].legend()
        
        # Plot 2: Volume over time
        if "volume_usd" in df.columns:
            axes[0, 1].plot(df["datetime"], df["volume_usd"], linewidth=1.5, color='green')
            axes[0, 1].set_title('Trading Volume Over Time')
            axes[0, 1].set_ylabel('Volume (USD)')
            axes[0, 1].tick_params(axis='x', rotation=45)
        
        # Plot 3: Price change distribution
        if "price_change_pct" in df.columns:
            axes[1, 0].hist(df["price_change_pct"].dropna(), bins=30, alpha=0.7, color='orange')
            axes[1, 0].set_title('Daily Price Change Distribution')
            axes[1, 0].set_xlabel('Price Change (%)')
            axes[1, 0].set_ylabel('Frequency')
        
        # Plot 4: Price vs Volume scatter
        if "volume_usd" in df.columns:
            axes[1, 1].scatter(df["volume_usd"], df["price_usd"], alpha=0.6, color='purple')
            axes[1, 1].set_title('Price vs Volume')
            axes[1, 1].set_xlabel('Volume (USD)')
            axes[1, 1].set_ylabel('Price (USD)')
        
        plt.tight_layout()
        
        # Save plot
        plot_filename = f"{dao}_{proposal_info['proposal_id']}_analysis.png"
        plot_path = os.path.join(self.analysis_output_dir, plot_filename)
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"   📊 Visualization saved: {plot_filename}")
        return plot_path
    
    def generate_proposal_report(self, filename: str, analysis: Dict) -> str:
        """Generate detailed report for individual proposal"""
        proposal_info = analysis["proposal_info"]
        dao = proposal_info["dao"]
        
        report = f"""# Individual Proposal Analysis Report

## Proposal Information
- **DAO**: {proposal_info['dao']}
- **Proposal ID**: {proposal_info['proposal_id']}
- **Title**: {proposal_info['title']}
- **Activist Score**: {proposal_info['activist_score']:.3f}
- **Top Voter Power**: {proposal_info['top_voter_percentage']:.1f}%

## Data Summary
- **Total Data Points**: {analysis['data_summary']['total_data_points']:,}
- **Date Range**: {analysis['data_summary']['date_range']['start'][:10]} to {analysis['data_summary']['date_range']['end'][:10]}
- **Duration**: {analysis['data_summary']['date_range']['duration_days']} days

## Period Analysis

### Pre-Proposal Period
- **Data Points**: {analysis['period_analysis']['pre_proposal']['data_points']}
- **Average Price**: ${analysis['period_analysis']['pre_proposal']['avg_price']:.6f}
- **Price Change**: {analysis['period_analysis']['pre_proposal']['price_change_pct']:.2f}%
- **Volatility**: ${analysis['period_analysis']['pre_proposal']['volatility']:.6f}

### During Proposal Period
- **Data Points**: {analysis['period_analysis']['during_proposal']['data_points']}
- **Average Price**: ${analysis['period_analysis']['during_proposal']['avg_price']:.6f}
- **Price Change**: {analysis['period_analysis']['during_proposal']['price_change_pct']:.2f}%
- **Volatility**: ${analysis['period_analysis']['during_proposal']['volatility']:.6f}

### Post-Proposal Period
- **Data Points**: {analysis['period_analysis']['post_proposal']['data_points']}
- **Average Price**: ${analysis['period_analysis']['post_proposal']['avg_price']:.6f}
- **Price Change**: {analysis['period_analysis']['post_proposal']['price_change_pct']:.2f}%
- **Volatility**: ${analysis['period_analysis']['post_proposal']['volatility']:.6f}

## Price Impact Analysis
"""
        
        if "pre_to_post_change_pct" in analysis["price_impact_analysis"]:
            report += f"""
- **Overall Impact (Pre to Post)**: {analysis['price_impact_analysis']['pre_to_post_change_pct']:.2f}%
- **Pre-Proposal Average**: ${analysis['price_impact_analysis']['pre_avg_price']:.6f}
- **Post-Proposal Average**: ${analysis['price_impact_analysis']['post_avg_price']:.6f}
"""
        
        report += f"""
## Volatility Analysis
- **Overall Volatility**: {analysis['volatility_analysis'].get('overall_volatility', 'N/A')}%
- **Max Daily Gain**: {analysis['volatility_analysis'].get('max_daily_gain', 'N/A')}%
- **Max Daily Loss**: {analysis['volatility_analysis'].get('max_daily_loss', 'N/A')}%

## Volume Analysis
- **Average Volume**: ${analysis['volume_analysis'].get('avg_volume', 0):,.2f}
- **Max Volume**: ${analysis['volume_analysis'].get('max_volume', 0):,.2f}

---
*Analysis generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        
        # Save report
        report_filename = f"{dao}_{proposal_info['proposal_id']}_report.md"
        report_path = os.path.join(self.analysis_output_dir, report_filename)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"   📄 Report saved: {report_filename}")
        return report_path
    
    def analyze_all_proposals(self) -> Dict:
        """Analyze all proposals in the price data directory"""
        print(f"🔬 ANALYZING ALL INDIVIDUAL PROPOSALS")
        print("=" * 50)
        
        # Find all CSV files
        csv_files = list(Path(self.price_data_dir).glob("*.csv"))
        csv_files = [f for f in csv_files if f.name not in ["master_price_analysis.csv", "scraping_summary.json"]]
        
        if not csv_files:
            print(f"❌ No proposal price files found in {self.price_data_dir}")
            return {}
        
        print(f"   Found {len(csv_files)} proposal files")
        
        all_analyses = {}
        successful_analyses = 0
        
        for csv_file in csv_files:
            try:
                # Analyze individual proposal
                analysis = self.analyze_single_proposal(csv_file.name)
                
                if "error" not in analysis:
                    # Create visualization
                    self.create_proposal_visualization(csv_file.name, analysis)
                    
                    # Generate report
                    self.generate_proposal_report(csv_file.name, analysis)
                    
                    all_analyses[csv_file.name] = analysis
                    successful_analyses += 1
                
            except Exception as e:
                print(f"   ❌ Error analyzing {csv_file.name}: {e}")
        
        print(f"\n✅ Individual analysis complete!")
        print(f"   Analyzed: {successful_analyses}/{len(csv_files)} proposals")
        print(f"   Reports saved to: {self.analysis_output_dir}")
        
        # Save combined analysis
        with open(f"{self.analysis_output_dir}/all_analyses.json", "w") as f:
            json.dump(all_analyses, f, indent=2)
        
        return all_analyses

def main():
    """Main individual analysis function"""
    analyzer = IndividualProposalAnalyzer()
    
    # Check if price data directory exists
    if not Path(analyzer.price_data_dir).exists():
        print(f"❌ Price data directory not found: {analyzer.price_data_dir}")
        print("   Run detailed_price_scraper.py first to generate price data")
        return
    
    # Analyze all proposals
    analyses = analyzer.analyze_all_proposals()
    
    if analyses:
        print(f"\n🎉 INDIVIDUAL ANALYSIS COMPLETE!")
        print(f"   📊 {len(analyses)} proposals analyzed")
        print(f"   📁 Check folder: {analyzer.analysis_output_dir}")
        print(f"   📄 Individual reports and visualizations created")
    else:
        print(f"❌ No proposals could be analyzed")

if __name__ == "__main__":
    main()
