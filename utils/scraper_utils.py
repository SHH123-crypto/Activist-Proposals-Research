# scraper_utils.py
from crawl4ai import AsyncWebCrawler
import re

def extract_proposals_from_content(content, dao_url):
    """
    Manual extraction of proposals from markdown content as fallback.
    """
    proposals = []

    # Simple regex patterns to find proposal-like content
    # This is a basic implementation - you may need to adjust based on actual content structure

    # Look for headings that might be proposal titles
    title_pattern = r'^#{1,3}\s+(.+)$'
    titles = re.findall(title_pattern, content, re.MULTILINE)

    # Look for links that might be proposal links
    link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
    links = re.findall(link_pattern, content)

    # Create basic proposal objects
    for i, title in enumerate(titles[:10]):  # Limit to first 10 titles
        proposal = {
            "id": f"manual_{i}_{hash(title) % 10000}",
            "title": title.strip(),
            "description": "",
            "link": dao_url,
            "state": "unknown",
            "createdAt": "",
            "proposer": "",
            "dao": dao_url
        }

        # Try to find a corresponding link
        for link_text, link_url in links:
            if title.lower() in link_text.lower() or link_text.lower() in title.lower():
                proposal["link"] = link_url if link_url.startswith('http') else dao_url + link_url
                break

        proposals.append(proposal)

    return proposals

async def fetch_all_proposals(crawler, dao_url, session_id, seen_ids, proposal_selector, max_clicks=100):
    """
    Fetch all proposals for a DAO using modern crawl4ai API.
    Note: session_id parameter is kept for compatibility but not used in current implementation.
    """
    from crawl4ai import CrawlerRunConfig, JsonCssExtractionStrategy

    # Define schema for proposal extraction
    proposal_schema = {
        "name": "DAO Proposals",
        "baseSelector": proposal_selector,
        "fields": [
            {
                "name": "id",
                "selector": "[data-testid='proposal-id'], .proposal-id, [id*='proposal']",
                "type": "attribute",
                "attribute": "data-id",
                "default": ""
            },
            {
                "name": "title",
                "selector": "h1, h2, h3, .title, .proposal-title",
                "type": "text",
                "default": ""
            },
            {
                "name": "description",
                "selector": ".description, .proposal-description, .content, p",
                "type": "text",
                "default": ""
            },
            {
                "name": "link",
                "selector": "a",
                "type": "attribute",
                "attribute": "href",
                "default": ""
            },
            {
                "name": "state",
                "selector": ".status, .state, .proposal-state",
                "type": "text",
                "default": "unknown"
            },
            {
                "name": "createdAt",
                "selector": ".date, .created, .timestamp",
                "type": "text",
                "default": ""
            },
            {
                "name": "proposer",
                "selector": ".proposer, .author, .creator",
                "type": "text",
                "default": ""
            }
        ]
    }

    extraction_strategy = JsonCssExtractionStrategy(proposal_schema, verbose=False)  # Disable verbose for speed

    # Try different approaches based on the URL
    if "tally.xyz" in dao_url.lower():
        # Optimized handling for Tally.xyz - much faster
        print(f"Using optimized Tally.xyz configuration for {dao_url}")
        run_config = CrawlerRunConfig(
            extraction_strategy=extraction_strategy,
            # Reduced timeouts for faster execution
            page_timeout=10000,  # 10 seconds (reduced from 20)
            delay_before_return_html=3000,  # 3 seconds (reduced from 8)
            # Minimal JS for speed
            js_code=[
                """
                // Quick scroll to trigger any lazy loading
                window.scrollTo(0, document.body.scrollHeight);
                await new Promise(r => setTimeout(r, 1000));  // Reduced from 2000ms
                """
            ]
        )
    else:
        # Standard configuration for other sites
        run_config = CrawlerRunConfig(
            extraction_strategy=extraction_strategy,
            js_code=[
                # Click "View All" if present
                """
                (async () => {
                    try {
                        // Try multiple selectors for "View All" button
                        const selectors = [
                            'button[aria-label*="View All"]',
                            'button:contains("View All")',
                            'a:contains("View All")',
                            '[data-testid*="view-all"]',
                            '.view-all-button'
                        ];

                        for (const selector of selectors) {
                            const btn = document.querySelector(selector);
                            if (btn && btn.offsetParent !== null) {
                                btn.click();
                                await new Promise(r => setTimeout(r, 2000));
                                break;
                            }
                        }
                    } catch (e) {
                        console.log('View All button not found or error:', e);
                    }
                })();
                """,
                # Load more content by clicking load more buttons
                f"""
                (async () => {{
                    try {{
                        let clicks = 0;
                        const maxClicks = {max_clicks};

                        while (clicks < maxClicks) {{
                            // Try multiple selectors for "Load More" button
                            const selectors = [
                                'button[aria-label*="Load More"]',
                                'button:contains("Load More")',
                                'a:contains("Load More")',
                                '[data-testid*="load-more"]',
                                '.load-more-button',
                                'button[data-testid="load-more"]'
                            ];

                            let found = false;
                            for (const selector of selectors) {{
                                const btn = document.querySelector(selector);
                                if (btn && btn.offsetParent !== null) {{
                                    btn.click();
                                    await new Promise(r => setTimeout(r, 1500));
                                    clicks++;
                                    found = true;
                                    break;
                                }}
                            }}

                            if (!found) break;
                        }}
                    }} catch (e) {{
                        console.log('Load More error:', e);
                    }}
                }})();
                """
            ],
            wait_for="domcontentloaded",
            wait_for_timeout=30000,
            page_timeout=45000,
            delay_before_return_html=3000
        )

    try:
        print(f"Attempting to crawl {dao_url}...")
        result = await crawler.arun(url=dao_url, config=run_config)

        if result.success and result.extracted_content:
            import json
            try:
                proposals = json.loads(result.extracted_content)
                print(f"Successfully extracted {len(proposals)} proposals from {dao_url}")

                # Filter out duplicates and add to seen_ids
                unique_proposals = []
                for proposal in proposals:
                    proposal_id = proposal.get("id") or proposal.get("title", "")
                    if proposal_id and proposal_id not in seen_ids:
                        seen_ids.add(proposal_id)
                        # Ensure all required fields exist
                        proposal.setdefault("dao", dao_url)
                        unique_proposals.append(proposal)

                print(f"After deduplication: {len(unique_proposals)} unique proposals")
                return unique_proposals

            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON from {dao_url}: {str(e)}")
                print(f"Raw content preview: {result.extracted_content[:200]}...")
                return []
        else:
            error_msg = getattr(result, 'error_message', 'Unknown error')
            print(f"Failed to extract proposals from {dao_url}: {error_msg}")

            # Try a fallback approach with minimal configuration
            print(f"Trying fallback approach for {dao_url}...")
            fallback_config = CrawlerRunConfig(
                # No extraction strategy, just get raw content
                page_timeout=15000,
                delay_before_return_html=5000
            )

            try:
                fallback_result = await crawler.arun(url=dao_url, config=fallback_config)
                if fallback_result.success:
                    print(f"Fallback successful: got content from {dao_url}")
                    print(f"Content length: {len(fallback_result.markdown)}")

                    # Try to extract proposals manually from the markdown/html
                    proposals = extract_proposals_from_content(fallback_result.markdown, dao_url)
                    print(f"Manual extraction found {len(proposals)} proposals")

                    unique_proposals = []
                    for proposal in proposals:
                        proposal_id = proposal.get("id") or proposal.get("title", "")
                        if proposal_id and proposal_id not in seen_ids:
                            seen_ids.add(proposal_id)
                            proposal.setdefault("dao", dao_url)
                            unique_proposals.append(proposal)

                    return unique_proposals
                else:
                    print(f"Fallback also failed for {dao_url}")
                    return []
            except Exception as fallback_e:
                print(f"Fallback error for {dao_url}: {str(fallback_e)}")
                return []

    except Exception as e:
        print(f"Error fetching proposals from {dao_url}: {str(e)}")
        # If it's a timeout error, suggest the issue
        if "timeout" in str(e).lower() or "networkidle" in str(e).lower():
            print(f"Timeout error detected. The site {dao_url} may have continuous network activity.")
            print("Consider using a different wait condition or shorter timeout.")
        return []
