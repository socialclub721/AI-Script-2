#!/usr/bin/env python3
import os
import json
import time
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from supabase import create_client, Client
from openai import OpenAI
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AICryptoNewsProcessor:
    def __init__(self):
        """Initialize AI Crypto News Processor with environment variables"""
        # API Keys
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        # Validate environment variables
        if not all([self.openai_api_key, supabase_url, supabase_key]):
            raise ValueError("Missing required environment variables")
        
        # Initialize clients
        self.supabase = create_client(supabase_url, supabase_key)
        self.client = OpenAI(api_key=self.openai_api_key)
        
        # Table names - crypto_rss_news and crypto_news_clean
        self.raw_table = 'crypto_rss_news'
        self.processed_table = 'crypto_news_clean'
        
        logger.info(f"Initialized with source table: {self.raw_table}")
        logger.info(f"Initialized with destination table: {self.processed_table}")
        
        # Updated crypto-focused evaluation prompt - MUCH LESS RESTRICTIVE
        self.evaluation_prompt = """
You are a crypto and blockchain news filter. Your job is to identify news that could be relevant to crypto investors, traders, blockchain professionals, or anyone following digital assets and DeFi.

PASS news that covers:

**Cryptocurrency & Tokens:**
- Any crypto price movements (>2% for major coins, >5% for altcoins)
- New token launches, airdrops, or listings
- Market cap milestones or ranking changes
- Trading volume spikes or unusual activity
- Whale movements or large transactions

**Blockchain & Technology:**
- Protocol updates, hard forks, network upgrades
- Smart contract developments
- Layer 2 solutions and scaling news
- Cross-chain bridges and interoperability
- Consensus mechanism changes

**DeFi & Applications:**
- DEX volumes, new protocols, yield farming
- Lending/borrowing platform updates
- NFT marketplace developments
- Gaming and metaverse projects
- Web3 infrastructure news

**Institutional & Adoption:**
- Corporate crypto adoption (any size company)
- Investment fund movements or announcements
- Bank partnerships with crypto companies
- Government digital currency developments
- Traditional finance entering crypto

**Regulatory & Legal:**
- Crypto regulations worldwide
- Court decisions affecting crypto
- Government policy on digital assets
- Tax implications and guidance
- Compliance developments

**Exchanges & Platforms:**
- Exchange listings, delistings, updates
- Custody solutions and security news
- Trading platform developments
- Wallet and infrastructure updates
- Security incidents or improvements

**Market Infrastructure:**
- ETF developments and approvals
- Derivatives and futures markets
- Stablecoin news and developments
- Mining industry updates
- Energy consumption discussions

**Innovation & Trends:**
- AI integration with blockchain
- Environmental crypto initiatives
- Social tokens and creator economy
- Decentralized identity solutions
- New consensus mechanisms

BLOCK only clearly irrelevant content:
- Pure traditional finance (unless crypto connection)
- General technology news (unless blockchain related)
- Entertainment without crypto/NFT angle
- Sports (unless crypto sponsorship/payments)
- Weather or unrelated current events

When in doubt about crypto relevance, PASS the news. It's better to include potentially relevant crypto content than to miss important developments.

Analyze this news and respond with ONLY "PASS" or "BLOCK" followed by a brief reason.

NEWS:
Headline: {headline}
Summary: {summary}
Source: {source}
"""

        self.processing_prompt = """
You are a crypto news processor that creates Watcher.guru style headlines and summaries. Analyze this article and create crypto-focused content.

ORIGINAL ARTICLE:
Headline: {headline}
Summary: {summary}
Source: {source}

WATCHER.GURU STYLE RULES:
1. Start with PERSON/ORGANIZATION NAME then what they did/what happened
2. Use NO commas or periods in headlines
3. Add country flag emojis 🇺🇸 at START for government officials only (Presidents Treasury Secretaries Fed Chairs etc) - NOT for companies
4. If multiple presidents/gov officials involved add multiple flags
5. Use specific dollar amounts with $ and commas ($5900000000 or $5.9 billion)
6. Include crypto tickers with $ prefix ($BTC $ETH $DOGE)
7. Keep urgent breaking news tone
8. Use "says" for quotes and "reaches" "surpasses" "files" for actions
9. Be specific with numbers and percentages
10. Never use prefixes like "JUST IN" or "BREAKING"

CRYPTO EXAMPLES:
- "🇺🇸 Treasury Secretary Bessent says US government exploring ways to acquire more Bitcoin to expand reserve"
- "Michael Saylor says volatility is a gift to the faithful"  
- "Grayscale files S-1 for Dogecoin $DOGE ETF"
- "$2.57 trillion asset manager Citigroup looks to add crypto custody services"
- "Bitcoin surpasses Google to become 5th largest asset by market cap"
- "$420000000 liquidated from crypto market in past 20 minutes"
- "Binance CEO says $BNB ready for institutional adoption surge"
- "Ethereum reaches new ATH as $ETH breaks $4500 resistance"

Create the following:

1. SHORT_HEADLINE: Watcher.guru style headline starting with person/org name (max 120 characters no commas no periods)
2. SHORT_SUMMARY: Key crypto impact in same style (max 180 characters no commas no periods)  
3. TICKERS: List relevant crypto tickers (BTC ETH SOL etc). If none specific write ["CRYPTO"]
4. SENTIMENT: Choose ONLY ONE: "BULLISH" or "BEARISH" or "NEUTRAL"
5. MARKET_IMPACT: Explain crypto market implications in Watcher.guru urgent tone (max 200 characters no commas no periods)

Format as JSON:
{{
    "short_headline": "...",
    "short_summary": "...",
    "tickers": ["BTC", "ETH"],
    "sentiment": "BULLISH", 
    "market_impact": "..."
}}
"""

    def fetch_latest_news(self) -> List[Dict]:
        """Fetch latest 20 articles from crypto_rss_news, regardless of processed status"""
        try:
            logger.info(f"Fetching latest 20 crypto articles from {self.raw_table}")
            
            # Get latest 20 articles ordered by ingested_at (most recent first)
            result = self.supabase.table(self.raw_table)\
                .select('*')\
                .order('ingested_at', desc=True)\
                .limit(20)\
                .execute()
            
            if result.data:
                logger.info(f"Fetched {len(result.data)} latest crypto articles from {self.raw_table}")
                return result.data
            else:
                logger.info("No crypto articles found")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching from {self.raw_table}: {e}")
            return []

    def is_already_processed(self, news_item: Dict) -> bool:
        """Check if this crypto article is already processed by checking the clean table"""
        try:
            original_id = str(news_item.get('id'))
            if not original_id:
                return False
                
            result = self.supabase.table(self.processed_table)\
                .select('original_id')\
                .eq('original_id', original_id)\
                .execute()
            
            return len(result.data) > 0
            
        except Exception as e:
            logger.error(f"Error checking if crypto news processed: {e}")
            return False

    def maintain_table_size_limit(self):
        """Keep only 100 articles in crypto_news_clean, remove oldest when limit exceeded"""
        try:
            # Count current articles
            count_result = self.supabase.table(self.processed_table)\
                .select('id', count='exact')\
                .execute()
            
            current_count = count_result.count
            logger.info(f"Current crypto articles in {self.processed_table}: {current_count}")
            
            if current_count >= 100:
                # Get oldest articles to delete (keep newest 99, so next insert makes it 100)
                articles_to_delete = current_count - 99
                
                oldest_articles = self.supabase.table(self.processed_table)\
                    .select('id')\
                    .order('processed_at', desc=False)\
                    .limit(articles_to_delete)\
                    .execute()
                
                if oldest_articles.data:
                    # Delete the oldest articles
                    ids_to_delete = [article['id'] for article in oldest_articles.data]
                    
                    for article_id in ids_to_delete:
                        self.supabase.table(self.processed_table)\
                            .delete()\
                            .eq('id', article_id)\
                            .execute()
                    
                    logger.info(f"🧹 Removed {len(ids_to_delete)} oldest crypto articles to maintain 100 article limit")
                
        except Exception as e:
            logger.error(f"Error maintaining crypto table size limit: {e}")

    def evaluate_news_importance(self, news_item: Dict) -> Tuple[bool, str]:
        """Evaluate if crypto news is market-moving using AI"""
        try:
            prompt = self.evaluation_prompt.format(
                headline=news_item.get('title', ''),  # Changed from 'headline' to 'title'
                summary=news_item.get('description', ''),   # Use description field
                source=news_item.get('author', '')    # Changed from 'source' to 'author'
            )
            
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a crypto and blockchain news evaluator. When in doubt, PASS the news. Respond with PASS or BLOCK and a brief reason."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=100
            )
            
            result = response.choices[0].message.content.strip()
            
            # Parse response
            if result.startswith("PASS"):
                logger.info(f"✅ PASSED: {news_item.get('title', '')[:50]}...")
                return True, result
            else:
                logger.info(f"❌ BLOCKED: {news_item.get('title', '')[:50]}...")
                return False, result
                
        except Exception as e:
            logger.error(f"Error evaluating crypto news: {e}")
            return False, f"Error: {str(e)}"

    def process_news_content(self, news_item: Dict) -> Optional[Dict]:
        """Process passed crypto news to extract structured information"""
        try:
            prompt = self.processing_prompt.format(
                headline=news_item.get('title', ''),  # Changed from 'headline' to 'title'
                summary=news_item.get('description', ''),   # Use description field
                source=news_item.get('author', '')    # Changed from 'source' to 'author'
            )
            
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a crypto news processor. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=300,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # Validate required fields
            required = ['short_headline', 'short_summary', 'tickers', 'sentiment', 'market_impact']
            if all(field in result for field in required):
                return result
            else:
                logger.error(f"Missing required fields in AI response")
                return None
                
        except Exception as e:
            logger.error(f"Error processing crypto news content: {e}")
            return None

    def store_processed_news(self, processed_data: Dict) -> bool:
        """Store processed crypto news in the clean Supabase table with size management"""
        try:
            # First maintain the 100 article limit
            self.maintain_table_size_limit()
            
            # Then insert the new article
            result = self.supabase.table(self.processed_table).insert(processed_data).execute()
            logger.info(f"✅ Stored processed crypto news: {processed_data['short_headline']}")
            return True
        except Exception as e:
            logger.error(f"Error storing processed crypto news: {e}")
            return False

    def mark_as_processed(self, news_item: Dict) -> bool:
        """Mark original news item as processed in crypto_rss_news table"""
        try:
            # Use id for crypto_rss_news table
            id_value = news_item.get('id')
            
            if not id_value:
                logger.warning(f"No id found for crypto news item")
                return False
            
            result = self.supabase.table(self.raw_table)\
                .update({'processed': True})\
                .eq('id', id_value)\
                .execute()
            
            logger.debug(f"Marked crypto news as processed: {id_value}")
            return True
        except Exception as e:
            logger.error(f"Error marking crypto news as processed: {e}")
            return False

    def process_single_news(self, news_item: Dict) -> bool:
        """Process a single crypto news item through the entire pipeline"""
        try:
            # Step 1: Check if already processed (skip duplicates)
            if self.is_already_processed(news_item):
                logger.info(f"⏭️  Already processed: {news_item.get('title', '')[:50]}...")
                return False
            
            # Step 2: Evaluate importance
            is_important, reason = self.evaluate_news_importance(news_item)
            
            if not is_important:
                # Mark as processed even if blocked (to avoid re-checking)
                self.mark_as_processed(news_item)
                return False
            
            # Step 3: Process content with AI
            processed = self.process_news_content(news_item)
            if not processed:
                return False
            
            # Step 4: Prepare data for storage (updated field mappings)
            final_data = {
                'original_id': str(news_item.get('id')),  # Use id from crypto_rss_news
                'original_headline': news_item.get('title', ''),  # Changed from headline to title
                'original_url': news_item.get('link', ''),  # Changed from url to link
                'short_headline': processed['short_headline'][:120],  # Enforce limits
                'short_summary': processed['short_summary'][:180],
                'tickers': processed['tickers'] if isinstance(processed['tickers'], list) else [],
                'sentiment': processed['sentiment'],
                'market_impact': processed['market_impact'],
                'original_datetime': None,  # RSS doesn't have unix timestamp, could use pubdate_parsed if needed
                'evaluation_reason': reason,
                'processed_at': datetime.now().isoformat()
            }
            
            # Step 5: Store in processed table (with automatic size management)
            if self.store_processed_news(final_data):
                # Step 6: Mark original as processed
                self.mark_as_processed(news_item)
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error processing single crypto news: {e}")
            return False

    def run(self, batch_size: int = 20):
        """Run the AI crypto processing pipeline"""
        try:
            logger.info("🪙 Starting AI Crypto News Processing Pipeline...")
            
            # Fetch latest 20 crypto articles (regardless of processed status)
            latest_articles = self.fetch_latest_news()
            
            if not latest_articles:
                logger.info("No crypto articles found")
                return True
            
            # Process all articles
            processed_count = 0
            passed_count = 0
            skipped_count = 0
            
            for i, news_item in enumerate(latest_articles, 1):
                logger.info(f"\n🪙 Processing {i}/{len(latest_articles)}")
                logger.info(f"   Title: {news_item.get('title', '')[:80]}...")
                
                # Check if already processed first
                if self.is_already_processed(news_item):
                    skipped_count += 1
                    logger.info(f"⏭️  Skipped (already processed)")
                    continue
                
                if self.process_single_news(news_item):
                    passed_count += 1
                
                processed_count += 1
                
                # Small delay to avoid rate limits
                time.sleep(1)
            
            logger.info(f"\n✅ Crypto processing complete!")
            logger.info(f"   Total articles: {len(latest_articles)}")
            logger.info(f"   Already processed (skipped): {skipped_count}")
            logger.info(f"   Newly processed: {processed_count}")
            logger.info(f"   Passed filter: {passed_count}")
            logger.info(f"   Blocked: {processed_count - passed_count}")
            
            return True
            
        except Exception as e:
            logger.error(f"Fatal error in crypto processing pipeline: {e}")
            return False


def main():
    """Main function - runs continuously every minute"""
    logger.info("=" * 60)
    logger.info("🪙 AI Crypto News Processing Service")
    logger.info("🎯 Evaluates and processes market-moving crypto news")
    logger.info("⚡ Updates every 60 seconds for real-time processing")
    logger.info("=" * 60)
    
    # Check for run mode
    run_mode = os.getenv('RUN_MODE', 'continuous').lower()
    batch_size = int(os.getenv('BATCH_SIZE', '20'))  # Changed default to 20
    
    # Initialize processor
    try:
        processor = AICryptoNewsProcessor()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Required environment variables:")
        logger.error("- OPENAI_API_KEY")
        logger.error("- SUPABASE_URL")
        logger.error("- SUPABASE_KEY")
        logger.error("- BATCH_SIZE (default: 20)")
        exit(1)
    
    # Run once mode
    if run_mode == 'once':
        logger.info("Running in ONCE mode")
        success = processor.run(batch_size=batch_size)
        exit(0 if success else 1)
    
    # Continuous mode - runs every minute
    logger.info(f"Running in CONTINUOUS mode (every 60 seconds)")
    failures = 0
    max_failures = 3
    
    while True:
        try:
            start_time = datetime.now()
            logger.info(f"\n{'=' * 50}")
            logger.info(f"⏰ Run started at {start_time.strftime('%H:%M:%S')}")
            
            # Run processing
            if processor.run(batch_size=batch_size):
                failures = 0
            else:
                failures += 1
                if failures >= max_failures:
                    logger.error(f"Too many failures ({max_failures}). Exiting...")
                    exit(1)
            
            # Calculate sleep time for 60-second intervals
            elapsed = (datetime.now() - start_time).total_seconds()
            sleep_seconds = max(60 - elapsed, 1)  # Run every minute
            
            logger.info(f"⏱️  Took {elapsed:.1f}s. Next run in {sleep_seconds:.1f}s...")
            time.sleep(sleep_seconds)
            
        except KeyboardInterrupt:
            logger.info("\n⛔ Shutting down...")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            failures += 1
            if failures >= max_failures:
                exit(1)
            time.sleep(60)


if __name__ == "__main__":
    main()
