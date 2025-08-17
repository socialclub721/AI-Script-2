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
        
        # Market impact evaluation prompt
        self.evaluation_prompt = """
You are a financial news evaluation agent responsible for filtering market-moving information from noise. Your critical task is to identify only high-impact news that genuinely affects stock markets, crypto markets, or global financial conditions - everything else must be blocked.

PASS if news matches ANY:

Critical Events:
- War outbreak, military escalation, geopolitical crisis
- Fed/Central bank decisions (rates, QE, Powell statements)
- Market crash >3% major indices
- Bank collapse/bailout (systemically important)
- Regulatory shocks (bans, new crypto/stock laws)
+ other critical events that might move markets instantly

Market Movers:
- Stock movement >10% (large cap) or >20% (mid cap)
- Earnings surprise >20% (S&P500 companies)
- M&A deals >$10B
- Insider trading >$100M
- Warren Buffett/Berkshire positions
+ other big asset managements moving stuff

Crypto:
- BTC & ETH movement >5% (24h)
- Top 10 crypto new ATH
- Institutional adoption >$1B AUM
- Liquidations >$500M
- Major exchange/protocol hack
- ETF approval/rejection
+ other major crypto news

Money Flows:
- Trillion dollar manager moves (BlackRock, Vanguard)
- Sector rotation >$10B
- DXY movement >1%
- Treasury yield change >10bps
- Commodity shock (Oil >5%, Gold >3%)
+ other major money flows

BLOCK:
- Entertainment/pop culture
- Small product launches
- Opinion pieces
- Micro-cap earnings (<$1B mcap)
- Non C-level personnel news
- Technical analysis
- Repeated/old news
+ unnecessary stuff that won't move markets and is consumer/normie based

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

    def fetch_unprocessed_news(self, hours_back: int = 24) -> List[Dict]:
        """Fetch unprocessed news from crypto_rss_news table"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            
            logger.info(f"Fetching unprocessed crypto news from {self.raw_table}")
            
            # Fetch news that hasn't been processed yet
            # Using correct column names: pubdate_parsed instead of datetime
            result = self.supabase.table(self.raw_table)\
                .select('*')\
                .eq('processed', False)\
                .gte('ingested_at', cutoff_time.isoformat())\
                .order('pubdate_parsed', desc=True)\
                .limit(50)\
                .execute()
            
            if result.data:
                logger.info(f"Fetched {len(result.data)} unprocessed crypto items from {self.raw_table}")
                # Sort by pubdate_parsed
                result.data.sort(key=lambda x: x.get('pubdate_parsed', ''), reverse=True)
                return result.data
            else:
                logger.info("No unprocessed crypto news found")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching from {self.raw_table}: {e}")
            return []

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
                    {"role": "system", "content": "You are a crypto news evaluator. Respond with PASS or BLOCK and a brief reason."},
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
        """Store processed crypto news in the clean Supabase table"""
        try:
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
            # Step 1: Evaluate importance
            is_important, reason = self.evaluate_news_importance(news_item)
            
            if not is_important:
                # Mark as processed even if blocked (to avoid re-checking)
                self.mark_as_processed(news_item)
                return False
            
            # Step 2: Process content with AI
            processed = self.process_news_content(news_item)
            if not processed:
                return False
            
            # Step 3: Prepare data for storage (updated field mappings)
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
            
            # Step 4: Store in processed table
            if self.store_processed_news(final_data):
                # Step 5: Mark original as processed
                self.mark_as_processed(news_item)
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error processing single crypto news: {e}")
            return False

    def run(self, batch_size: int = 10):
        """Run the AI crypto processing pipeline"""
        try:
            logger.info("🪙 Starting AI Crypto News Processing Pipeline...")
            
            # Fetch unprocessed news
            unprocessed = self.fetch_unprocessed_news(hours_back=24)
            
            if not unprocessed:
                logger.info("No unprocessed crypto news found")
                return True
            
            # Process in batches
            processed_count = 0
            passed_count = 0
            
            for i, news_item in enumerate(unprocessed[:batch_size], 1):
                logger.info(f"\n🪙 Processing {i}/{min(batch_size, len(unprocessed))}")
                logger.info(f"   Title: {news_item.get('title', '')[:80]}...")
                
                if self.process_single_news(news_item):
                    passed_count += 1
                
                processed_count += 1
                
                # Small delay to avoid rate limits
                time.sleep(1)
            
            logger.info(f"\n✅ Crypto processing complete!")
            logger.info(f"   Processed: {processed_count} articles")
            logger.info(f"   Passed filter: {passed_count} articles")
            logger.info(f"   Blocked: {processed_count - passed_count} articles")
            
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
    batch_size = int(os.getenv('BATCH_SIZE', '10'))
    
    # Initialize processor
    try:
        processor = AICryptoNewsProcessor()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Required environment variables:")
        logger.error("- OPENAI_API_KEY")
        logger.error("- SUPABASE_URL")
        logger.error("- SUPABASE_KEY")
        logger.error("- BATCH_SIZE (default: 10)")
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
