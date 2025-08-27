#!/usr/bin/env python3
import os
import json
import time
import re
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
        
        # Table names for crypto processing
        self.raw_table = 'crypto_news_articles'
        self.processed_table = 'crypto_clean_articles'
        
        logger.info(f"Initialized with source table: {self.raw_table}")
        logger.info(f"Initialized with destination table: {self.processed_table}")
        
        # Crypto-focused evaluation prompt
        self.evaluation_prompt = """
You are a crypto and blockchain news filter specializing in cryptocurrency content. Your job is to identify news specifically about crypto, blockchain, DeFi, and digital assets.

ALWAYS PASS news about:

**Cryptocurrency Price & Markets:**
- ANY crypto price movements (even <1% for major coins)
- Market cap changes or milestones
- Trading volume spikes or unusual activity
- Technical analysis levels (support/resistance)
- Whale movements or large transactions
- Liquidations and margin calls
- Exchange inflows/outflows

**Specific Cryptocurrencies (ALWAYS PASS):**
- Bitcoin (BTC) - any mention
- Ethereum (ETH) - any mention
- Major altcoins (SOL, ADA, AVAX, DOT, MATIC, etc.)
- Memecoins (DOGE, SHIB, PEPE) if significant movement
- Stablecoins (USDT, USDC, DAI) developments

**DeFi & Protocols:**
- DEX volumes and liquidity changes
- Yield farming and staking updates
- Lending/borrowing protocol news
- Protocol hacks or exploits
- TVL (Total Value Locked) changes
- New protocol launches
- Governance proposals and votes

**NFTs & Gaming:**
- Major NFT collection news
- NFT marketplace updates
- Blockchain gaming developments
- Metaverse projects
- Play-to-earn economies

**Blockchain Technology:**
- Network upgrades and hard forks
- Layer 2 developments (Arbitrum, Optimism, Polygon)
- Smart contract innovations
- Consensus mechanism changes
- Cross-chain bridges
- Zero-knowledge proofs
- Scalability solutions

**Mining & Infrastructure:**
- Hash rate changes
- Mining difficulty adjustments
- Mining profitability
- Energy usage debates
- Mining bans or regulations
- ASIC developments

**Institutional Crypto:**
- Corporate Bitcoin/crypto purchases
- Crypto ETF news and approvals
- Traditional finance crypto adoption
- Payment companies crypto integration
- Crypto custody solutions
- Institutional trading platforms

**Regulation & Legal:**
- Crypto-specific regulations
- SEC actions on crypto
- Crypto tax policies
- Exchange licenses
- Stablecoin regulations
- CBDC developments

**Crypto Exchanges:**
- New listing announcements
- Exchange volumes
- Security incidents
- Platform updates
- Withdrawal/deposit issues
- Trading pair additions

**Web3 & Emerging:**
- DAO developments
- Social tokens
- Decentralized identity
- Blockchain + AI integration
- RWA (Real World Assets) tokenization

BLOCK news that is:
- General finance without crypto angle
- Traditional stock market only
- General technology without blockchain
- Macro economics without crypto impact
- Political news without crypto connection

Also provide detailed metadata about the crypto relevance.

Analyze this crypto news and respond with JSON:
{{
    "decision": "PASS" or "BLOCK",
    "reason": "Brief explanation",
    "relevance_score": 0.0 to 1.0,
    "categories": ["DeFi", "Bitcoin", "Ethereum", "NFT", "Regulation", etc.],
    "importance": "HIGH", "MEDIUM", or "LOW",
    "mentioned_cryptos": ["BTC", "ETH", etc.],
    "mentioned_blockchains": ["Ethereum", "Solana", etc.]
}}

CRYPTO NEWS:
Headline: {headline}
Description: {description}
Source: {source}
"""

        # Crypto-specific processing prompt
        self.processing_prompt = """
You are a crypto news processor that creates Watcher.guru style headlines for cryptocurrency news. Analyze this crypto article and create focused content.

ORIGINAL CRYPTO ARTICLE:
Headline: {headline}
Description: {description}
Source: {source}
Link: {link}

IMPORTANT: If the description is missing, identical to the headline, or too brief:
1. Create a NEW crypto-focused description based on the headline
2. Use your crypto knowledge to provide relevant context
3. Include relevant metrics if known (price, percentage, volume)
4. Keep it crypto-focused and urgent

WATCHER.GURU CRYPTO STYLE RULES:
1. Start with PERSON/ORGANIZATION/CRYPTO NAME then action
2. Use NO commas or periods in headlines EXCEPT in dollar amounts
3. Add 🇺🇸 flags ONLY for government officials (SEC Chair, Treasury Secretary)
4. MONEY FORMATTING:
   - Under $1 million: Use exact amounts with commas: $750,000 (NOT $750k)
   - $1 million and above: Use words: $2.5 million, $1.3 billion
   - Commas in dollar amounts are THE ONLY exception to the no-comma rule
5. ALWAYS include crypto tickers with $ prefix ($BTC $ETH $SOL)
6. Include percentages for price movements
7. Use urgent crypto trading tone
8. Specific verbs: "pumps" "dumps" "surges" "crashes" "moons" "bleeds"

CRYPTO HEADLINE EXAMPLES:
- "Bitcoin $BTC surges 8% to break $45,000 resistance as ETF approval nears"
- "Vitalik Buterin burns $500,000 worth of memecoins sent to his wallet"
- "🇺🇸 SEC Chair Gensler says most cryptocurrencies are securities"
- "Binance sees $2.1 billion in withdrawals following CEO resignation"
- "Ethereum $ETH gas fees drop 90% after Dencun upgrade activation"
- "Michael Saylor's MicroStrategy buys 12,333 Bitcoin $BTC for $347 million"
- "Solo miner strikes gold with $373,000 Bitcoin $BTC block beating millions of competitors"
- "$420 million liquidated from crypto market as Bitcoin $BTC drops below $40,000"
- "Crypto trader loses $850,000 in failed leverage position on Binance"

CRITICAL CRYPTO TICKER EXTRACTION:
Extract ALL crypto tickers mentioned by name or symbol (max 5, most important):

**Top Cryptos (name → ticker):**
- Bitcoin/BTC → BTC
- Ethereum/ETH → ETH
- Tether → USDT
- BNB/Binance Coin → BNB
- Solana → SOL
- XRP/Ripple → XRP
- Cardano → ADA
- Dogecoin/Doge → DOGE
- TRON → TRX
- Avalanche → AVAX
- Shiba Inu → SHIB
- Polygon/Matic → MATIC
- Polkadot → DOT
- Chainlink → LINK
- Wrapped Bitcoin → WBTC
- Litecoin → LTC
- Bitcoin Cash → BCH
- NEAR Protocol → NEAR
- Cosmos → ATOM
- Arbitrum → ARB
- Optimism → OP
- Aptos → APT

**DeFi Tokens:**
- Uniswap → UNI
- Aave → AAVE
- Maker → MKR
- Compound → COMP
- Curve → CRV
- PancakeSwap → CAKE
- SushiSwap → SUSHI

**Memecoins:**
- Pepe → PEPE
- Floki → FLOKI
- Bonk → BONK

EXTRACT NUMERIC VALUES:
If the article mentions specific numbers, extract them:
- Price: "$45,000" or "$45.5K" → store actual number
- Percentage: "surged 15%" → store 15.0
- Volume: "$2.3 billion volume" → store number
- Market cap: "$1 trillion market cap" → store number

Create the following JSON:

{{
    "processed_headline": "Watcher.guru crypto headline (max 120 chars, commas ONLY in dollar amounts)",
    "processed_description": "Crypto-focused description (max 180 chars, commas ONLY in dollar amounts)",
    "tickers": ["BTC", "ETH", etc.] max 5 tickers - NEVER empty, NEVER ["CRYPTO"],
    "sentiment": "BULLISH" or "BEARISH" or "NEUTRAL",
    "market_impact": "Crypto market implications (max 200 chars)",
    "price_mentioned": null or number,
    "price_change_percent": null or number,
    "volume_mentioned": null or number,
    "market_cap_mentioned": null or number
}}
"""

    def fetch_latest_crypto_news(self) -> List[Dict]:
        """Fetch latest 20 articles from crypto_news_articles table"""
        try:
            logger.info(f"Fetching latest 20 crypto articles from {self.raw_table}")
            
            # Get latest 20 articles ordered by published_at
            result = self.supabase.table(self.raw_table)\
                .select('*')\
                .order('published_at', desc=True)\
                .limit(20)\
                .execute()
            
            if result.data:
                logger.info(f"Fetched {len(result.data)} crypto articles from {self.raw_table}")
                return result.data
            else:
                logger.info("No crypto articles found")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching from {self.raw_table}: {e}")
            return []

    def is_already_processed(self, news_item: Dict) -> bool:
        """Check if this crypto article is already processed using multiple checks"""
        try:
            # Check by link first
            original_link = news_item.get('link')
            if original_link:
                result = self.supabase.table(self.processed_table)\
                    .select('id')\
                    .eq('original_link', original_link)\
                    .execute()
                
                if len(result.data) > 0:
                    return True
            
            # Also check by headline to avoid duplicates with different IDs
            original_headline = news_item.get('headline', '')
            if original_headline:
                # Check if same headline was processed in last 24 hours
                from datetime import datetime, timedelta
                cutoff_time = (datetime.now() - timedelta(hours=24)).isoformat()
                
                result = self.supabase.table(self.processed_table)\
                    .select('id')\
                    .eq('original_headline', original_headline)\
                    .gte('processed_at', cutoff_time)\
                    .execute()
                
                if len(result.data) > 0:
                    logger.info(f"   Found duplicate headline processed within 24 hours")
                    return True
            
            # Also check by original_id if it exists
            original_id = news_item.get('id')
            if original_id:
                result = self.supabase.table(self.processed_table)\
                    .select('id')\
                    .eq('original_id', str(original_id))\
                    .execute()
                
                if len(result.data) > 0:
                    return True
                    
            return False
            
        except Exception as e:
            logger.error(f"Error checking if crypto news processed: {e}")
            return False

    def maintain_table_size_limit(self):
        """Keep only 100 articles in crypto_clean_articles"""
        try:
            count_result = self.supabase.table(self.processed_table)\
                .select('id', count='exact')\
                .execute()
            
            current_count = count_result.count
            logger.info(f"Current crypto articles in {self.processed_table}: {current_count}")
            
            if current_count >= 100:
                articles_to_delete = current_count - 99
                
                oldest_articles = self.supabase.table(self.processed_table)\
                    .select('id')\
                    .order('original_published_at', desc=False)\
                    .limit(articles_to_delete)\
                    .execute()
                
                if oldest_articles.data:
                    ids_to_delete = [article['id'] for article in oldest_articles.data]
                    
                    for article_id in ids_to_delete:
                        self.supabase.table(self.processed_table)\
                            .delete()\
                            .eq('id', article_id)\
                            .execute()
                    
                    logger.info(f"🧹 Removed {len(ids_to_delete)} oldest crypto articles")
                
        except Exception as e:
            logger.error(f"Error maintaining crypto table size limit: {e}")

    def evaluate_crypto_relevance(self, news_item: Dict) -> Tuple[bool, Dict]:
        """Evaluate if news is crypto-specific"""
        try:
            description = news_item.get('description', '')
            headline = news_item.get('headline', '')
            
            if not description or description == headline:
                description = "[No description - evaluate based on headline]"
            
            prompt = self.evaluation_prompt.format(
                headline=headline,
                description=description,
                source=news_item.get('source_name', 'Unknown')
            )
            
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a crypto news evaluator. Focus on crypto/blockchain content. Respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=250,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            if result.get('decision') == "PASS":
                logger.info(f"✅ CRYPTO PASSED (score: {result.get('relevance_score', 0):.2f}): {headline[:50]}...")
                return True, result
            else:
                logger.info(f"❌ BLOCKED (not crypto): {headline[:50]}...")
                return False, result
                
        except Exception as e:
            logger.error(f"Error evaluating crypto news: {e}")
            return False, {"reason": f"Error: {str(e)}", "relevance_score": 0, "categories": [], "importance": "LOW"}

    def process_crypto_content(self, news_item: Dict) -> Optional[Dict]:
        """Process crypto news with enhanced extraction"""
        try:
            description = news_item.get('description', '')
            headline = news_item.get('headline', '')
            
            if not description or description.strip() == headline.strip():
                description = "[CREATE CRYPTO DESCRIPTION - Original missing/identical]"
            
            prompt = self.processing_prompt.format(
                headline=headline,
                description=description,
                source=news_item.get('source_name', 'Unknown'),
                link=news_item.get('link', '')
            )
            
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a crypto news processor. Extract ALL crypto tickers from names (Bitcoin→BTC). Money formatting: under $1M use commas ($750,000), over $1M use words ($1.5 million). Commas ONLY allowed in dollar amounts, nowhere else. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=500,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # Validate required fields
            required = ['processed_headline', 'processed_description', 'tickers', 'sentiment', 'market_impact']
            if all(field in result for field in required):
                # Ensure tickers is never empty for crypto news
                if not result['tickers'] or result['tickers'] == ["CRYPTO"]:
                    # Default to BTC if no specific crypto mentioned
                    result['tickers'] = ["BTC"]
                return result
            else:
                logger.error(f"Missing required fields in crypto AI response")
                return None
                
        except Exception as e:
            logger.error(f"Error processing crypto content: {e}")
            return None

    def extract_number_from_text(self, text: str, pattern: str) -> Optional[float]:
        """Extract numeric values from text using regex patterns"""
        try:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                number_str = match.group(1).replace(',', '').replace('$', '')
                
                # Handle millions/billions
                if 'million' in match.group(0).lower():
                    return float(number_str) * 1000000
                elif 'billion' in match.group(0).lower():
                    return float(number_str) * 1000000000
                elif 'trillion' in match.group(0).lower():
                    return float(number_str) * 1000000000000
                else:
                    return float(number_str)
        except:
            pass
        return None

    def store_processed_crypto_news(self, news_item: Dict, evaluation_data: Dict, processed_data: Dict) -> bool:
        """Store processed crypto news with all metadata"""
        try:
            # Maintain table size limit
            self.maintain_table_size_limit()
            
            # Extract numeric values if mentioned
            full_text = f"{news_item.get('headline', '')} {news_item.get('description', '')}"
            
            # Prepare data for storage
            final_data = {
                # Original data
                'original_id': news_item.get('id', news_item.get('link', '')),
                'original_headline': news_item.get('headline', ''),
                'original_description': news_item.get('description', ''),
                'original_link': news_item.get('link', ''),
                'original_published_at': news_item.get('published_at'),  # PRESERVE TIME
                'original_source_name': news_item.get('source_name', ''),
                
                # AI-processed crypto content
                'processed_headline': processed_data['processed_headline'][:120],
                'processed_description': processed_data['processed_description'][:180],
                'tickers': processed_data['tickers'][:5],  # Max 5 tickers
                'sentiment': processed_data['sentiment'],
                'market_impact': processed_data['market_impact'][:200],
                
                # Crypto-specific metadata
                'relevance_score': evaluation_data.get('relevance_score', 0.5),
                'evaluation_reason': evaluation_data.get('reason', ''),
                'categories': evaluation_data.get('categories', []),
                'importance_level': evaluation_data.get('importance', 'MEDIUM'),
                
                # Blockchain/protocol data
                'blockchain_mentioned': evaluation_data.get('mentioned_blockchains', []),
                'defi_protocol': [],  # Could be enhanced to extract DeFi protocols
                
                # Numeric values (if provided by AI)
                'price_mentioned': processed_data.get('price_mentioned'),
                'price_change_percent': processed_data.get('price_change_percent'),
                'volume_mentioned': processed_data.get('volume_mentioned'),
                'market_cap_mentioned': processed_data.get('market_cap_mentioned'),
                
                # Processing metadata
                'processed_at': datetime.now().isoformat(),
                'processing_version': '1.0'
            }
            
            # Insert into database
            result = self.supabase.table(self.processed_table).insert(final_data).execute()
            logger.info(f"✅ Stored crypto news: {processed_data['processed_headline'][:60]}...")
            return True
            
        except Exception as e:
            logger.error(f"Error storing processed crypto news: {e}")
            return False

    def process_single_crypto_news(self, news_item: Dict) -> bool:
        """Process a single crypto news item"""
        try:
            # Check if already processed
            if self.is_already_processed(news_item):
                logger.info(f"⏭️  Already processed: {news_item.get('headline', '')[:50]}...")
                return False
            
            # Evaluate crypto relevance
            is_relevant, evaluation_data = self.evaluate_crypto_relevance(news_item)
            
            if not is_relevant:
                return False
            
            # Process content
            processed = self.process_crypto_content(news_item)
            if not processed:
                return False
            
            # Store in database
            if self.store_processed_crypto_news(news_item, evaluation_data, processed):
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error processing single crypto news: {e}")
            return False

    def run(self, batch_size: int = 20):
        """Run the crypto news processing pipeline"""
        try:
            logger.info("🪙 Starting AI Crypto News Processing Pipeline...")
            
            # Fetch latest crypto articles
            latest_articles = self.fetch_latest_crypto_news()
            
            if not latest_articles:
                logger.info("No crypto articles found")
                return True
            
            # Process all articles
            processed_count = 0
            passed_count = 0
            skipped_count = 0
            
            for i, news_item in enumerate(latest_articles, 1):
                logger.info(f"\n🪙 Processing crypto article {i}/{len(latest_articles)}")
                logger.info(f"   Headline: {news_item.get('headline', '')[:80]}...")
                logger.info(f"   Published: {news_item.get('published_at', 'Unknown')}")
                
                # Check if already processed
                if self.is_already_processed(news_item):
                    skipped_count += 1
                    logger.info(f"⏭️  Skipped (already processed)")
                    continue
                
                if self.process_single_crypto_news(news_item):
                    passed_count += 1
                
                processed_count += 1
                
                # Small delay to avoid rate limits
                time.sleep(1)
            
            logger.info(f"\n✅ Crypto processing complete!")
            logger.info(f"   Total crypto articles: {len(latest_articles)}")
            logger.info(f"   Already processed: {skipped_count}")
            logger.info(f"   Newly processed: {processed_count}")
            logger.info(f"   Passed crypto filter: {passed_count}")
            logger.info(f"   Blocked (not crypto): {processed_count - passed_count}")
            
            return True
            
        except Exception as e:
            logger.error(f"Fatal error in crypto processing pipeline: {e}")
            return False


def main():
    """Main function - runs continuously every minute"""
    logger.info("=" * 60)
    logger.info("🪙 AI Crypto News Processing Service")
    logger.info("₿ Specialized for cryptocurrency and blockchain news")
    logger.info("📰 Processing from: crypto_news_articles table")
    logger.info("✨ Storing to: crypto_clean_articles table")
    logger.info("⚡ Updates every 60 seconds")
    logger.info("=" * 60)
    
    # Check for run mode
    run_mode = os.getenv('RUN_MODE', 'continuous').lower()
    batch_size = int(os.getenv('BATCH_SIZE', '20'))
    
    # Initialize processor
    try:
        processor = AICryptoNewsProcessor()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Required environment variables:")
        logger.error("- OPENAI_API_KEY")
        logger.error("- SUPABASE_URL")
        logger.error("- SUPABASE_KEY")
        logger.error("Optional:")
        logger.error("- RUN_MODE (once/continuous, default: continuous)")
        logger.error("- BATCH_SIZE (default: 20)")
        exit(1)
    
    # Run once mode
    if run_mode == 'once':
        logger.info("Running in ONCE mode")
        success = processor.run(batch_size=batch_size)
        exit(0 if success else 1)
    
    # Continuous mode
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
            
            # Calculate sleep time
            elapsed = (datetime.now() - start_time).total_seconds()
            sleep_seconds = max(60 - elapsed, 1)
            
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
