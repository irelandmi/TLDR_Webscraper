import json
import os
import boto3
import requests
import time
import random
from datetime import datetime, timedelta
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List, Optional, Dict
import re
import hashlib
import logging

with open('user_agent_name.json', 'r') as f:
    USER_AGENT_DATA = json.load(f)
    USER_AGENT = USER_AGENT_DATA.get("user_agent")

# Configure logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)

@dataclass
class ScrapingConfig:
    """Configuration for responsible web scraping"""
    user_agent: str = USER_AGENT
    delay_range: tuple = (1, 3)
    max_retries: int = 3
    timeout: int = 10
    respect_robots_txt: bool = True
    max_consecutive_failures: int = 5

class RobotsTxtChecker:
    """Helper class to check robots.txt compliance"""
    
    def __init__(self):
        self.robot_parsers = {}
    
    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Check if we can fetch a URL according to robots.txt"""
        domain = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        
        if domain not in self.robot_parsers:
            robots_url = urljoin(domain, '/robots.txt')
            rp = RobotFileParser()
            rp.set_url(robots_url)
            try:
                response = requests.get(robots_url, timeout=10)
                if response.status_code == 200:
                    rp.parse(response.text.splitlines())
                    self.robot_parsers[domain] = rp
                    logger.info(f"Loaded robots.txt from {robots_url}")
                else:
                    raise Exception(f"HTTP {response.status_code}")
            except Exception as e:
                logger.warning(f"Could not load robots.txt from {robots_url}: {e}")
                rp = RobotFileParser()
                rp.set_url(robots_url)
                self.robot_parsers[domain] = rp
        
        return self.robot_parsers[domain].can_fetch(user_agent, url)
    
    def get_crawl_delay(self, url: str, user_agent: str = "*") -> Optional[float]:
        """Get crawl delay from robots.txt"""
        domain = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        if domain in self.robot_parsers:
            return self.robot_parsers[domain].crawl_delay(user_agent)
        return None

class LambdaScraper:
    """Lambda-optimized scraper"""
    
    def __init__(self, config: ScrapingConfig = None):
        self.config = config or ScrapingConfig()
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.config.user_agent})
        self.robots_checker = RobotsTxtChecker()
        self.consecutive_failures = 0
    
    def _should_scrape_url(self, url: str) -> bool:
        """Check if URL should be scraped"""
        if self.config.respect_robots_txt:
            if not self.robots_checker.can_fetch(url, self.config.user_agent):
                logger.warning(f"robots.txt disallows scraping {url}")
                return False
        return True
    
    def _get_delay(self, url: str) -> float:
        """Get appropriate delay before next request"""
        robots_delay = self.robots_checker.get_crawl_delay(url, self.config.user_agent)
        if robots_delay:
            return max(robots_delay, random.uniform(*self.config.delay_range))
        return random.uniform(*self.config.delay_range)
    
    def scrape_url(self, url: str) -> Optional[Dict]:
        """Scrape a single URL and return structured data"""
        if not self._should_scrape_url(url):
            return None
        
        for attempt in range(self.config.max_retries):
            try:
                delay = self._get_delay(url)
                logger.info(f"Waiting {delay:.2f} seconds before scraping {url}")
                time.sleep(delay)
                
                response = self.session.get(url, timeout=self.config.timeout)
                response.raise_for_status()
                
                self.consecutive_failures = 0
                
                soup = BeautifulSoup(response.content, 'html.parser')
                articles = self._extract_articles(soup, url)
                
                data = {
                    'url': url,
                    'title': self._extract_title(soup),
                    'content': self._extract_content(soup),
                    'meta_description': self._extract_meta_description(soup),
                    'headings': self._extract_headings(soup),
                    'articles': articles,
                    'article_count': len(articles),
                    'status_code': response.status_code,
                    'scraped_at': time.time()
                }
                
                logger.info(f"Successfully scraped {url} - Found {len(articles)} articles")
                return data
                
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        self.consecutive_failures += 1
        logger.error(f"Failed to scrape {url} after {self.config.max_retries} attempts")
        return None
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract page title"""
        title_tag = soup.find('title')
        return title_tag.get_text().strip() if title_tag else ""
    
    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Extract main content"""
        for script in soup(["script", "style"]):
            script.decompose()
        
        content_selectors = [
            'main', 'article', '.content', '#content', 
            '.post-content', '.entry-content', '.article-body'
        ]
        
        for selector in content_selectors:
            content = soup.select_one(selector)
            if content:
                return content.get_text(separator=' ', strip=True)
        
        body = soup.find('body')
        if body:
            return body.get_text(separator=' ', strip=True)
        
        return soup.get_text(separator=' ', strip=True)
    
    def _extract_meta_description(self, soup: BeautifulSoup) -> str:
        """Extract meta description"""
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        return meta_desc.get('content', '').strip() if meta_desc else ""
    
    def _extract_headings(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract headings with hierarchy"""
        headings = []
        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            headings.append({
                'level': heading.name,
                'text': heading.get_text().strip()
            })
        return headings
    
    def _extract_articles(self, soup: BeautifulSoup, page_url: str) -> List[Dict]:
        """Extract individual articles from newsletter-style pages"""
        articles = []
        
        article_containers = []
        article_containers.extend(soup.find_all('div', class_=lambda x: x and 'flex' in x and any(
            pattern in str(x) for pattern in ['flex-col', 'flex-row-reverse']
        )))
        article_containers.extend(soup.find_all('article'))
        article_containers.extend(soup.find_all('div', class_=lambda x: x and any(
            pattern in str(x).lower() for pattern in ['article', 'post', 'story', 'item']
        )))
        
        seen_containers = set()
        unique_containers = []
        for container in article_containers:
            container_html = str(container)[:200]
            if container_html not in seen_containers:
                seen_containers.add(container_html)
                unique_containers.append(container)
        
        logger.info(f"Found {len(unique_containers)} potential article containers")
        
        for idx, container in enumerate(unique_containers):
            try:
                article = self._parse_article_container(container, page_url, idx)
                if article and article.get('title'):
                    articles.append(article)
            except Exception as e:
                logger.warning(f"Failed to parse article container {idx}: {e}")
                continue
        
        return articles
    
    def _parse_article_container(self, container: BeautifulSoup, page_url: str, index: int) -> Optional[Dict]:
        """Parse an individual article container"""
        title = None
        title_tags = container.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        for tag in title_tags:
            title_text = tag.get_text().strip()
            if title_text and len(title_text) > 10:
                title = title_text
                break
        
        if not title:
            return None
        
        article_link = None
        links = container.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            if href and not href.startswith('#') and not href.startswith('/'):
                article_link = href
                break
            elif href and href.startswith('/'):
                article_link = urljoin(page_url, href)
        
        image_url = None
        img_tag = container.find('img')
        if img_tag:
            image_url = img_tag.get('src') or img_tag.get('data-src')
            if image_url and not image_url.startswith('http'):
                image_url = urljoin(page_url, image_url)
        
        description = None
        desc_candidates = container.find_all('div', class_=lambda x: x and any(
            pattern in str(x).lower() for pattern in ['line-clamp', 'description', 'summary', 'excerpt']
        ))
        
        for candidate in desc_candidates:
            desc_text = candidate.get_text().strip()
            if desc_text and len(desc_text) > 20:
                description = desc_text
                break
        
        if not description:
            p_tags = container.find_all('p')
            for p in p_tags:
                p_text = p.get_text().strip()
                if p_text and len(p_text) > 20:
                    description = p_text
                    break
        
        full_text = []
        for elem in container.find_all(text=True):
            if elem.parent.name not in ['script', 'style', 'button', 'a']:
                text = elem.strip()
                if text and len(text) > 5:
                    full_text.append(text)
        
        body_text = ' '.join(full_text)
        article_id = hashlib.md5(f"{page_url}|{title}|{index}".encode()).hexdigest()[:16]
        
        return {
            'record_id': article_id,
            'title': title,
            'link': article_link,
            'description': description,
            'body': body_text,
            'image_url': image_url,
            'source_page': page_url,
            'extraction_index': index
        }

def generate_date_range(start, end):
    """Generate dates between start and end"""
    start_dt = datetime.strptime(start, '%Y-%m-%d')
    end_dt = datetime.strptime(end, '%Y-%m-%d')
    
    dates = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    return dates

def format_url_with_date(url_template: str, date_str: str) -> str:
    """Replace date placeholders in URL template"""
    date = datetime.strptime(date_str, '%Y-%m-%d')
    replacements = {
        '{YYYY}': date.strftime('%Y'),
        '{MM}': date.strftime('%m'),
        '{DD}': date.strftime('%d'),
        '{YYYY-MM-DD}': date.strftime('%Y-%m-%d'),
        '{M}': str(date.month),
        '{D}': str(date.day)
    }
    
    formatted_url = url_template
    for placeholder, value in replacements.items():
        formatted_url = formatted_url.replace(placeholder, value)
    
    return formatted_url

def lambda_handler(event, context):
    """
    Lambda handler for web scraping with category support
    
    Event format:
    {
        "category": "tech",  # Options: tech, ai, devops, crypto, design, marketing, etc.
        "start_date": "2025-01-01",
        "end_date": "2025-01-31"
    }
    OR
    {
        "category": "ai",
        "date": "2025-01-01"
    }
    OR (for custom URLs)
    {
        "url_template": "https://custom-site.com/newsletter/{YYYY-MM-DD}",
        "start_date": "2025-01-01",
        "end_date": "2025-01-31"
    }
    OR (for multiple categories)
    {
        "categories": ["tech", "ai", "devops"],
        "date": "2025-01-01"
    }
    """
    logger.info(f"Lambda started with event: {json.dumps(event)}")
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    bucket = os.environ.get('S3_BUCKET')
    
    if not bucket:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'S3_BUCKET environment variable not set'})
        }
    
    # Known TLDR categories
    TLDR_CATEGORIES = {
        'tech': 'https://tldr.tech/tech/{YYYY-MM-DD}',
        'ai': 'https://tldr.tech/ai/{YYYY-MM-DD}',
        'devops': 'https://tldr.tech/devops/{YYYY-MM-DD}',
        'crypto': 'https://tldr.tech/crypto/{YYYY-MM-DD}',
        'design': 'https://tldr.tech/design/{YYYY-MM-DD}',
        'marketing': 'https://tldr.tech/marketing/{YYYY-MM-DD}',
        'founders': 'https://tldr.tech/founders/{YYYY-MM-DD}',
        'webdev': 'https://tldr.tech/webdev/{YYYY-MM-DD}',
        'infosec': 'https://tldr.tech/infosec/{YYYY-MM-DD}',
        'product': 'https://tldr.tech/product/{YYYY-MM-DD}'
    }
    
    # Determine which URLs to scrape
    url_templates = []
    
    # Handle single category
    if 'category' in event:
        category = event['category'].lower()
        if category in TLDR_CATEGORIES:
            url_templates.append({
                'template': TLDR_CATEGORIES[category],
                'category': category
            })
        else:
            # Allow custom category with standard TLDR format
            url_templates.append({
                'template': f'https://tldr.tech/{category}/{{YYYY-MM-DD}}',
                'category': category
            })
            logger.warning(f"Using custom category '{category}' - may not exist on TLDR")
    
    # Handle multiple categories
    elif 'categories' in event:
        for category in event['categories']:
            category = category.lower()
            if category in TLDR_CATEGORIES:
                url_templates.append({
                    'template': TLDR_CATEGORIES[category],
                    'category': category
                })
            else:
                url_templates.append({
                    'template': f'https://tldr.tech/{category}/{{YYYY-MM-DD}}',
                    'category': category
                })
    
    # Handle custom URL template (backward compatibility)
    elif 'url_template' in event:
        url_templates.append({
            'template': event['url_template'],
            'category': 'custom'
        })
    
    # Default to tech if nothing specified
    else:
        url_templates.append({
            'template': TLDR_CATEGORIES['tech'],
            'category': 'tech'
        })
        logger.info("No category specified, defaulting to 'tech'")
    
    # Determine dates to scrape
    if 'date' in event:
        dates = [event['date']]
    elif 'start_date' in event and 'end_date' in event:
        dates = generate_date_range(
            event.get('start_date', datetime.now().strftime('%Y-%m-%d')),
            event.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        )
    else:
        # Default to yesterday's date
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        dates = [yesterday_str]
        logger.info("No date range specified, defaulting to yesterday")
    
    # Initialize scraper
    config = ScrapingConfig(
        user_agent=event.get('user_agent', USER_AGENT),
        delay_range=tuple(event.get('delay_range', [1, 3])),
        max_retries=event.get('max_retries', 3)
    )
    scraper = LambdaScraper(config)
    
    all_results = []
    
    for url_info in url_templates:
        url_template = url_info['template']
        category = url_info['category']
        category_results = []
        
        logger.info(f"Processing category: {category}")
        
        for date_str in dates:
            logger.info(f"Processing date: {date_str} for category: {category}")
            
            # Format URL with date
            url = format_url_with_date(url_template, date_str)
            logger.info(f"Scraping URL: {url}")
            
            # Scrape the URL
            data = scraper.scrape_url(url)
            
            if data:
                # Add date and category metadata
                data['scraped_date'] = date_str
                data['category'] = category
                
                # Save full data to S3 with category in path
                full_key = f"scraped_data/{category}/full/{date_str}.json"
                s3.put_object(
                    Bucket=bucket,
                    Key=full_key,
                    Body=json.dumps(data, indent=2),
                    ContentType='application/json'
                )
                logger.info(f"Saved full data to s3://{bucket}/{full_key}")
                
                # Save articles only to S3 (optimized for downstream processing)
                articles_key = None
                if data.get('articles'):
                    articles_key = f"scraped_data/{category}/articles/{date_str}.json"
                    articles_data = {
                        'date': date_str,
                        'category': category,
                        'url': url,
                        'article_count': len(data['articles']),
                        'articles': data['articles'],
                        'scraped_at': data['scraped_at']
                    }
                    s3.put_object(
                        Bucket=bucket,
                        Key=articles_key,
                        Body=json.dumps(articles_data, indent=2),
                        ContentType='application/json'
                    )
                    logger.info(f"Saved articles to s3://{bucket}/{articles_key}")
                
                category_results.append({
                    'date': date_str,
                    'category': category,
                    'url': url,
                    'full_data_key': full_key,
                    'articles_key': articles_key,
                    'article_count': len(data.get('articles', [])),
                    'status': 'success'
                })
            else:
                logger.warning(f"Failed to scrape {date_str} for category {category}")
                category_results.append({
                    'date': date_str,
                    'category': category,
                    'url': url,
                    'status': 'failed'
                })
        
        all_results.extend(category_results)
    
    # Generate summary
    summary = {
        'total_urls_processed': len(all_results),
        'successful': sum(1 for r in all_results if r['status'] == 'success'),
        'failed': sum(1 for r in all_results if r['status'] == 'failed'),
        'total_articles': sum(r.get('article_count', 0) for r in all_results),
        'categories_processed': list(set(r['category'] for r in all_results)),
        'dates_processed': list(set(r['date'] for r in all_results))
    }
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f"Processed {len(all_results)} URLs across {len(url_templates)} categories",
            'summary': summary,
            'results': all_results
        }, indent=2)
    }