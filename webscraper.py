import requests
import time
import random
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List, Optional, Dict, Generator
import logging
from pathlib import Path
from datetime import datetime, timedelta
from io import StringIO
import re
import json
import hashlib
import csv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ScrapingConfig:
    """Configuration for responsible web scraping"""
    user_agent: str = "TLDR-Scraper/1.0 (Research; datubyte@datubyte.com)"
    delay_range: tuple = (1, 3)  # Random delay between requests in seconds
    max_retries: int = 3
    timeout: int = 10
    respect_robots_txt: bool = True
    max_pages_per_domain: int = 100
    debug_mode: bool = False  # Save raw HTML when True
    save_robots_txt: bool = True  # Save robots.txt files
    skip_weekends: bool = False  # Skip Saturday/Sunday when iterating dates
    skip_missing_dates: bool = True  # Continue if a date returns 404
    max_consecutive_failures: int = 5  # Stop after N consecutive failures
    
class RobotsTxtChecker:
    """Helper class to check robots.txt compliance"""
    
    def __init__(self, save_robots_txt: bool = True, output_dir: str = "scraped_data"):
        self.robot_parsers = {}
        self.robots_content = {}  # Store raw robots.txt content
        self.save_robots_txt = save_robots_txt
        self.output_dir = output_dir
        self.saved_robots = set()  # Track which robots.txt files we've saved
    
    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Check if we can fetch a URL according to robots.txt"""
        domain = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        
        if domain not in self.robot_parsers:
            robots_url = urljoin(domain, '/robots.txt')
            rp = RobotFileParser()
            rp.set_url(robots_url)
            try:
                # Fetch robots.txt content manually to save it
                response = requests.get(robots_url, timeout=10)
                if response.status_code == 200:
                    robots_content = response.text
                    self.robots_content[domain] = robots_content
                    
                    # Save robots.txt file if requested
                    if self.save_robots_txt and domain not in self.saved_robots:
                        self._save_robots_txt(domain, robots_content)
                        self.saved_robots.add(domain)
                    
                    # Parse with RobotFileParser
                    rp.read()
                    self.robot_parsers[domain] = rp
                    logger.info(f"Loaded and saved robots.txt from {robots_url}")
                else:
                    raise Exception(f"HTTP {response.status_code}")
                    
            except Exception as e:
                logger.warning(f"Could not load robots.txt from {robots_url}: {e}")
                # Create a permissive parser if robots.txt is not accessible
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
    
    def _save_robots_txt(self, domain: str, content: str):
        """Save robots.txt content to file"""
        output_path = Path(self.output_dir) / "robots_txt"
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create safe filename from domain
        safe_domain = domain.replace('https://', '').replace('http://', '').replace('/', '_')
        filename = f"robots_{safe_domain}.txt"
        filepath = output_path / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"# robots.txt for {domain}\n")
            f.write(f"# Retrieved at: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n")
            f.write(content)
        
        logger.info(f"Saved robots.txt to {filepath}")

class DateRangeGenerator:
    """Generate URLs with date patterns for time-series scraping"""
    
    @staticmethod
    def generate_date_range(start_date: str, end_date: str, skip_weekends: bool = False) -> Generator[datetime, None, None]:
        """Generate dates between start_date and end_date
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format  
            skip_weekends: If True, skip Saturday (5) and Sunday (6)
        """
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        current = start
        while current <= end:
            # Skip weekends if requested
            if skip_weekends and current.weekday() >= 5:  # 5=Saturday, 6=Sunday
                current += timedelta(days=1)
                continue
                
            yield current
            current += timedelta(days=1)
    
    @staticmethod
    def format_url_with_date(url_template: str, date: datetime) -> str:
        """Replace date placeholders in URL template with actual date
        
        Supported placeholders:
            {YYYY} - 4-digit year (2025)
            {MM} - 2-digit month (09) 
            {DD} - 2-digit day (15)
            {YYYY-MM-DD} - Full date (2025-09-15)
            {M} - 1-digit month (9)
            {D} - 1-digit day (15)
        """
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
    
    @staticmethod
    def extract_date_from_url(url: str) -> Optional[datetime]:
        """Extract date from URL if it contains a date pattern"""
        # Common date patterns in URLs
        patterns = [
            r'(\d{4})-(\d{2})-(\d{2})',  # YYYY-MM-DD
            r'(\d{4})/(\d{2})/(\d{2})',  # YYYY/MM/DD
            r'(\d{4})(\d{2})(\d{2})',    # YYYYMMDD
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                year, month, day = match.groups()
                try:
                    return datetime(int(year), int(month), int(day))
                except ValueError:
                    continue
        
        return None
    """Helper class to check robots.txt compliance"""
    
    def __init__(self, save_robots_txt: bool = True, output_dir: str = "scraped_data"):
        self.robot_parsers = {}
        self.robots_content = {}  # Store raw robots.txt content
        self.save_robots_txt = save_robots_txt
        self.output_dir = output_dir
        self.saved_robots = set()  # Track which robots.txt files we've saved
    
    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Check if we can fetch a URL according to robots.txt"""
        domain = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        
        if domain not in self.robot_parsers:
            robots_url = urljoin(domain, '/robots.txt')
            rp = RobotFileParser()
            rp.set_url(robots_url)
            try:
                # Fetch robots.txt content manually to save it
                response = requests.get(robots_url, timeout=10)
                if response.status_code == 200:
                    robots_content = response.text
                    self.robots_content[domain] = robots_content
                    
                    # Save robots.txt file if requested
                    if self.save_robots_txt and domain not in self.saved_robots:
                        self._save_robots_txt(domain, robots_content)
                        self.saved_robots.add(domain)
                    
                    # Parse with RobotFileParser
                    rp.read()
                    self.robot_parsers[domain] = rp
                    logger.info(f"Loaded and saved robots.txt from {robots_url}")
                else:
                    raise Exception(f"HTTP {response.status_code}")
                    
            except Exception as e:
                logger.warning(f"Could not load robots.txt from {robots_url}: {e}")
                # Create a permissive parser if robots.txt is not accessible
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
    
    def _save_robots_txt(self, domain: str, content: str):
        """Save robots.txt content to file"""
        output_path = Path(self.output_dir) / "robots_txt"
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create safe filename from domain
        safe_domain = domain.replace('https://', '').replace('http://', '').replace('/', '_')
        filename = f"robots_{safe_domain}.txt"
        filepath = output_path / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"# robots.txt for {domain}\n")
            f.write(f"# Retrieved at: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n")
            f.write(content)
        
        logger.info(f"Saved robots.txt to {filepath}")

class ResponsibleScraper:
    """A responsible web scraper that respects robots.txt and implements rate limiting"""
    
    def __init__(self, config: ScrapingConfig = None):
        self.config = config or ScrapingConfig()
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.config.user_agent})
        self.robots_checker = RobotsTxtChecker(
            save_robots_txt=self.config.save_robots_txt,
            output_dir="scraped_data"
        )
        self.domain_counters = {}
        self.consecutive_failures = 0
        
    def _should_scrape_url(self, url: str) -> bool:
        """Check if URL should be scraped based on various criteria"""
        domain = urlparse(url).netloc
        
        # Check domain limits
        if domain in self.domain_counters:
            if self.domain_counters[domain] >= self.config.max_pages_per_domain:
                logger.warning(f"Reached max pages limit for domain {domain}")
                return False
        
        # Check robots.txt
        if self.config.respect_robots_txt:
            if not self.robots_checker.can_fetch(url, self.config.user_agent):
                logger.warning(f"robots.txt disallows scraping {url}")
                return False
        
        return True
    
    def _get_delay(self, url: str) -> float:
        """Get appropriate delay before next request"""
        # Check robots.txt crawl delay
        robots_delay = self.robots_checker.get_crawl_delay(url, self.config.user_agent)
        if robots_delay:
            return max(robots_delay, random.uniform(*self.config.delay_range))
        
        return random.uniform(*self.config.delay_range)
    
    def scrape_url(self, url: str) -> Optional[Dict]:
        """Scrape a single URL and return structured data"""
        if not self._should_scrape_url(url):
            return None
        
        domain = urlparse(url).netloc
        
        for attempt in range(self.config.max_retries):
            try:
                # Respect rate limiting
                delay = self._get_delay(url)
                logger.info(f"Waiting {delay:.2f} seconds before scraping {url}")
                time.sleep(delay)
                
                response = self.session.get(url, timeout=self.config.timeout)
                response.raise_for_status()
                
                # Update domain counter and reset consecutive failures
                self.domain_counters[domain] = self.domain_counters.get(domain, 0) + 1
                self.consecutive_failures = 0
                
                # Parse content
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract structured articles (for sites like TLDR)
                articles = self._extract_articles(soup, url)
                
                # Extract structured data
                data = {
                    'url': url,
                    'title': self._extract_title(soup),
                    'content': self._extract_content(soup),
                    'meta_description': self._extract_meta_description(soup),
                    'headings': self._extract_headings(soup),
                    'links': self._extract_links(soup, url),
                    'articles': articles,  # New: structured article extraction
                    'article_count': len(articles),
                    'status_code': response.status_code,
                    'content_type': response.headers.get('content-type', ''),
                    'scraped_at': time.time()
                }
                
                # Add raw HTML in debug mode
                if self.config.debug_mode:
                    data['raw_html'] = response.text
                    data['response_headers'] = dict(response.headers)
                
                logger.info(f"Successfully scraped {url} - Found {len(articles)} articles")
                return data
                
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        # Increment consecutive failures counter
        self.consecutive_failures += 1
        logger.error(f"Failed to scrape {url} after {self.config.max_retries} attempts")
        return None
    
    def scrape_date_range(self, url_template: str, start_date: str, end_date: str) -> List[Dict]:
        """Scrape URLs across a date range
        
        Args:
            url_template: URL with date placeholders (e.g., "https://tldr.tech/tech/{YYYY-MM-DD}")
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of scraped data dictionaries
        """
        results = []
        total_dates = 0
        successful_scrapes = 0
        
        logger.info(f"Starting date range scrape from {start_date} to {end_date}")
        logger.info(f"URL template: {url_template}")
        
        for date in DateRangeGenerator.generate_date_range(start_date, end_date, self.config.skip_weekends):
            total_dates += 1
            url = DateRangeGenerator.format_url_with_date(url_template, date)
            
            logger.info(f"Processing date {date.strftime('%Y-%m-%d')} -> {url}")
            
            # Check if we've hit too many consecutive failures
            if self.consecutive_failures >= self.config.max_consecutive_failures:
                logger.warning(f"Stopping due to {self.consecutive_failures} consecutive failures")
                break
            
            data = self.scrape_url(url)
            if data:
                # Add date information to the scraped data
                data['scraped_date'] = date.strftime('%Y-%m-%d')
                data['date_from_url'] = DateRangeGenerator.extract_date_from_url(url)
                results.append(data)
                successful_scrapes += 1
                logger.info(f"Successfully scraped {date.strftime('%Y-%m-%d')}")
            else:
                logger.warning(f"Failed to scrape {date.strftime('%Y-%m-%d')}")
                
                # If skip_missing_dates is False, we might want to stop on failures
                if not self.config.skip_missing_dates:
                    logger.info("skip_missing_dates is False, continuing anyway...")
        
        logger.info(f"Date range scraping completed:")
        logger.info(f"  Total dates processed: {total_dates}")
        logger.info(f"  Successful scrapes: {successful_scrapes}")
        logger.info(f"  Success rate: {(successful_scrapes/total_dates)*100:.1f}%" if total_dates > 0 else "  No dates processed")
        
        return results
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract page title"""
        title_tag = soup.find('title')
        return title_tag.get_text().strip() if title_tag else ""
    
    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Extract main content, removing scripts and styles"""
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Try to find main content areas
        content_selectors = [
            'main', 'article', '.content', '#content', 
            '.post-content', '.entry-content', '.article-body'
        ]
        
        for selector in content_selectors:
            content = soup.select_one(selector)
            if content:
                return content.get_text(separator=' ', strip=True)
        
        # Fallback to body content
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
    
    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract all links from the page"""
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            absolute_url = urljoin(base_url, href)
            links.append(absolute_url)
        return links
    
    def _extract_articles(self, soup: BeautifulSoup, page_url: str) -> List[Dict]:
        """Extract individual articles from newsletter-style pages like TLDR
        
        This method identifies article cards and extracts structured data from each.
        """
        articles = []
        
        # Look for common article container patterns
        # Based on the TLDR HTML structure you provided
        article_containers = []
        
        # Pattern 1: TLDR-style cards with flex layout
        article_containers.extend(soup.find_all('div', class_=lambda x: x and 'flex' in x and any(
            pattern in str(x) for pattern in ['flex-col', 'flex-row-reverse']
        )))
        
        # Pattern 2: Generic article tags
        article_containers.extend(soup.find_all('article'))
        
        # Pattern 3: Divs with specific article-related classes
        article_containers.extend(soup.find_all('div', class_=lambda x: x and any(
            pattern in str(x).lower() for pattern in ['article', 'post', 'story', 'item']
        )))
        
        # Deduplicate containers
        seen_containers = set()
        unique_containers = []
        for container in article_containers:
            container_html = str(container)[:200]  # Use first 200 chars as fingerprint
            if container_html not in seen_containers:
                seen_containers.add(container_html)
                unique_containers.append(container)
        
        logger.info(f"Found {len(unique_containers)} potential article containers")
        
        for idx, container in enumerate(unique_containers):
            try:
                article = self._parse_article_container(container, page_url, idx)
                if article and article.get('title'):  # Only add if we got a title
                    articles.append(article)
            except Exception as e:
                logger.warning(f"Failed to parse article container {idx}: {e}")
                continue
        
        return articles
    
    def _parse_article_container(self, container: BeautifulSoup, page_url: str, index: int) -> Optional[Dict]:
        """Parse an individual article container and extract structured data"""
        
        # Extract title
        title = None
        title_tags = container.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        for tag in title_tags:
            title_text = tag.get_text().strip()
            if title_text and len(title_text) > 10:  # Avoid very short titles
                title = title_text
                break
        
        if not title:
            return None
        
        # Extract article link
        article_link = None
        links = container.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            # Prefer external links (not internal navigation)
            if href and not href.startswith('#') and not href.startswith('/'):
                article_link = href
                break
            elif href and href.startswith('/'):
                article_link = urljoin(page_url, href)
        
        # Extract image
        image_url = None
        img_tag = container.find('img')
        if img_tag:
            image_url = img_tag.get('src') or img_tag.get('data-src')
            if image_url and not image_url.startswith('http'):
                image_url = urljoin(page_url, image_url)
        
        # Extract description/summary
        description = None
        # Look for divs with description-like classes or line-clamp
        desc_candidates = container.find_all('div', class_=lambda x: x and any(
            pattern in str(x).lower() for pattern in ['line-clamp', 'description', 'summary', 'excerpt']
        ))
        
        for candidate in desc_candidates:
            desc_text = candidate.get_text().strip()
            if desc_text and len(desc_text) > 20:
                description = desc_text
                break
        
        # If no description found, try paragraph tags
        if not description:
            p_tags = container.find_all('p')
            for p in p_tags:
                p_text = p.get_text().strip()
                if p_text and len(p_text) > 20:
                    description = p_text
                    break
        
        # Extract date/category information
        date_info = None
        category = None
        date_spans = container.find_all('span', class_=lambda x: x and 'date' in str(x).lower())
        for span in date_spans:
            text = span.get_text().strip()
            if text:
                # Check if it contains a date pattern or category
                if any(month in text for month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                                                     'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']):
                    date_info = text
                elif '|' in text:
                    parts = text.split('|')
                    date_info = parts[0].strip() if len(parts) > 0 else None
                    category = parts[1].strip() if len(parts) > 1 else None
        
        # Extract reading time
        reading_time = None
        text_content = container.get_text()
        reading_time_match = re.search(r'(\d+)\s*minute\s*read', text_content, re.IGNORECASE)
        if reading_time_match:
            reading_time = f"{reading_time_match.group(1)} minute read"
        
        # Generate unique article ID
        article_id = self._generate_article_id(page_url, title, index)
        
        # Extract full text content (excluding links and buttons)
        full_text = []
        for elem in container.find_all(text=True):
            if elem.parent.name not in ['script', 'style', 'button', 'a']:
                text = elem.strip()
                if text and len(text) > 5:
                    full_text.append(text)
        
        body_text = ' '.join(full_text)
        
        return {
            'record_id': article_id,
            'title': title,
            'link': article_link,
            'description': description,
            'body': body_text,
            'image_url': image_url,
            'date': date_info,
            'category': category,
            'reading_time': reading_time,
            'source_page': page_url,
            'extraction_index': index
        }
    
    def _generate_article_id(self, page_url: str, title: str, index: int) -> str:
        """Generate a unique ID for an article"""
        # Use MD5 hash of URL + title + index for consistent IDs
        content = f"{page_url}|{title}|{index}"
        return hashlib.md5(content.encode()).hexdigest()[:16]

def save_scraped_data(data: Dict, output_dir: str = "scraped_data", debug_mode: bool = False):
    """Save scraped data to file for vector database ingestion"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Create filename from URL
    filename = urlparse(data['url']).path.replace('/', '_').strip('_')
    if not filename:
        filename = urlparse(data['url']).netloc
    filename = f"{filename}_{int(data['scraped_at'])}"
    
    # Save processed content for vector database (original format)
    content_filepath = output_path / f"{filename}.txt"
    
    # Format content for vector database
    content = f"""URL: {data['url']}
Title: {data['title']}
Meta Description: {data['meta_description']}

Content:
{data['content']}

Headings:
{chr(10).join([f"{h['level']}: {h['text']}" for h in data['headings']])}
"""
    
    with open(content_filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    logger.info(f"Saved processed content to {content_filepath}")
    
    # Save structured articles as JSON
    if data.get('articles'):
        json_path = output_path / "articles_json"
        json_path.mkdir(exist_ok=True)
        
        json_filepath = json_path / f"{filename}_articles.json"
        
        # Prepare JSON structure with metadata
        json_data = {
            'page_url': data['url'],
            'page_title': data['title'],
            'scraped_at': data['scraped_at'],
            'scraped_date': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(data['scraped_at'])),
            'article_count': data['article_count'],
            'articles': data['articles']
        }
        
        with open(json_filepath, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(data['articles'])} articles to {json_filepath}")
        
        # Also save as CSV for easy viewing
        save_articles_csv(data['articles'], output_path, filename)
    
    # Save raw HTML in debug mode
    if debug_mode and 'raw_html' in data:
        debug_path = output_path / "debug_html"
        debug_path.mkdir(exist_ok=True)
        
        html_filepath = debug_path / f"{filename}.html"
        with open(html_filepath, 'w', encoding='utf-8') as f:
            f.write(data['raw_html'])
        
        # Save response headers too
        headers_filepath = debug_path / f"{filename}_headers.txt"
        with open(headers_filepath, 'w', encoding='utf-8') as f:
            f.write(f"URL: {data['url']}\n")
            f.write(f"Status Code: {data['status_code']}\n")
            f.write(f"Content Type: {data['content_type']}\n")
            f.write(f"Scraped At: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(data['scraped_at']))}\n\n")
            f.write("Response Headers:\n")
            if 'response_headers' in data:
                for header, value in data['response_headers'].items():
                    f.write(f"{header}: {value}\n")
        
        logger.info(f"Saved debug HTML to {html_filepath}")
        logger.info(f"Saved debug headers to {headers_filepath}")

def save_articles_csv(articles: List[Dict], output_path: Path, filename_prefix: str):
    """Save articles as CSV file"""
    
    if not articles:
        return
    
    csv_path = output_path / "articles_csv"
    csv_path.mkdir(exist_ok=True)
    
    csv_filepath = csv_path / f"{filename_prefix}_articles.csv"
    
    # Define CSV columns
    fieldnames = ['record_id', 'title', 'link', 'description', 'body', 'image_url', 
                  'date', 'category', 'reading_time', 'source_page', 'extraction_index']
    
    with open(csv_filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for article in articles:
            # Write only the fields that exist
            row = {k: article.get(k, '') for k in fieldnames}
            writer.writerow(row)
    
    logger.info(f"Saved articles to CSV: {csv_filepath}")

def save_all_articles_combined(all_results: List[Dict], output_dir: str = "scraped_data"):
    """Combine all articles from multiple scrapes into single files"""
    output_path = Path(output_dir)
    
    all_articles = []
    for result in all_results:
        if result and result.get('articles'):
            all_articles.extend(result['articles'])
    
    if not all_articles:
        logger.warning("No articles found to combine")
        return
    
    # Save combined JSON
    combined_json_path = output_path / "combined_all_articles.json"
    combined_data = {
        'total_articles': len(all_articles),
        'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
        'articles': all_articles
    }
    
    with open(combined_json_path, 'w', encoding='utf-8') as f:
        json.dump(combined_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Saved {len(all_articles)} combined articles to {combined_json_path}")
    
    # Save combined CSV
    
    combined_csv_path = output_path / "combined_all_articles.csv"
    fieldnames = ['record_id', 'title', 'link', 'description', 'body', 'image_url', 
                  'date', 'category', 'reading_time', 'source_page', 'extraction_index']
    
    with open(combined_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for article in all_articles:
            row = {k: article.get(k, '') for k in fieldnames}
            writer.writerow(row)
    
    logger.info(f"Saved combined CSV to {combined_csv_path}")

def save_robots_summary(scraper: ResponsibleScraper, output_dir: str = "scraped_data"):
    """Save a summary of all robots.txt files found"""
    output_path = Path(output_dir)
    summary_path = output_path / "robots_summary.txt"
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("# Robots.txt Summary Report\n")
        f.write(f"# Generated at: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n")
        
        if scraper.robots_checker.robots_content:
            f.write(f"Found robots.txt files for {len(scraper.robots_checker.robots_content)} domains:\n\n")
            for domain, content in scraper.robots_checker.robots_content.items():
                f.write(f"## {domain}\n")
                f.write(f"Content length: {len(content)} characters\n")
                f.write(f"Lines: {len(content.splitlines())}\n")
                
                # Check for common directives
                lines = content.lower().splitlines()
                has_crawl_delay = any('crawl-delay' in line for line in lines)
                has_sitemap = any('sitemap' in line for line in lines)
                disallow_count = sum(1 for line in lines if line.strip().startswith('disallow:'))
                
                f.write(f"Has crawl-delay: {has_crawl_delay}\n")
                f.write(f"Has sitemap: {has_sitemap}\n")
                f.write(f"Disallow rules: {disallow_count}\n\n")
        else:
            f.write("No robots.txt files were found or accessible.\n")
    
    logger.info(f"Saved robots.txt summary to {summary_path}")

def scrape_date_range_example():
    """Example function showing how to scrape a date range"""
    config = ScrapingConfig(
        user_agent="TLDR-Scraper/1.0 (Research; contact@myemail.com)",
        delay_range=(2, 4),
        skip_weekends=True,  # Skip weekends for business content
        skip_missing_dates=True,  # Continue even if some dates are missing
        max_consecutive_failures=3,  # Stop after 3 consecutive failures
        debug_mode=False  # Set to True if you want to save HTML
    )
    
    scraper = ResponsibleScraper(config)
    
    # Example: Scrape TLDR Tech for the last week
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    url_template = "https://tldr.tech/tech/{YYYY-MM-DD}"
    
    results = scraper.scrape_date_range(url_template, start_date, end_date)
    
    # Save all results
    for data in results:
        save_scraped_data(data, debug_mode=config.debug_mode)
    
    # Generate summary report
    save_robots_summary(scraper)
    
    return results

# Example usage
if __name__ == "__main__":
    # Example 1: Regular URL scraping
    config = ScrapingConfig(
        user_agent="MyVectorDB-Scraper/1.0 (Research; contact@myemail.com)",
        delay_range=(2, 4),
        max_pages_per_domain=10,
        debug_mode=True,  # Enable debug mode to save HTML
        save_robots_txt=True  # Save robots.txt files
    )
    
    # URLs to scrape (replace with your target URLs)
    urls_to_scrape = [
        "https://example.com",
        "https://example.com/about",
        # Add your target URLs here
    ]
    
    scraper = ResponsibleScraper(config)
    
    # Regular scraping
    print("=== Regular URL Scraping ===")
    for url in urls_to_scrape:
        data = scraper.scrape_url(url)
        if data:
            save_scraped_data(data, debug_mode=config.debug_mode)
            print(f"Scraped: {data['title']}")
        else:
            print(f"Failed to scrape: {url}")
    
    # Example 2: Date range scraping
    print("\n=== Date Range Scraping ===")
    
    # Configure for date range scraping
    date_config = ScrapingConfig(
        user_agent="TLDR-Scraper/1.0 (Research; datubyte@datubyte.com)",
        delay_range=(3, 5),  # Be extra respectful for bulk scraping
        skip_weekends=True,   # Skip weekends for business content
        skip_missing_dates=True,  # Continue if some dates return 404
        max_consecutive_failures=3,  # Stop after 3 consecutive failures
        debug_mode=False
    )
    
    date_scraper = ResponsibleScraper(date_config)
    
    # Scrape TLDR Tech for a specific date range
    url_template = "https://tldr.tech/ai/{YYYY-MM-DD}"
    start_date = "2025-01-10"  # Start date
    end_date = "2025-09-20"    # End date
    
    print(f"Scraping date range: {start_date} to {end_date}")
    print(f"URL template: {url_template}")
    
    results = date_scraper.scrape_date_range(url_template, start_date, end_date)
    
    # Save all results
    for data in results:
        save_scraped_data(data, debug_mode=date_config.debug_mode)
        if data.get('scraped_date'):
            print(f"Scraped {data['scraped_date']}: {data['title']} - {data['article_count']} articles")
    
    # Save combined articles file
    save_all_articles_combined(results)
    
    # Save robots.txt summary report
    save_robots_summary(date_scraper)
    print(f"\nCompleted! Scraped {len(results)} pages across date range.")
    
    # Print summary statistics
    total_articles = sum(r.get('article_count', 0) for r in results if r)
    print(f"Total articles extracted: {total_articles}")
    print("Check the output directories for results:")
    print("  - articles_json/ for individual JSON files")
    print("  - articles_csv/ for individual CSV files")
    print("  - combined_all_articles.json for all articles combined")
    print("  - combined_all_articles.csv for all articles combined")