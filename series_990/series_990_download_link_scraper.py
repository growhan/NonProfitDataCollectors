import requests
from bs4 import BeautifulSoup
from typing import List, Dict
import logging
import re

class Series990Scraper:
    """
    A class to scrape Form 990 XML download links from the IRS website.
    """
    
    def __init__(self):
        self.base_url = "https://www.irs.gov/charities-non-profits/form-990-series-downloads"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def extract_year(self, url: str) -> str:
        """
        Extract the year from a download URL.
        
        Args:
            url (str): The download URL
            
        Returns:
            str: The year if found, otherwise None
        """
        # Try to find year in different formats
        year_patterns = [
            r'/(\d{4})_TEOS_XML',  # Matches TEOS format
            r'/(\d{4})/download990xml',  # Matches old format
            r'download990xml_(\d{4})',  # Matches alternative old format
        ]
        
        for pattern in year_patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    def get_download_links(self) -> Dict[str, List[str]]:
        """
        Scrapes the IRS website for Form 990 XML download links.
        Looks for both older style links containing 'download990xml' and 
        newer style links containing 'TEOS'.
        
        Returns:
            Dict[str, List[str]]: A dictionary mapping years to lists of download URLs
        """
        try:
            # Make request to the IRS website
            response = requests.get(self.base_url, headers=self.headers)
            response.raise_for_status()
            
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Dictionary to store links by year
            links_by_year: Dict[str, List[str]] = {}
            
            # Find all links that contain either 'download990xml' or 'TEOS'
            for link in soup.find_all('a', href=True):
                href = link['href'].lower()
                if 'download990xml' in href or ('teos' in href and '.zip' in href):
                    # Make sure we have absolute URLs
                    full_url = link['href']
                    if not href.startswith('http'):
                        base = "https://apps.irs.gov"
                        full_url = f"{base}{link['href']}"
                    
                    # Extract year and group links
                    year = self.extract_year(full_url)
                    if year:
                        if year not in links_by_year:
                            links_by_year[year] = []
                        links_by_year[year].append(full_url)
            
            # Sort years in descending order and remove duplicates within each year
            sorted_links: Dict[str, List[str]] = {}
            for year in sorted(links_by_year.keys(), reverse=True):
                sorted_links[year] = list(dict.fromkeys(links_by_year[year]))
            
            total_links = sum(len(links) for links in sorted_links.values())
            self.logger.info(f"Found {total_links} download links across {len(sorted_links)} years")
            return sorted_links
            
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from IRS website: {str(e)}")
            return {}
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            return {}

if __name__ == "__main__":
    # Example usage
    scraper = Series990Scraper()
    links_by_year = scraper.get_download_links()
    
    print("\nForm 990 XML Download Links by Year:")
    for year, links in links_by_year.items():
        print(f"\n{year}:")
        for link in links:
            print(f"  - {link}")
