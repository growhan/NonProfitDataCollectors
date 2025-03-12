import logging
from pathlib import Path
import os
from dotenv import load_dotenv
import sys
import zipfile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from series_990_download_link_scraper import Series990Scraper
from series_990_downloader import Series990Downloader
from gdrive.gdrive_df_uploader import GoogleDriveUploader
from constants import DataClass

# Get the parent directory path
PARENT_DIR = Path(__file__).parent.parent

# Load environment variables from parent directory
load_dotenv(PARENT_DIR / '.env')

class Series990Processor:
    """A class that orchestrates downloading Series 990 data and uploading it to Google Drive."""
    
    def __init__(self):
        """Initialize the Series990Processor with its component classes."""
        # Setup logging
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logging.basicConfig(level=getattr(logging, log_level))
        self.logger = logging.getLogger(__name__)
        
        # Initialize component classes
        self.scraper = Series990Scraper()
        self.downloader = Series990Downloader()
        self.uploader = GoogleDriveUploader()
    
    def compress_file(self, file_path: Path) -> Path:
        """Compress a file using ZIP format.
        
        Args:
            file_path: Path to the file to compress
            
        Returns:
            Path: Path to the compressed file
        """
        compressed_path = file_path.parent / f"{file_path.stem}.zip"
        try:
            self.logger.info(f"Compressing file {file_path}...")
            with zipfile.ZipFile(compressed_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.write(file_path, file_path.name)
            self.logger.info(f"Successfully compressed file to {compressed_path}")
            return compressed_path
        except Exception as e:
            self.logger.error(f"Error compressing file: {str(e)}")
            if compressed_path.exists():
                compressed_path.unlink()
            raise
    
    def process_and_upload(self, start_year: str = None, end_year: str = None):
        """Download Series 990 data, process it, and upload to Google Drive for specified years.
        
        Args:
            start_year (str, optional): Start year to process (inclusive)
            end_year (str, optional): End year to process (inclusive)
        
        Returns:
            dict: Dictionary mapping years to upload results
        """
        try:
            # Step 1: Get all download links by year
            self.logger.info("Fetching download links...")
            links_by_year = self.scraper.get_download_links()
            
            if not links_by_year:
                self.logger.error("No download links found")
                return {}
            
            # Filter years if specified
            if start_year or end_year:
                filtered_links = {}
                for year in links_by_year:
                    if ((not start_year or year >= start_year) and 
                        (not end_year or year <= end_year)):
                        filtered_links[year] = links_by_year[year]
                links_by_year = filtered_links
            
            results = {}
            
            # Process each year
            for year, urls in links_by_year.items():
                try:
                    self.logger.info(f"\nProcessing year {year}...")
                    
                    # Step 2: Download and process all files for the year
                    csv_path = self.downloader.process_year_data(year, urls)
                    if not csv_path:
                        self.logger.error(f"Failed to process data for year {year}")
                        continue
                    
                    # Step 3: Compress the CSV file
                    compressed_path = None
                    try:
                        compressed_path = self.compress_file(csv_path)
                        
                        # Step 4: Upload compressed file to Google Drive
                        self.logger.info(f"Uploading compressed data for year {year} to Google Drive...")
                        result = self.uploader.upload(
                            data=str(compressed_path),
                            data_class=DataClass.SERIES_990
                        )
                        
                        if result:
                            self.logger.info(f"Successfully uploaded data for year {year}")
                            results[year] = result
                        else:
                            self.logger.error(f"Failed to upload data for year {year}")
                            
                    finally:
                        # Step 5: Cleanup both CSV and ZIP files
                        if csv_path.exists():
                            csv_path.unlink()
                        if compressed_path and compressed_path.exists():
                            compressed_path.unlink()
                    
                except Exception as e:
                    self.logger.error(f"Error processing year {year}: {str(e)}")
                    continue
            
            return results
                
        except Exception as e:
            self.logger.error(f"Error in process_and_upload: {str(e)}")
            raise

if __name__ == "__main__":
    # Example usage
    processor = Series990Processor()
    
    # Process all years
    processor.process_and_upload()
    
    # Or process specific years
    #processor.process_and_upload(start_year="2025", end_year="2025")
