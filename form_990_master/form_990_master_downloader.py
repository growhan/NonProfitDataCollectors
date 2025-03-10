import os
import requests
import pandas as pd
from pathlib import Path
import logging
from dotenv import load_dotenv

# Get the parent directory path
PARENT_DIR = Path(__file__).parent.parent

# Load environment variables from parent directory
load_dotenv(PARENT_DIR / '.env')

class Form990MasterDownloader:
    # URLs for the four CSV files
    CSV_URLS = [
        'https://www.irs.gov/pub/irs-soi/eo1.csv',
        'https://www.irs.gov/pub/irs-soi/eo2.csv',
        'https://www.irs.gov/pub/irs-soi/eo3.csv',
        'https://www.irs.gov/pub/irs-soi/eo4.csv'
    ]

    def __init__(self):
        """
        Initialize the Form990MasterDownloader.
        """
        self.current_dir = Path(os.getenv('OUTPUT_DIR', './'))
        self.current_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logging.basicConfig(level=getattr(logging, log_level))
        self.logger = logging.getLogger(__name__)

    def download_file(self, url):
        """Download a CSV file from the specified URL."""
        try:
            self.logger.info(f"Downloading file from {url}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Get the filename from the URL
            filename = url.split('/')[-1]
            file_path = self.current_dir / filename
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            self.logger.info(f"Download completed. File saved to {file_path}")
            return file_path
        except Exception as e:
            self.logger.error(f"Error downloading file: {str(e)}")
            raise

    def process_to_dataframe(self, file_path):
        """Process the CSV file and return a pandas DataFrame with EIN as string."""
        try:
            self.logger.info(f"Processing file to DataFrame: {file_path}")
            
            # Read the CSV file with EIN as string
            df = pd.read_csv(
                file_path,
                dtype={'EIN': str},
                encoding='utf-8'
            )
            
            self.logger.info(f"DataFrame processing completed for {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Error processing file to DataFrame: {str(e)}")
            raise

    def cleanup(self, file_path):
        """Delete the downloaded file."""
        try:
            self.logger.info("Cleaning up temporary files...")
            if file_path.exists():
                file_path.unlink()
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            raise

    def process(self):
        """Run the complete process of downloading and processing all CSV files.
        
        Returns:
            pandas.DataFrame: The combined processed data from all CSV files
        """
        all_dfs = []
        downloaded_files = []
        
        try:
            # Process each URL
            for url in self.CSV_URLS:
                # Download the file
                file_path = self.download_file(url)
                downloaded_files.append(file_path)
                
                # Process to DataFrame
                df = self.process_to_dataframe(file_path)
                all_dfs.append(df)
            
            # Combine all DataFrames
            combined_df = pd.concat(all_dfs, ignore_index=True)
            self.logger.info(f"Combined DataFrame shape: {combined_df.shape}")
            
            self.logger.info("Process completed successfully")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"Error in processing: {str(e)}")
            raise
        finally:
            # Cleanup will happen regardless of success or failure
            for file_path in downloaded_files:
                self.cleanup(file_path)
