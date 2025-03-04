import os
import requests
import zipfile
import pandas as pd
from pathlib import Path
import logging
from dotenv import load_dotenv

# Get the parent directory path
PARENT_DIR = Path(__file__).parent.parent

# Load environment variables from parent directory
load_dotenv(PARENT_DIR / '.env')

class Pub78Downloader:
    PUB_78_DOWNLOAD_URL = os.getenv('IRS_PUB78_URL', 'https://apps.irs.gov/pub/epostcard/data-download-pub78.zip')

    def __init__(self):
        """
        Initialize the Pub78Downloader.
        """
        self.current_dir = Path(os.getenv('OUTPUT_DIR', './'))
        self.current_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logging.basicConfig(level=getattr(logging, log_level))
        self.logger = logging.getLogger(__name__)

    def download_file(self):
        """Download the zip file from the specified URL."""
        try:
            self.logger.info("Downloading Pub 78 file...")
            response = requests.get(self.PUB_78_DOWNLOAD_URL, stream=True)
            response.raise_for_status()
            
            # Get the filename from the URL
            zip_filename = self.PUB_78_DOWNLOAD_URL.split('/')[-1]
            zip_path = self.current_dir / zip_filename
            
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            self.logger.info(f"Download completed. File saved to {zip_path}")
            return zip_path
        except Exception as e:
            self.logger.error(f"Error downloading file: {str(e)}")
            raise

    def find_zip_file(self):
        """Find the zip file in the current directory."""
        try:
            zip_files = list(self.current_dir.glob("*.zip"))
            if not zip_files:
                # If no zip file found, download it
                return self.download_file()
            if len(zip_files) > 1:
                self.logger.warning(f"Multiple zip files found. Using the first one: {zip_files[0]}")
            return zip_files[0]
        except Exception as e:
            self.logger.error(f"Error finding zip file: {str(e)}")
            raise

    def unzip_file(self, zip_path):
        """Unzip the file to the same directory as the zip file."""
        try:
            self.logger.info("Unzipping file...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.current_dir)
            self.logger.info("Unzipping completed")
        except Exception as e:
            self.logger.error(f"Error unzipping file: {str(e)}")
            raise

    def find_unzipped_file(self):
        """Find the unzipped .txt file in the current directory."""
        try:
            txt_files = list(self.current_dir.glob("*.txt"))
            if not txt_files:
                raise FileNotFoundError("No .txt file found after unzipping")
            if len(txt_files) > 1:
                self.logger.warning(f"Multiple .txt files found. Using the first one: {txt_files[0]}")
            return txt_files[0]
        except Exception as e:
            self.logger.error(f"Error finding unzipped file: {str(e)}")
            raise

    def process_to_dataframe(self, unzipped_file):
        """Process the unzipped file and return a pandas DataFrame with specified columns."""
        try:
            self.logger.info("Processing file to DataFrame...")
            # Define the column names
            columns = [
                'EIN',
                'Legal Name',
                'City',
                'State',
                'Country',
                'Deductibility Status'
            ]
            
            # Read the pipe-delimited file with specified column names and EIN as string
            df = pd.read_csv(
                unzipped_file, 
                delimiter='|', 
                names=columns,
                dtype={'EIN': str}
            )
            
            # Rename columns to lowercase
            df.columns = [
                'ein',
                'legal_name',
                'city',
                'state',
                'country',
                'deductibility_status'
            ]
            
            self.logger.info("DataFrame processing completed")
            return df
        except Exception as e:
            self.logger.error(f"Error processing file to DataFrame: {str(e)}")
            raise

    def cleanup(self, zip_path, unzipped_file):
        """Delete the zip and unzipped files."""
        try:
            self.logger.info("Cleaning up temporary files...")
            if zip_path.exists():
                zip_path.unlink()
            if unzipped_file.exists():
                unzipped_file.unlink()
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            raise

    def process(self):
        """Run the complete process of downloading (if needed), unzipping, processing, and cleaning up.
        
        Returns:
            pandas.DataFrame: The processed Pub 78 data
        """
        zip_path = None
        unzipped_file = None
        try:
            # Find or download the zip file
            zip_path = self.find_zip_file()
            
            # Unzip the file
            self.unzip_file(zip_path)
            
            # Find the unzipped .txt file
            unzipped_file = self.find_unzipped_file()
            
            # Process to DataFrame
            df = self.process_to_dataframe(unzipped_file)
            
            self.logger.info("Process completed successfully")
            return df
        except Exception as e:
            self.logger.error(f"Error in processing: {str(e)}")
            raise
        finally:
            # Cleanup will happen regardless of success or failure
            if zip_path or unzipped_file:
                self.cleanup(zip_path, unzipped_file)