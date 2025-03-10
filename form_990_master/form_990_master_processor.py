import logging
from pathlib import Path
import os
from dotenv import load_dotenv
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from form_990_master.form_990_master_downloader import Form990MasterDownloader
from gdrive.gdrive_df_uploader import GoogleDriveUploader
from constants import DataClass

# Get the parent directory path
PARENT_DIR = Path(__file__).parent.parent

# Load environment variables from parent directory
load_dotenv(PARENT_DIR / '.env')

class Form990MasterProcessor:
    """A class that combines downloading Form 990 Master data and uploading it to Google Drive."""
    
    def __init__(self):
        """Initialize the Form990MasterProcessor with its component classes."""
        # Setup logging
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logging.basicConfig(level=getattr(logging, log_level))
        self.logger = logging.getLogger(__name__)
        
        # Initialize component classes
        self.downloader = Form990MasterDownloader()
        self.uploader = GoogleDriveUploader()
        
    def process_and_upload(self):
        """Download Form 990 Master data, process it, and upload to Google Drive.
        
        Returns:
            dict: File metadata from Drive if successful, None otherwise
        """
        try:
            # Step 1: Download and process the data
            self.logger.info("Starting Form 990 Master data download and processing...")
            df = self.downloader.process()
            self.logger.info(f"Successfully processed data. Shape: {df.shape}")
            
            # Step 2: Upload to Google Drive
            self.logger.info("Uploading processed data to Google Drive...")
            result = self.uploader.upload(
                df=df,
                data_class=DataClass.FORM_990_MASTER
            )
            
            if result:
                self.logger.info("Successfully uploaded data to Google Drive")
                return result
            else:
                self.logger.error("Failed to upload data to Google Drive")
                return None
                
        except Exception as e:
            self.logger.error(f"Error in process_and_upload: {str(e)}")
            raise

if __name__ == "__main__":
    # Example usage
    processor = Form990MasterProcessor()
    processor.process_and_upload()
