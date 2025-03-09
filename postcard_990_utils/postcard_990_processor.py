import logging
from pathlib import Path
import os
from dotenv import load_dotenv
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from postcard_990_utils.postcard_990_downloader import PostcardDownloader
from gdrive.gdrive_df_uploader import GoogleDriveUploader
from constants import DataClass

# Get the parent directory path
PARENT_DIR = Path(__file__).parent.parent

# Load environment variables from parent directory
load_dotenv(PARENT_DIR / '.env')

class PostcardProcessor:
    """A class that combines downloading 990 Postcard data and uploading it to Google Drive."""
    
    def __init__(self):
        """Initialize the PostcardProcessor with its component classes."""
        # Setup logging
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logging.basicConfig(level=getattr(logging, log_level))
        self.logger = logging.getLogger(__name__)
        
        # Initialize component classes
        self.downloader = PostcardDownloader()
        self.uploader = GoogleDriveUploader()
        
    def process_and_upload(self):
        """Download 990 Postcard data, process it, and upload to Google Drive.
        
        Returns:
            dict: File metadata from Drive if successful, None otherwise
        """
        try:
            # Step 1: Download and process the data
            self.logger.info("Starting 990 Postcard data download and processing...")
            df = self.downloader.process()
            self.logger.info(f"Successfully processed data. Shape: {df.shape}")
            
            # Step 2: Upload to Google Drive
            self.logger.info("Uploading processed data to Google Drive...")
            result = self.uploader.upload(
                df=df,
                data_class=DataClass.POSTCARD_990
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
    processor = PostcardProcessor()
    processor.process_and_upload()
