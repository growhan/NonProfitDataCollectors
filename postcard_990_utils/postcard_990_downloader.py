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

class PostcardDownloader:
    POSTCARD_DOWNLOAD_URL = os.getenv('IRS_990_POSTCARD_URL', 'https://apps.irs.gov/pub/epostcard/data-download-epostcard.zip')

    def __init__(self):
        """
        Initialize the PostcardDownloader.
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
            self.logger.info("Downloading 990 Postcard file...")
            response = requests.get(self.POSTCARD_DOWNLOAD_URL, stream=True)
            response.raise_for_status()
            
            # Get the filename from the URL
            zip_filename = self.POSTCARD_DOWNLOAD_URL.split('/')[-1]
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
                "EIN", "Tax Year", "Organization Name", "Gross receipts not greater than", 
                "Organization has terminated", "Tax Period Begin Date", "Tax Period End Date", 
                "Website URL", "Principal Officer's Name", "Principal Officer's Address Line 1",
                "Principal Officer's Address Line 2", "Principal Officer's Address City",
                "Principal Officer's Address Province", "Principal Officer's Address State",
                "Principal Officer's Address Zip Code", "Principal Officer's Address Country",
                "Organization Mailing Address Line 1", "Organization Mailing Address Line 2",
                "Organization Mailing Address City", "Organization Mailing Address Province",
                "Organization Mailing Address State", "Organization Mailing Address Postal Code",
                "Organization Mailing Address Country", "Organization's Doing Business as Name 1",
                "Organization's Doing Business as Name 2", "Organization's Doing Business as Name 3"
            ]
            
            # First, read the file line by line to identify problematic rows
            self.logger.info("Scanning file for problematic rows...")
            with open(unzipped_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    fields = line.strip().split('|')
                    if len(fields) != len(columns):
                        self.logger.warning(f"\nProblematic row at line {line_num}:")
                        self.logger.warning(f"Expected {len(columns)} columns, found {len(fields)} columns")
                        self.logger.warning("Row content:")
                        self.logger.warning(line.strip())
                        self.logger.warning("Fields found:")
                        for i, field in enumerate(fields):
                            self.logger.warning(f"Field {i + 1}: {field}")
                        self.logger.warning("-" * 80)
            
            # Read the pipe-delimited file with specified column names and EIN as string
            df = pd.read_csv(
                unzipped_file, 
                delimiter='|', 
                names=columns,
                dtype={
                    'EIN': str,
                    'Tax Year': str,
                    'Tax Period Begin Date': str,
                    'Tax Period End Date': str,
                    'Principal Officer\'s Address Zip Code': str,
                    'Organization Mailing Address Postal Code': str
                },
                on_bad_lines='warn',  # Warn about bad lines instead of failing
                quoting=3,  # QUOTE_NONE: Don't use quotes
                quotechar=None,  # No quote character
                escapechar=None,  # No escape character
                encoding='utf-8',  # Specify encoding explicitly
                engine='python'  # Use Python engine which is more flexible with malformed data
            )
            
            # Check for and report any columns beyond expected
            if len(df.columns) > len(columns):
                extra_cols = set(df.columns) - set(columns)
                self.logger.warning(f"\nFound unexpected columns: {extra_cols}")
                self.logger.warning("Sample rows with values in extra columns:")
                for col in extra_cols:
                    non_null_rows = df[df[col].notna()]
                    if not non_null_rows.empty:
                        self.logger.warning(f"\nRows with values in column '{col}':")
                        self.logger.warning(non_null_rows[['EIN', col]].head())
            
            # Drop any extra columns that might have been created
            df = df[columns]
            
            # Rename columns to lowercase and snake_case
            df.columns = [
                'ein', 'tax_year', 'organization_name', 'gross_receipts_not_greater_than',
                'organization_has_terminated', 'tax_period_begin_date', 'tax_period_end_date',
                'website_url', 'principal_officer_name', 'principal_officer_address_line_1',
                'principal_officer_address_line_2', 'principal_officer_address_city',
                'principal_officer_address_province', 'principal_officer_address_state',
                'principal_officer_address_zip_code', 'principal_officer_address_country',
                'organization_mailing_address_line_1', 'organization_mailing_address_line_2',
                'organization_mailing_address_city', 'organization_mailing_address_province',
                'organization_mailing_address_state', 'organization_mailing_address_postal_code',
                'organization_mailing_address_country', 'organization_doing_business_as_name_1',
                'organization_doing_business_as_name_2', 'organization_doing_business_as_name_3'
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
            pandas.DataFrame: The processed 990 Postcard data
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
            print(df.head())
            return df
        except Exception as e:
            self.logger.error(f"Error in processing: {str(e)}")
            raise
        finally:
            # Cleanup will happen regardless of success or failure
            if zip_path or unzipped_file:
                self.cleanup(zip_path, unzipped_file)
