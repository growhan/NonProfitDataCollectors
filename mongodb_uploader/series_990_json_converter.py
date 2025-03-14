import os
import sys
import json
import csv
import logging
from datetime import datetime
import io
import zipfile
from typing import Optional
from pathlib import Path
from googleapiclient.http import MediaIoBaseDownload
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from gdrive.gdrive_df_uploader import GoogleDriveUploader
from constants import DataClass

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def build_json_structure(row):
    result = {}
    
    for key, value in row.items():
        if value:  # Only process non-empty values
            keys = key.split('_')  # Split the flattened key into components
            d = result
            for part in keys[:-1]:  # Traverse through the parts, except the last one
                d = d.setdefault(part, {})  # Create nested dictionaries if they don't exist
            d[keys[-1]] = value  # Set the value to the last part of the key
    
    return result

class Series990JSONConverter:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Initialize Google Drive uploader
        self.gdrive = GoogleDriveUploader()
        
        # Setup data directory
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        self.downloads_dir = os.path.join(self.data_dir, 'downloads')
        self.extracted_dir = os.path.join(self.data_dir, 'extracted')
        self.json_dir = os.path.join(self.data_dir, 'json_output')
        self.output_file = os.path.join(self.json_dir, 'series_990_all.jsonl')
        
        # Create directories if they don't exist
        for directory in [self.downloads_dir, self.extracted_dir, self.json_dir]:
            os.makedirs(directory, exist_ok=True)
        logger.info(f"Using data directory at {self.data_dir}")
        logger.info(f"JSON output will be written to {self.output_file}")

    def get_recent_files(self):
        """Get list of most recent files for each year from Google Drive."""
        folder_id = os.getenv('SERIES_990_UPLOAD_FOLDER_ID')
        if not folder_id:
            raise ValueError("SERIES_990_UPLOAD_FOLDER_ID not found in environment variables")

        files = self.gdrive.service.files().list(
            q=f"'{folder_id}' in parents and mimeType='application/zip'",
            fields="files(id, name, createdTime, webContentLink)"
        ).execute().get('files', [])

        # Group files by year and get most recent for each
        files_by_year = {}
        for file in files:
            try:
                # Check if file name matches the expected pattern
                name_parts = file['name'].split('_')
                if len(name_parts) >= 3 and name_parts[0] == 'series' and name_parts[1] == '990':
                    try:
                        year = int(name_parts[2])  # Get year from the third part
                        created_time = datetime.fromisoformat(file['createdTime'].replace('Z', '+00:00'))
                        
                        if year not in files_by_year or created_time > files_by_year[year]['created_time']:
                            files_by_year[year] = {
                                'id': file['id'],
                                'name': file['name'],
                                'created_time': created_time,
                                'download_link': file['webContentLink']
                            }
                    except ValueError:
                        logger.warning(f"Skipping file {file['name']}: Invalid year format")
                else:
                    logger.warning(f"Skipping file {file['name']}: Does not match expected pattern 'series_990_year'")
            except (IndexError, ValueError) as e:
                logger.warning(f"Skipping file {file['name']}: Invalid format")
                continue

        return files_by_year

    def process_csv_file(self, csv_path: str, year: int) -> int:
        """
        Process CSV file line by line and append JSON lines to the main output file.
        Uses buffered writing for better performance.
        """
        total_documents = 0
        batch_size = 10000  # Process records in batches
        current_batch = []
        
        try:
            with open(csv_path, 'r', buffering=1024*1024) as csvfile:  # Increased buffer size
                reader = csv.DictReader(csvfile)
                
                with open(self.output_file, 'a', buffering=1024*1024) as jsonfile:  # Append mode
                    for row_num, row in enumerate(reader, 1):
                        # Process only non-empty values
                        processed_row = {k: (v.strip() if v.strip() != '' else None) 
                                      for k, v in row.items() if v and v.strip()}
                        
                        if processed_row:  # Only process if we have data
                            # Add year to the document
                            document = build_json_structure(processed_row)
                            document['tax_year'] = year
                            current_batch.append(json.dumps(document))
                            
                            # Write batch if we've reached batch size
                            if len(current_batch) >= batch_size:
                                jsonfile.write('\n'.join(current_batch) + '\n')
                                total_documents += len(current_batch)
                                logger.info(f"Batch processed for year {year}: {len(current_batch)} documents (Total: {total_documents})")
                                current_batch = []
                        
                        # Log progress periodically
                        if row_num % 50000 == 0:
                            logger.info(f"Processed {row_num} rows for year {year}")
                    
                    # Write any remaining documents
                    if current_batch:
                        jsonfile.write('\n'.join(current_batch) + '\n')
                        total_documents += len(current_batch)
                        logger.info(f"Final batch processed for year {year}: {len(current_batch)} documents (Total: {total_documents})")
        
        except Exception as e:
            logger.error(f"Error processing CSV file: {str(e)}")
            raise
        
        return total_documents

    def cleanup_files(self, zip_path: Optional[str] = None, csv_path: Optional[str] = None, year_dir: Optional[str] = None):
        """Clean up downloaded and extracted files."""
        try:
            if zip_path and os.path.exists(zip_path):
                os.remove(zip_path)
                logger.info(f"Cleaned up zip file: {zip_path}")
                
            if csv_path and os.path.exists(csv_path):
                os.remove(csv_path)
                logger.info(f"Cleaned up CSV file: {csv_path}")
                
            if year_dir and os.path.exists(year_dir):
                try:
                    os.rmdir(year_dir)
                    logger.info(f"Cleaned up year directory: {year_dir}")
                except OSError:
                    # Directory might not be empty
                    pass
        except Exception as e:
            logger.warning(f"Error during cleanup: {str(e)}")

    def download_and_process_file(self, file_info):
        """Download, unzip, and process a single file."""
        file_id = file_info['id']
        file_name = file_info['name']
        year = int(file_name.split('_')[2])  # Updated to use correct year position
        
        zip_path = os.path.join(self.downloads_dir, file_name)
        year_dir = os.path.join(self.extracted_dir, str(year))
        csv_path = os.path.join(year_dir, f"series_990_{year}.csv")
        
        try:
            logger.info(f"Processing file for year {year}: {file_name}")
            
            # Download file using Google Drive API
            request = self.gdrive.service.files().get_media(fileId=file_id)
            zip_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(zip_buffer, request)
            
            # Download the file with progress tracking
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logger.info(f"Download progress: {int(status.progress() * 100)}%")
            
            # Save the downloaded content to a file
            zip_buffer.seek(0)
            with open(zip_path, 'wb') as f:
                f.write(zip_buffer.read())
            
            logger.info(f"Downloaded zip file to {zip_path}")
            
            # Create year-specific directory for extracted files
            os.makedirs(year_dir, exist_ok=True)
            
            # Unzip file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                csv_name = next(name for name in zip_ref.namelist() if name.endswith('.csv'))
                with zip_ref.open(csv_name) as source, open(csv_path, 'wb') as target:
                    target.write(source.read())
            
            logger.info(f"Extracted CSV to {csv_path}")
            
            # Process the CSV file and write to JSON
            total_documents = self.process_csv_file(csv_path, year)
            logger.info(f"Total documents processed for year {year}: {total_documents}")
            
            # Clean up intermediate files
            self.cleanup_files(zip_path, csv_path, year_dir)
            
            return total_documents
            
        except Exception as e:
            logger.error(f"Error processing file for year {year}: {str(e)}")
            # Clean up files on failure
            self.cleanup_files(zip_path, csv_path, year_dir)
            raise

    def process_specific_year(self, year: int) -> bool:
        """Process a specific year's data file."""
        try:
            files_by_year = self.get_recent_files()
            
            if year not in files_by_year:
                logger.error(f"No file found for year {year}")
                return False
                
            file_info = files_by_year[year]
            self.download_and_process_file(file_info)
            logger.info(f"Successfully processed year {year}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing year {year}: {str(e)}")
            return False

    def process_all_files(self, start_year: Optional[int] = None, end_year: Optional[int] = None):
        """Process files from Google Drive for all years or a specific range."""
        try:
            files_by_year = self.get_recent_files()
            available_years = sorted(files_by_year.keys())
            
            if not available_years:
                logger.error("No files found to process")
                return
                
            start = start_year if start_year in available_years else available_years[0]
            end = end_year if end_year in available_years else available_years[-1]
            
            if start > end:
                logger.error(f"Invalid year range: start year {start} is after end year {end}")
                return
                
            years_to_process = [year for year in available_years if start <= year <= end]
            logger.info(f"Processing years from {start} to {end}")
            logger.info(f"Found {len(years_to_process)} years to process")
            
            for year in years_to_process:
                try:
                    self.download_and_process_file(files_by_year[year])
                    logger.info(f"Successfully processed year {year}")
                except Exception as e:
                    logger.error(f"Error processing year {year}: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Error in process_all_files: {str(e)}")
            raise

def main():
    converter = Series990JSONConverter()
    converter.process_all_files()

if __name__ == "__main__":
    # Example usage:
    converter = Series990JSONConverter()
    
    # Process specific year range:
    converter.process_all_files(start_year=2025, end_year=2025)
    
    # Or process a specific year:
    # converter.process_specific_year(2022)
