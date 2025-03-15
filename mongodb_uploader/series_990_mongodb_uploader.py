import os
import sys
import tempfile
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, BulkWriteError
from pymongo.operations import InsertOne
from dotenv import load_dotenv
import zipfile
from datetime import datetime
import logging
from pathlib import Path
import io
import csv
from googleapiclient.http import MediaIoBaseDownload
from typing import Optional, Dict, Any

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

class Series990MongoDBUploader:
    def __init__(self):
        # Reload the .env file and override any existing environment variables
        load_dotenv(override=True)
        
        # Get MongoDB credentials
        self.mongo_host = os.getenv('MONGODB_HOST')
        self.mongo_port = os.getenv('MONGODB_PORT')
        self.mongo_user = os.getenv('MONGODB_USER')
        self.mongo_pass = os.getenv('MONGODB_PASSWORD')
        self.mongo_db = os.getenv('MONGODB_DATABASE')
        self.series990_collection = os.getenv('MONGODB_SERIES_990_COLLECTION', 'series_990')
        
        # Validate MongoDB configuration
        if not all([self.mongo_host, self.mongo_port, self.mongo_user, self.mongo_pass, self.mongo_db]):
            raise ValueError("Missing required MongoDB configuration in .env file")
        
        # Initialize MongoDB connection
        try:
            # Try connecting with credentials
            self.mongo_uri = f"mongodb://{self.mongo_user}:{self.mongo_pass}@{self.mongo_host}:{self.mongo_port}/{self.mongo_db}?authSource={self.mongo_db}"
            self.client = MongoClient(self.mongo_uri)
            
            # Test the connection
            self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB with authentication")
            
            # Check if the connected user has readWrite permissions
            user_info = self.client[self.mongo_db].command("usersInfo", {"user": self.mongo_user, "db": self.mongo_db})
            if user_info.get("users"):
                roles = user_info["users"][0].get("roles", [])
                if not any(role.get("role") == "readWrite" for role in roles):
                    raise Exception(f"The user {self.mongo_user} does not have readWrite permissions on database {self.mongo_db}")
            else:
                raise Exception("Unable to retrieve user information to verify readWrite permissions")
            
            # Set the database and collection
            self.db = self.client[self.mongo_db]
            self.collection = self.db[self.series990_collection]
            
        except (ConnectionFailure, OperationFailure) as e:
            logger.error(f"MongoDB Authentication failed: {str(e)}")
            logger.info("Attempting to connect without authentication...")
            try:
                # Try connecting without credentials
                self.mongo_uri = f"mongodb://{self.mongo_host}:{self.mongo_port}/{self.mongo_db}"
                self.client = MongoClient(self.mongo_uri)
                self.client.admin.command('ping')
                logger.info("Successfully connected to MongoDB without authentication")
                
                # Set the database and collection
                self.db = self.client[self.mongo_db]
                self.collection = self.db[self.series990_collection]
                
            except Exception as e2:
                logger.error(f"All MongoDB connection attempts failed: {str(e2)}")
                raise
        except Exception as e:
            logger.error(f"Unexpected MongoDB connection error: {str(e)}")
            raise
        
        # Initialize Google Drive uploader
        self.gdrive = GoogleDriveUploader()
        
        # Setup data directory
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        self.downloads_dir = os.path.join(self.data_dir, 'downloads')
        self.extracted_dir = os.path.join(self.data_dir, 'extracted')
        
        # Create directories if they don't exist
        os.makedirs(self.downloads_dir, exist_ok=True)
        os.makedirs(self.extracted_dir, exist_ok=True)
        logger.info(f"Using data directory at {self.data_dir}")

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
        Process CSV file line by line and insert into MongoDB using bulk operations.
        Uses unordered writes for better performance.
        """
        total_documents = 0
        batch_size = 10000  # Increased batch size for better performance
        bulk_operations = []
        
        try:
            with open(csv_path, 'r', buffering=1024*1024) as csvfile:  # Increased buffer size
                reader = csv.DictReader(csvfile)
                
                for row_num, row in enumerate(reader, 1):
                    # Process only non-empty values
                    processed_row = {k: (v.strip() if v.strip() != '' else None) 
                                  for k, v in row.items() if v and v.strip()}
                    
                    if processed_row:  # Only process if we have data
                        document = build_json_structure(processed_row)
                        bulk_operations.append(InsertOne(document))
                        
                        # Process batch if we've reached batch size
                        if len(bulk_operations) >= batch_size:
                            try:
                                result = self.collection.bulk_write(bulk_operations, ordered=False)
                                total_documents += result.inserted_count
                                logger.info(f"Batch processed for year {year}: {result.inserted_count} documents (Total: {total_documents})")
                            except BulkWriteError as bwe:
                                logger.warning(f"Some documents failed to insert: {bwe.details['writeErrors'][:3]}")
                                total_documents += bwe.details['nInserted']
                            finally:
                                bulk_operations = []
                    
                    # Log progress periodically
                    if row_num % 50000 == 0:
                        logger.info(f"Processed {row_num} rows for year {year}")
                
                # Process any remaining documents
                if bulk_operations:
                    try:
                        result = self.collection.bulk_write(bulk_operations, ordered=False)
                        total_documents += result.inserted_count
                        logger.info(f"Final batch processed for year {year}: {result.inserted_count} documents (Total: {total_documents})")
                    except BulkWriteError as bwe:
                        logger.warning(f"Some documents failed to insert in final batch: {bwe.details['writeErrors'][:3]}")
                        total_documents += bwe.details['nInserted']
        
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
        year = int(file_name.split('_')[2])
        
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
            
            # Process the CSV file
            total_documents = self.process_csv_file(csv_path, year)
            logger.info(f"Total documents processed for year {year}: {total_documents}")
            
            # Clean up files after successful processing
            self.cleanup_files(zip_path, csv_path, year_dir)
            
            return total_documents
            
        except Exception as e:
            logger.error(f"Error processing file for year {year}: {str(e)}")
            # Clean up files on failure
            self.cleanup_files(zip_path, csv_path, year_dir)
            raise

    def process_specific_year(self, year: int) -> bool:
        """
        Process a specific year's data file.
        
        Args:
            year: The year to process (e.g., 2022)
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            # Get all files and find the one for the specified year
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
        """
        Process files from Google Drive. Can process all years or a specific range of years.
        
        Args:
            start_year: Optional start year (e.g., 2020). If None, starts from earliest available year.
            end_year: Optional end year (e.g., 2022). If None, processes up to latest available year.
        """
        try:
            # Get list of most recent files
            files_by_year = self.get_recent_files()
            available_years = sorted(files_by_year.keys())
            
            if not available_years:
                logger.error("No files found to process")
                return
                
            # Determine year range
            start = start_year if start_year in available_years else available_years[0]
            end = end_year if end_year in available_years else available_years[-1]
            
            # Validate year range
            if start > end:
                logger.error(f"Invalid year range: start year {start} is after end year {end}")
                return
                
            # Filter years to process
            years_to_process = [year for year in available_years if start <= year <= end]
            logger.info(f"Processing years from {start} to {end}")
            logger.info(f"Found {len(years_to_process)} years to process")
            
            # Process each file in the range
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
        finally:
            # Close MongoDB connection
            self.client.close()
            logger.info("Closed MongoDB connection")

def main():
    uploader = Series990MongoDBUploader()
    uploader.process_all_files()

if __name__ == "__main__":
    # Example usage:
    # Process all years:
    # main()
    
    # Process specific year range:
    uploader = Series990MongoDBUploader()
    
    # Process all years (default behavior)
    # uploader.process_all_files()
    
    # Process specific year range:
    uploader.process_all_files(start_year=2025, end_year=2025)
    
    # Process from specific year to latest:
    # uploader.process_all_files(start_year=2020)
    
    # Process from earliest to specific year:
    # uploader.process_all_files(end_year=2022)
