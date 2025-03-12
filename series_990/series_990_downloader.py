import os
import requests
import zipfile
import pandas as pd
from pathlib import Path
import logging
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
import re
import tempfile
import shutil
from typing import Dict, List, Optional, Set, Tuple
import json
import csv
from datetime import datetime

# Get the parent directory path
PARENT_DIR = Path(__file__).parent.parent

# Load environment variables from parent directory
load_dotenv(PARENT_DIR / '.env')

class Series990Downloader:
    def __init__(self):
        """Initialize the Series990Downloader."""
        # Create data directory in parent directory
        self.current_dir = PARENT_DIR / 'data'
        self.current_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logging.basicConfig(level=getattr(logging, log_level))
        self.logger = logging.getLogger(__name__)

    def download_file(self, url: str, year: str) -> Optional[Path]:
        """Download a zip file from the specified URL."""
        try:
            self.logger.info(f"Downloading Series 990 file for year {year} from {url}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Create a temporary directory for this year if it doesn't exist
            year_dir = self.current_dir / str(year)
            year_dir.mkdir(parents=True, exist_ok=True)
            
            # Get the filename from the URL
            zip_filename = url.split('/')[-1]
            zip_path = year_dir / zip_filename
            
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            self.logger.info(f"Download completed. File saved to {zip_path}")
            return zip_path
        except Exception as e:
            self.logger.error(f"Error downloading file: {str(e)}")
            return None

    def unzip_file(self, zip_path: Path) -> Optional[Path]:
        """Unzip the file to a temporary directory."""
        try:
            self.logger.info(f"Unzipping file {zip_path}...")
            # Create a temporary directory for extracted files
            extract_dir = zip_path.parent / f"extract_{zip_path.stem}"
            extract_dir.mkdir(parents=True, exist_ok=True)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            self.logger.info(f"Unzipping completed to {extract_dir}")
            return extract_dir
        except Exception as e:
            self.logger.error(f"Error unzipping file: {str(e)}")
            return None

    @staticmethod
    def flatten_xml(xml_string: str, filename: str) -> dict:
        """Flattens an XML string into a dictionary and adds the filename."""
        root = ET.fromstring(xml_string)
        flat_data = {'fileName': filename}  # Add filename to the flattened data

        def _flatten(element, prefix=''):
            for child in element:
                tag = re.sub(r'\{.*\}', '', child.tag)
                key = prefix + tag if prefix else tag
                if len(child) == 0:  # Leaf node
                    flat_data[key] = child.text
                else:
                    _flatten(child, key + '_')

        _flatten(root)
        return flat_data

    def process_xml_files(self, extract_dir: Path, jsonl_file, all_fields: Set[str]) -> int:
        """Process all XML files in the extracted directory and write as JSON lines.
        
        Args:
            extract_dir: Directory containing XML files
            jsonl_file: File object to write JSON lines to
            all_fields: Set to collect all field names
            
        Returns:
            int: Number of records processed
        """
        try:
            self.logger.info(f"Processing XML files in {extract_dir}...")
            records_processed = 0
            
            # Recursively find all XML files
            for xml_file in extract_dir.rglob('*.xml'):
                try:
                    with open(xml_file, 'r', encoding='utf-8') as f:
                        xml_content = f.read()
                    flat_dict = self.flatten_xml(xml_content, xml_file.name)
                    
                    # Update all_fields set with new fields
                    all_fields.update(flat_dict.keys())
                    
                    # Write dictionary as JSON line
                    json.dump(flat_dict, jsonl_file)
                    jsonl_file.write('\n')
                    
                    records_processed += 1
                    if records_processed % 1000 == 0:
                        self.logger.info(f"Processed {records_processed} records")
                    
                except Exception as e:
                    self.logger.error(f"Error processing {xml_file}: {str(e)}")
                    continue
            
            self.logger.info(f"Processed {records_processed} XML files total")
            return records_processed
        except Exception as e:
            self.logger.error(f"Error processing XML files: {str(e)}")
            return 0

    def convert_jsonl_to_csv(self, jsonl_path: Path, csv_path: Path, fieldnames: List[str]) -> bool:
        """Convert JSONL file to CSV with all collected fields."""
        try:
            self.logger.info(f"Converting JSONL to CSV with {len(fieldnames)} fields...")
            
            with open(jsonl_path, 'r', encoding='utf-8') as jsonl_file, \
                 open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
                
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                
                for line in jsonl_file:
                    record = json.loads(line.strip())
                    writer.writerow(record)
            
            self.logger.info("Successfully converted JSONL to CSV")
            return True
        except Exception as e:
            self.logger.error(f"Error converting JSONL to CSV: {str(e)}")
            return False

    def cleanup(self, zip_path: Optional[Path], extract_dir: Optional[Path]):
        """Delete the zip and extracted files."""
        try:
            self.logger.info("Cleaning up temporary files...")
            if zip_path and zip_path.exists():
                zip_path.unlink()
            if extract_dir and extract_dir.exists():
                shutil.rmtree(extract_dir)
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

    def process_year_data(self, year: str, urls: List[str]) -> Optional[Path]:
        """Process all data for a specific year."""
        try:
            self.logger.info(f"Processing data for year {year}")
            
            # Create year directory if it doesn't exist
            year_dir = self.current_dir / str(year)
            year_dir.mkdir(parents=True, exist_ok=True)
            
            # Define output paths with current date and time
            current_datetime = datetime.now().strftime('%Y-%m-%d_%H%M%S')
            output_csv = year_dir / f"series_990_{year}_data_{current_datetime}.csv"
            temp_jsonl = year_dir / f"series_990_{year}_temp_{current_datetime}.jsonl"
            
            # Ensure parent directory exists for both files
            output_csv.parent.mkdir(parents=True, exist_ok=True)
            temp_jsonl.parent.mkdir(parents=True, exist_ok=True)
            
            total_records = 0
            all_fields = set(['fileName'])  # Initialize with filename field
            
            # Process each URL and write to temporary JSONL
            with open(temp_jsonl, 'w', encoding='utf-8') as jsonl_file:
                for url in urls:
                    zip_path = None
                    extract_dir = None
                    try:
                        # Download and process each zip file
                        zip_path = self.download_file(url, year)
                        if not zip_path:
                            continue
                            
                        extract_dir = self.unzip_file(zip_path)
                        if not extract_dir:
                            continue
                        
                        # Process XML files and write to JSONL
                        records = self.process_xml_files(extract_dir, jsonl_file, all_fields)
                        total_records += records
                            
                    finally:
                        # Cleanup after processing each zip file
                        self.cleanup(zip_path, extract_dir)
            
            if total_records > 0:
                # Convert JSONL to CSV using all collected fields
                self.logger.info(f"Converting {total_records} records to CSV...")
                fieldnames = sorted(list(all_fields))
                if self.convert_jsonl_to_csv(temp_jsonl, output_csv, fieldnames):
                    # Remove temporary JSONL
                    temp_jsonl.unlink()
                    self.logger.info(f"Saved processed data to {output_csv}")
                    return output_csv
            
            # Clean up temporary files if no records or conversion failed
            if temp_jsonl.exists():
                temp_jsonl.unlink()
            return None
            
        except Exception as e:
            self.logger.error(f"Error processing year {year}: {str(e)}")
            # Clean up temporary files in case of error
            if 'temp_jsonl' in locals() and temp_jsonl.exists():
                temp_jsonl.unlink()
            return None
