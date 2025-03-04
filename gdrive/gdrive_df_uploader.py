import os
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
import pandas as pd
import io
import sys
from datetime import datetime
import zipfile
import tempfile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from constants import DataClass

class GoogleDriveUploader:
    # Define scopes for Google Drive API
    SCOPES = [
        'https://www.googleapis.com/auth/drive.metadata.readonly',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive.metadata'  # Added for sharing functionality
    ]

    def __init__(self):
        # Get the parent directory path and load environment variables
        self.parent_dir = Path(__file__).parent.parent
        load_dotenv(self.parent_dir / '.env')
        self.service = self._get_drive_service()

    def _get_drive_service(self):
        """Get an authorized Google Drive service instance."""
        try:
            # Debug: Print the current working directory and .env file path
            print(f"Current working directory: {os.getcwd()}")
            env_path = self.parent_dir / '.env'
            print(f"Loading .env from: {env_path}")
            
            # Explicitly load .env file
            load_dotenv(env_path, override=True)
            
            # Get the private key and properly format it
            private_key = os.getenv('GOOGLE_PRIVATE_KEY')
            if not private_key:
                raise ValueError("GOOGLE_PRIVATE_KEY not found in environment variables")
                
            # Debug: Print the first and last few characters of the key
            print(f"Private key starts with: {private_key[:10]}...")
            print(f"Private key ends with: ...{private_key[-10:]}")
            
            if private_key.startswith('"') and private_key.endswith('"'):
                private_key = private_key[1:-1]  # Remove surrounding quotes
            private_key = private_key.replace('\\n', '\n')  # Replace string \n with actual newlines
            
            # Create service account info dictionary
            service_account_info = {
                "type": os.getenv('GOOGLE_TYPE'),
                "project_id": os.getenv('GOOGLE_PROJECT_ID'),
                "private_key_id": os.getenv('GOOGLE_PRIVATE_KEY_ID'),
                "private_key": private_key,
                "client_email": os.getenv('GOOGLE_CLIENT_EMAIL'),
                "client_id": os.getenv('GOOGLE_CLIENT_ID'),
                "auth_uri": os.getenv('GOOGLE_AUTH_URI'),
                "token_uri": os.getenv('GOOGLE_TOKEN_URI'),
                "auth_provider_x509_cert_url": os.getenv('GOOGLE_AUTH_PROVIDER_CERT_URL'),
                "client_x509_cert_url": os.getenv('GOOGLE_CLIENT_X509_CERT_URL')
            }
            
            # Verify all required fields are present
            missing_fields = [k for k, v in service_account_info.items() if not v]
            if missing_fields:
                raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
            
            # Create credentials
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=self.SCOPES
            )
            
            return build('drive', 'v3', credentials=credentials)
        except Exception as e:
            print(f"Error creating Drive service: {str(e)}")
            print(f"Exception type: {type(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return None

    def share_file(self, file_id, email='npearch24@gmail.com', role='reader'):
        """Share a file with a specific email address."""
        try:
            user_permission = {
                'type': 'user',
                'role': role,
                'emailAddress': email
            }
            
            permission = self.service.permissions().create(
                fileId=file_id,
                body=user_permission,
                fields='id'
            ).execute()
            
            print(f"File shared with {email}")
            return permission
        except HttpError as error:
            print(f"Error sharing file with {email}: {error}")
            return None

    @staticmethod
    def compress_dataframe_to_zip(df: pd.DataFrame, csv_filename: str) -> tuple[io.BytesIO, str]:
        """Compress a DataFrame to a ZIP file containing a CSV."""
        zip_buffer = io.BytesIO()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = os.path.join(temp_dir, csv_filename)
            df.to_csv(csv_path, index=False)
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.write(csv_path, csv_filename)
        
        zip_buffer.seek(0)
        zip_filename = csv_filename.rsplit('.', 1)[0] + '.zip'
        
        return zip_buffer, zip_filename

    def upload_dataframe(self, df, filename, folder_id=None):
        """Upload a pandas DataFrame as a compressed ZIP file to Google Drive."""
        try:
            if not self.service:
                print("Failed to get Drive service")
                return None
            
            zip_buffer, zip_filename = self.compress_dataframe_to_zip(df, filename)
            
            file_metadata = {
                'name': zip_filename,
                'mimeType': 'application/zip'
            }
            
            if folder_id:
                file_metadata['parents'] = [folder_id]
            
            media = MediaIoBaseUpload(
                zip_buffer,
                mimetype='application/zip',
                resumable=True
            )
            
            print(f"\nCompressed file size: {zip_buffer.getbuffer().nbytes / 1024:.2f} KB")
            
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink'
            ).execute()
            
            print(f"\nFile uploaded successfully: {file.get('name')}")
            print(f"File ID: {file.get('id')}")
            print(f"File link: {file.get('webViewLink')}")
            
            self.share_file(file.get('id'))
            
            return file
        except HttpError as error:
            print(f"An error occurred: {error}")
            return None
        finally:
            if 'zip_buffer' in locals():
                zip_buffer.close()

    @staticmethod
    def get_upload_folder_id(data_class: str) -> str:
        """Get the appropriate folder ID based on the data class."""
        return os.getenv(DataClass.get_env_var_name(data_class))

    def list_shared_folders(self):
        """List all shared folders in Google Drive."""
        if not self.service:
            print("Failed to get Drive service")
            return None
        
        try:
            query = "mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name, webViewLink)'
            ).execute()
            
            folders = results.get('files', [])
            
            if not folders:
                print("No shared folders found.")
                return {}
                
            print("\nAvailable shared folders:")
            print("-" * 50)
            folder_dict = {}
            for folder in folders:
                folder_dict[folder['name']] = {
                    'id': folder['id'],
                    'link': folder.get('webViewLink', 'N/A')
                }
                print(f"Folder: {folder['name']}")
                print(f"ID: {folder['id']}")
                print(f"Link: {folder.get('webViewLink', 'N/A')}")
                print("-" * 50)
                
            return folder_dict
            
        except HttpError as error:
            print(f"An error occurred while listing folders: {error}")
            return None

    def upload(self, df: pd.DataFrame, data_class: str = None):
        """Upload a pandas DataFrame to Google Drive based on the specified data class."""
        try:
            if not data_class:
                print("Error: data_class parameter is required")
                return None
                
            if not DataClass.is_valid(data_class):
                print(f"Error: Invalid data class '{data_class}'")
                return None
            
            if not self.service:
                print("Failed to get Drive service")
                return None
                
            print("\nListing available folders before upload:")
            folders = self.list_shared_folders()
            if folders is None:
                return None
                
            folder_id = self.get_upload_folder_id(data_class)
            
            if not folder_id:
                print(f"Error: Upload folder ID not found for data class {data_class}")
                return None
                
            print("\nTarget upload folder:")
            for name, info in folders.items():
                if info['id'] == folder_id:
                    print(f"Name: {name}")
                    print(f"ID: {folder_id}")
                    print(f"Link: {info['link']}")
                    break
            
            current_datetime = datetime.now().strftime('%Y-%m-%d_%H%M%S')
            filename = f'{data_class.lower()}_{current_datetime}_data.csv'
            
            print(f"\nUploading file '{filename}' to the target folder...")
            
            return self.upload_dataframe(
                df=df,
                filename=filename,
                folder_id=folder_id
            )
            
        except HttpError as error:
            print(f"An error occurred: {error}")
            return None

if __name__ == "__main__":
    # Example usage with PUB_78 data class
    example_df = pd.DataFrame({
        'name': ['John', 'Jane', 'Bob'],
        'age': [30, 25, 35],
        'city': ['New York', 'London', 'Paris']
    })
    
    uploader = GoogleDriveUploader()
    uploader.upload(df=example_df, data_class=DataClass.PUB_78)