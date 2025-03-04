import os
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
import pandas as pd
import io
from datetime import datetime

# Get the parent directory path and load environment variables
PARENT_DIR = Path(__file__).parent.parent
load_dotenv(PARENT_DIR / '.env')

# Define scopes for Google Drive API
SCOPES = [
    'https://www.googleapis.com/auth/drive.metadata.readonly',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive.metadata'  # Added for sharing functionality
]

def get_drive_service():
    """Get an authorized Google Drive service instance."""
    try:
        # Debug: Print the current working directory and .env file path
        print(f"Current working directory: {os.getcwd()}")
        env_path = PARENT_DIR / '.env'
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
            scopes=SCOPES
        )
        
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        print(f"Error creating Drive service: {str(e)}")
        print(f"Exception type: {type(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None

def share_file(service, file_id, email='npearch24@gmail.com', role='reader'):
    """Share a file with a specific email address.
    
    Args:
        service: Google Drive service instance
        file_id: ID of the file to share
        email: Email address to share with
        role: Role to grant (default: 'reader')
    
    Returns:
        dict: Permission metadata if successful, None otherwise
    """
    try:
        user_permission = {
            'type': 'user',
            'role': role,
            'emailAddress': email
        }
        
        permission = service.permissions().create(
            fileId=file_id,
            body=user_permission,
            fields='id'
        ).execute()
        
        print(f"File shared with {email}")
        return permission
    except HttpError as error:
        print(f"Error sharing file with {email}: {error}")
        return None

def upload_dataframe(df, filename, folder_id=None):
    """Upload a pandas DataFrame as a CSV to Google Drive.
    
    Args:
        df: pandas DataFrame to upload
        filename: Name to give the file in Drive
        folder_id: ID of the folder to upload to (optional)
    
    Returns:
        dict: File metadata from Drive if successful, None otherwise
    """
    try:
        service = get_drive_service()
        if not service:
            print("Failed to get Drive service")
            return None
        
        # Convert DataFrame to CSV string
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        # Create file metadata
        file_metadata = {
            'name': filename,
            'mimeType': 'text/csv'
        }
        
        # Add parent folder if specified
        if folder_id:
            file_metadata['parents'] = [folder_id]
        
        # Create media upload with bytes content
        media = MediaIoBaseUpload(
            io.BytesIO(csv_content.encode('utf-8')),
            mimetype='text/csv',
            resumable=True
        )
        
        # Upload file
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink'
        ).execute()
        
        print(f"\nFile uploaded successfully: {file.get('name')}")
        print(f"File ID: {file.get('id')}")
        print(f"File link: {file.get('webViewLink')}")
        
        # Share the file with npearch24@gmail.com
        share_file(service, file.get('id'))
        
        return file
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

def get_upload_folder_id(data_class: str) -> str:
    """Get the appropriate folder ID based on the data class.
    
    Args:
        data_class: String identifier for the data type (e.g., 'PUB_78')
    
    Returns:
        str: Folder ID from environment variables, or None if not found
    """
    if data_class == "PUB_78":
        return os.getenv('PUB_78_UPLOAD_FOLDER_ID')
    # Add more data classes here as needed
    return None

def main(data_class: str = None):
    """Upload data to Google Drive based on the specified data class.
    
    Args:
        data_class: String identifier for the data type (e.g., 'PUB_78')
    """
    try:
        if not data_class:
            print("Error: data_class parameter is required")
            return
            
        # Get the appropriate folder ID based on data class
        folder_id = get_upload_folder_id(data_class)
        
        if not folder_id:
            print(f"Error: Upload folder ID not found for data class {data_class}")
            return
        
        # Example: Create a sample DataFrame
        df = pd.DataFrame({
            'name': ['John', 'Jane', 'Bob'],
            'age': [30, 25, 35],
            'city': ['New York', 'London', 'Paris']
        })
        
        # Generate filename with date, time and data class
        current_datetime = datetime.now().strftime('%Y-%m-%d_%H%M%S')
        filename = f'{data_class.lower()}_{current_datetime}_data.csv'
        
        # Upload the DataFrame to Drive in the specified folder
        upload_dataframe(
            df=df,
            filename=filename,
            folder_id=folder_id
        )
        
    except HttpError as error:
        print(f"An error occurred: {error}")

if __name__ == "__main__":
    # Example usage with PUB_78 data class
    main(data_class="PUB_78")

    # If you add more data classes in the future, you can use them like:
    # main(data_class="OTHER_DATA_CLASS")