class DataClass:
    PUB_78 = 'PUB_78'
    POSTCARD_990 = 'POSTCARD_990'
    FORM_990_MASTER = 'FORM_990_MASTER'
    SERIES_990 = 'SERIES_990'

    @classmethod
    def is_valid(cls, value: str) -> bool:
        return value in cls.get_all_values()

    @classmethod
    def get_all_values(cls) -> set[str]:
        # Return a set of all class-level string constants
        return {cls.PUB_78, cls.POSTCARD_990, cls.FORM_990_MASTER, cls.SERIES_990}
    
    @classmethod
    def validate_or_raise(cls, value: str) -> 'DataClass':
        if not cls.is_valid(value):
            raise ValueError(f"Invalid data class: {value}")
        return value  # Return the string value since we're not using Enum here
        
    @staticmethod
    def get_env_var_name(value: str) -> str:
        """Get the environment variable name for the folder ID of this data class.
        
        Returns:
            str: The environment variable name (e.g., 'PUB_78_UPLOAD_FOLDER_ID')
        """
        return f"{value}_UPLOAD_FOLDER_ID"