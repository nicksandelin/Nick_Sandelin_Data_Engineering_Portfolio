# Created by Nick Sandelin 3/2/2026
# config.py — Database and ETL path configuration 

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",       # Update if your Dolt user differs
    "password": "",       # Update with your Dolt password
    "database": "Operations",
}

# Path to the folder containing incoming sales*.txt files
SOURCE_DIR = r"C:\Users\nicks\Documents\pyETL"

# Subfolder where processed files are moved after a successful load
ARCHIVE_DIR = r"C:\Users\nicks\Documents\pyETL\archived"

# Glob pattern used to discover sales files
FILE_PATTERN = "sales*.txt"

# Expected CSV delimiter inside the sales files
DELIMITER = ","
