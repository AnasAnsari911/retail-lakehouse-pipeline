"""
Shared PostgreSQL connection settings for the Spark jobs.
Credentials come from the .env file, never from the code.
"""

import os

from dotenv import load_dotenv

from src.common import config

# Read the values from the .env file in the project root
load_dotenv(config.ROOT / ".env")

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")


def jdbc_options():
    """Return the options Spark needs to connect to PostgreSQL."""
    return {
        "url": f"jdbc:postgresql://localhost:{POSTGRES_PORT}/{POSTGRES_DB}",
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }