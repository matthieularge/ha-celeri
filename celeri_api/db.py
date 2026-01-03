import json
import logging
import mysql.connector

logger = logging.getLogger(__name__)


def load_config():
    with open("/data/options.json") as f:
        return json.load(f)


config = load_config()

DB_CONFIG = {
    "host": config["DB_HOST"],
    "user": config["DB_USER"],
    "password": config["DB_PASSWORD"],
    "database": config["DB_NAME"],
}


def get_connection():
    logger.debug("Creating new database connection")
    return mysql.connector.connect(**DB_CONFIG)
