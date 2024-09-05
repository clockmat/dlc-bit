import logging
import os

from bson.codec_options import CodecOptions
from dotenv import load_dotenv
from pymongo import MongoClient

from rssbox.config import Config

load_dotenv()


if not os.path.exists(Config.DOWNLOAD_PATH):
    os.makedirs(Config.DOWNLOAD_PATH)


if os.path.exists(Config.LOG_FILE):
    with open(Config.LOG_FILE, "w") as f:
        pass


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Config.LOG_FILE),
    ],
)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("deta").setLevel(logging.WARNING)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


mongo_client = MongoClient(Config.MONGO_URL)
options = CodecOptions(tz_aware=True)

if Config.MONGO_DATABASE:
    mongo = mongo_client.get_database(Config.MONGO_DATABASE, codec_options=options)
else:
    mongo = mongo_client.get_default_database(codec_options=options)

accounts = mongo.get_collection("accounts", codec_options=options)

downloads = mongo.get_collection("downloads", codec_options=options)
downloads.create_index([("url", 1)], unique=True)

watchrss_database = mongo.get_collection("watchrss", codec_options=options)
workers = mongo.get_collection("workers", codec_options=options)
