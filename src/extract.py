import configparser
from pymongo import MongoClient  # type: ignore


def read_config():
    import os
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), "..", "config", "config.ini"))
    return config


def extract_mongodb(config):
    # MongoDB connection details
    uri = config["MONGODB"]["URI"]
    database_name = config["MONGODB"]["DATABASE"]
    collection_name = config["MONGODB"]["COLLECTION"]

    client = MongoClient(uri)
    db = client[database_name]
    collection = db[collection_name]

    documents = list(collection.find())

    # Remove _id
    for doc in documents:
        doc.pop("_id", None)

    return documents


