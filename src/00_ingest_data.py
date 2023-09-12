import os
import re
import json
import boto3
import tarfile
import logging
import pandas as pd
from glob import glob
# from dotenv import load_env
from pathlib import Path
from radiant_mlhub import Dataset, Collection, client

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# # Load environment variables
# load_dotenv('.env')


def list_mlhub_datasets():
    """List all datasets on Radiant ML Hub"""
    for dataset in Dataset.list():
        print(dataset.id)
    print()


def list_collection_metadata(dataset_id: str):
    """List all metadata in the dataset"""
    # 
    dataset_client = Dataset.fetch(dataset_id)
    
    # List all image collections in the dataset
    print('Source Imagery Collections')
    print('--------------------------')
    for collection in dataset_client.collections.source_imagery:
        print(collection.id)
    
    print()
    
    # List all label collections in the dataset
    print('Label Collections')
    print('-----------------')
    for collection in dataset_client.collections.labels:
        print(collection.id)

    return dataset_client


def print_summary(item, collection):
    print(f'Collection: {collection.id}')
    print(f'Item: {item["id"]}')
    print('Assets:')
    for asset_name, asset in item.get('assets', {}).items():
        print(f'- {asset_name}: {asset["title"]} [{asset["type"]}]')
    
    print('\n')

def explore_collection(dataset_client: Dataset):
    """
    """
    for collection in dataset_client.collections:
        item = next(client.list_collection_items(collection.id, limit=1))
        print_summary(item, collection)

def download_dataset(dataset_client: Dataset, download_dir: str | Path):
    """
    """
    # NOTE: Extracting the archives takes a while so
    #       this cell may take 5-10 minutes to complete
    archive_paths = dataset_client.download(output_dir=download_dir)
    for archive_path in archive_paths:
        print(f'Extracting {archive_path}...')
        with tarfile.open(archive_path) as tfile:
            tfile.extractall(path=download_dir)
    print('Done')

class ObjectWrapper:
    """Encapsulates S3 object actions."""
    def __init__(self, s3_object):
        """
        :param s3_object: A Boto3 Object resource. This is a high-level resource in Boto3
                          that wraps object actions in a class-like structure.
        """
        self.object = s3_object
        self.key = self.object.key

    def put(self, data):
        """
        Upload data to the object.

        :param data: The data to upload. This can either be bytes or a string. When this
                     argument is a string, it is interpreted as a file name, which is
                     opened in read bytes mode.
        """
        put_data = data
        if isinstance(data, str):
            try:
                put_data = open(data, 'rb')
            except IOError:
                logger.exception("Expected file name or binary data, got '%s'.", data)
                raise

        try:
            self.object.put(Body=put_data)
            self.object.wait_until_exists()
            logger.info(
                "Put object '%s' to bucket '%s'.", self.object.key,
                self.object.bucket_name)
        except Exception:
            logger.exception(
                "Couldn't put object '%s' to bucket '%s'.", self.object.key,
                self.object.bucket_name)
            raise
        finally:
            if getattr(put_data, 'close', None):
                put_data.close()

def upload_directory(data_dir: str | Path):
    """Upload a directory to S3"""
    # Get the bucket name from the environment
    bucket_name = os.environ['AWS_BUCKET_NAME']

    # Create a Boto3 resource to use for uploading
    s3_resource = boto3.resource('s3')

    # Walk through the directory and upload each file
    for root, dirs, files in os.walk(data_dir):
        for filename in files:
            # Construct the full local path
            local_path = os.path.join(root, filename)

            # Construct the full Dropbox path
            relative_path = os.path.relpath(local_path, data_dir)
            s3_path = os.path.join(data_dir, relative_path)

            # Upload the file
            s3_resource.Object(bucket_name, s3_path).put(local_path)


def main():
    dataset_id = 'nasa_tropical_storm_competition'

    dataset_client = list_collection_metadata(dataset_id=dataset_id)
    
    # Use this to download to a data folder the current working directory
    download_dir = Path(__file__).parent.parent / 'data'
    os.makedirs(str(download_dir), exist_ok=True)

    print()

    explore_collection(dataset_client=dataset_client)
    
    print()

    download_dataset(dataset_client=dataset_client, download_dir=download_dir)

    upload_directory(data_dir=download_dir)



if __name__ == '__main__':
    print()
    main()
    print()