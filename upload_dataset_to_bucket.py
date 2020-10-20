import os
from google.cloud import storage

def create_bucket_class_location(bucket_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

bucket_name = "fk_dotz_bucket"

if __name__ == "__main__":
    #create_bucket_class_location(bucket_name)
    upload_blob(bucket_name, f'{ os.getcwd() }/dataset/bill_of_materials.csv', 'dataset/bill_of_materials.csv')
    upload_blob(bucket_name, f'{ os.getcwd() }/dataset/comp_boss.csv', 'dataset/comp_boss.csv')
    upload_blob(bucket_name, f'{ os.getcwd() }/dataset/price_quote.csv', 'dataset/price_quote.csv')