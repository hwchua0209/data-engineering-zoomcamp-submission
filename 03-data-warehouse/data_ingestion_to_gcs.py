import os
import argparse
from typing import Any

from google.cloud import storage  # type: ignore

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./keys/my_creds.json"  # type: ignore

parser = argparse.ArgumentParser(description="Ingest data to GCS")
parser.add_argument("--year", type=int, help="Record year of NYD taxi data in parquet")
parser.add_argument("--color", type=str, help="Yellow or green taxi")
parser.add_argument("--bucket", type=str, help="GCS bucket name")
parser.add_argument("--blob", type=str, help="Destination blob name")


def upload_blob(
    bucket_name: str, source_file_name: str, destination_blob_name: str
) -> None:
    """Uploads a file to GCS bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()  # type: ignore
    bucket = storage_client.bucket(bucket_name)  # type: ignore
    blob = bucket.blob(destination_blob_name)  # type: ignore

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    generation_match_precondition = 0

    blob.upload_from_filename(  # type: ignore
        source_file_name, if_generation_match=generation_match_precondition
    )

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


def main(params: Any) -> None:
    for month in [f"{i:>02}" for i in range(1, 13)]:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{params.color}_tripdata_{params.year}-{month}.parquet"
        local_path = (
            f"./taxi_data/{params.color}_tripdata_{params.year}-{month}.parquet"
        )
        os.system(f"wget {url} -O {local_path}")
        upload_blob(
            params.bucket,
            local_path,
            f"{params.blob}/{params.color}_tripdata_{params.year}-{month}.parquet",
        )


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
