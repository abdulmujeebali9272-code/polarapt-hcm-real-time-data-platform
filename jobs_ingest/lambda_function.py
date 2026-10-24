import json
import os
import boto3
import urllib.request
from datetime import datetime, timezone

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["BUCKET_NAME"]
USAJOBS_API_KEY = os.environ["USAJOBS_API_KEY"]
USAJOBS_EMAIL = os.environ["USAJOBS_EMAIL"]

USAJOBS_URL = "https://data.usajobs.gov/api/search"


def lambda_handler(event, context):

    timestamp = datetime.now(timezone.utc)

    headers = {
        "Host": "data.usajobs.gov",
        "User-Agent": USAJOBS_EMAIL,
        "Authorization-Key": USAJOBS_API_KEY
    }

    request = urllib.request.Request(
        USAJOBS_URL,
        headers=headers,
        method="GET"
    )

    try:
        with urllib.request.urlopen(request, timeout=20) as response:

            status_code = response.getcode()

            response_body = response.read().decode("utf-8")

            api_data = json.loads(response_body)

        jobs = (
            api_data
            .get("SearchResult", {})
            .get("SearchResultItems", [])
        )

        record_count = len(jobs)

        metadata = {
            "timestamp": timestamp.isoformat(),
            "source": "jobs",
            "record_count": record_count,
            "response_status": status_code
        }

        output = {
            "metadata": metadata,
            "data": jobs
        }

        object_key = (
            f"raw/jobs/"
            f"{timestamp:%Y/%m/%d}/"
            f"{timestamp:%H%M%S}.json"
        )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=object_key,
            Body=json.dumps(output, default=str).encode("utf-8"),
            ContentType="application/json",
            Metadata={
                "source": "jobs",
                "timestamp": timestamp.isoformat(),
                "record-count": str(record_count),
                "response-status": str(status_code)
            }
        )

        print(
            json.dumps({
                "message": "USAJOBS ingestion successful",
                "bucket": BUCKET_NAME,
                "key": object_key,
                "record_count": record_count,
                "response_status": status_code
            })
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "USAJOBS ingestion successful",
                "bucket": BUCKET_NAME,
                "key": object_key,
                "record_count": record_count
            })
        }

    except Exception as error:

        print(f"USAJOBS ingestion failed: {str(error)}")

        raise