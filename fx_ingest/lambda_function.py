import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["BUCKET_NAME"]
OPENEXCHANGE_APP_ID = os.environ["OPENEXCHANGE_APP_ID"]

TARGET_CURRENCIES = ["PKR", "SAR", "AED", "GBP", "EUR", "INR"]


def lambda_handler(event, context):

    timestamp = datetime.now(timezone.utc)

    url = (
        "https://openexchangerates.org/api/latest.json"
        f"?app_id={OPENEXCHANGE_APP_ID}"
    )

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Polarapt-HCM-Hackathon/1.0"
        }
    )

    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            response_status = response.status
            api_data = json.loads(response.read().decode("utf-8"))

        all_rates = api_data.get("rates", {})

        rates = {
            currency: all_rates.get(currency)
            for currency in TARGET_CURRENCIES
            if currency in all_rates
        }

        metadata = {
            "timestamp": timestamp.isoformat(),
            "source": "fx",
            "record_count": len(rates),
            "response_status": response_status
        }

        output = {
            "metadata": metadata,
            "data": {
                "base": api_data.get("base", "USD"),
                "rates": rates
            }
        }

        key = (
            f"raw/fx/"
            f"{timestamp:%Y/%m/%d}/"
            f"{timestamp:%H%M%S}.json"
        )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(output).encode("utf-8"),
            ContentType="application/json",
            Metadata={
                "source": "fx",
                "timestamp": timestamp.isoformat(),
                "record_count": str(len(rates)),
                "response_status": str(response_status)
            }
        )

        return {
            "statusCode": 200,
            "message": "FX ingestion successful",
            "bucket": BUCKET_NAME,
            "key": key,
            "record_count": len(rates),
            "rates": rates
        }

    except Exception as e:
        print(f"ERROR: {str(e)}")
        raise