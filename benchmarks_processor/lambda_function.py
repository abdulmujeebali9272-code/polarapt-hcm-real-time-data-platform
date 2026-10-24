import json
import boto3
import os
from datetime import datetime, timezone

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

QUEUE_URL = os.environ["QUEUE_URL"]
BUCKET_NAME = os.environ["BUCKET_NAME"]


def lambda_handler(event, context):

    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1,
        MessageAttributeNames=["All"],
        AttributeNames=["All"]
    )

    messages = response.get("Messages", [])

    if not messages:
        print("No benchmark messages available.")
        return {
            "statusCode": 200,
            "processed_messages": 0
        }

    processed_count = 0

    for message in messages:
        try:
            routed_event = json.loads(message["Body"])

            source = routed_event.get("source")

            if source != "benchmarks":
                print(f"Skipping unexpected source: {source}")
                continue

            bucket = routed_event["bucket"]
            raw_key = routed_event["key"]

            raw_object = s3.get_object(
                Bucket=bucket,
                Key=raw_key
            )

            raw_data = json.loads(
                raw_object["Body"].read().decode("utf-8")
            )

            records = raw_data.get("data", [])

            transformed_records = []

            for index, record in enumerate(records, start=1):

                occupation = (
                    record.get("occupation")
                    or record.get("title")
                    or record.get("Occupation")
                    or "Unknown"
                )

                salary = (
                    record.get("median_annual_pay")
                    or record.get("salary")
                    or record.get("Median Annual Pay")
                    or 0
                )

                try:
                    salary = int(
                        str(salary)
                        .replace("$", "")
                        .replace(",", "")
                        .strip()
                    )
                except (ValueError, TypeError):
                    salary = 0

                transformed_records.append({
                    "rank": index,
                    "occupation": occupation,
                    "median_annual_pay_usd": salary,
                    "median_monthly_pay_usd": round(salary / 12, 2),
                    "salary_category": (
                        "Very High"
                        if salary >= 400000
                        else "High"
                        if salary >= 300000
                        else "Standard"
                    )
                })

            now = datetime.now(timezone.utc)

            processed_payload = {
                "metadata": {
                    "source": "benchmarks",
                    "processed_timestamp": now.isoformat(),
                    "raw_s3_key": raw_key,
                    "record_count": len(transformed_records),
                    "processor": "lambda_benchmarks_processor"
                },
                "data": transformed_records
            }

            processed_key = (
                f"processed/benchmarks/"
                f"{now:%Y/%m/%d}/"
                f"{now:%H%M%S}.json"
            )

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=processed_key,
                Body=json.dumps(
                    processed_payload,
                    indent=2
                ).encode("utf-8"),
                ContentType="application/json",
                Metadata={
                    "source": "benchmarks",
                    "record_count": str(len(transformed_records)),
                    "raw_key": raw_key
                }
            )

            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=message["ReceiptHandle"]
            )

            processed_count += 1

            print(json.dumps({
                "message": "Benchmark processed successfully",
                "raw_key": raw_key,
                "processed_key": processed_key,
                "record_count": len(transformed_records)
            }))

        except Exception as exc:
            print(f"Processing failed: {str(exc)}")
            raise

    return {
        "statusCode": 200,
        "processed_messages": processed_count
    }