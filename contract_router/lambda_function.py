import boto3
import json
import os
import urllib.parse

s3 = boto3.client("s3")
sns = boto3.client("sns")

ROUTED_TOPIC_ARN = os.environ["ROUTED_TOPIC_ARN"]


def lambda_handler(event, context):

    routed_count = 0

    for sns_record in event.get("Records", []):

        # Standard SNS message contains the S3 event as JSON
        message = sns_record["Sns"]["Message"]
        s3_event = json.loads(message)

        # Ignore S3 test events if received
        if "Records" not in s3_event:
            print(f"Ignoring non-record event: {s3_event}")
            continue

        for record in s3_event["Records"]:

            bucket = record["s3"]["bucket"]["name"]

            key = urllib.parse.unquote_plus(
                record["s3"]["object"]["key"]
            )

            # Read contract metadata written by ingestion Lambda
            head = s3.head_object(
                Bucket=bucket,
                Key=key
            )

            metadata = head.get("Metadata", {})

            source = metadata.get("source")

            if not source:
                raise ValueError(
                    f"Missing source metadata for s3://{bucket}/{key}"
                )

            if source not in ["jobs", "benchmarks", "fx"]:
                raise ValueError(
                    f"Unsupported source '{source}' for {key}"
                )

            routed_message = {
                "bucket": bucket,
                "key": key,
                "source": source,
                "timestamp": metadata.get("timestamp"),
                "record_count": metadata.get("record-count"),
                "response_status": metadata.get("response-status")
            }

            response = sns.publish(
                TopicArn=ROUTED_TOPIC_ARN,
                Message=json.dumps(routed_message),
                MessageGroupId=source,
                MessageAttributes={
                    "source": {
                        "DataType": "String",
                        "StringValue": source
                    }
                }
            )

            routed_count += 1

            print(json.dumps({
                "message": "Event routed successfully",
                "source": source,
                "bucket": bucket,
                "key": key,
                "message_id": response["MessageId"]
            }))

    return {
        "statusCode": 200,
        "routed_count": routed_count
    }