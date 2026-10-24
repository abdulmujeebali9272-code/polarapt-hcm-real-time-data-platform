import json
import os
from datetime import datetime, timezone

import boto3
import pymssql

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

QUEUE_URL = os.environ["QUEUE_URL"]
SQL_HOST = os.environ["SQL_HOST"]
SQL_USER = os.environ["SQL_USER"]
SQL_PASSWORD = os.environ["SQL_PASSWORD"]

DATABASE_NAME = "HCM_DB"


def get_connection(database="master", autocommit=False):
    return pymssql.connect(
        server=SQL_HOST,
        user=SQL_USER,
        password=SQL_PASSWORD,
        database=database,
        port=1433,
        login_timeout=15,
        timeout=20,
        autocommit=autocommit
    )


def ensure_database():
    conn = get_connection("master", autocommit=True)
    cursor = conn.cursor()

    cursor.execute("""
    IF DB_ID('HCM_DB') IS NULL
        CREATE DATABASE HCM_DB
    """)

    cursor.close()
    conn.close()


def ensure_table(conn):
    cursor = conn.cursor()

    cursor.execute("""
    IF OBJECT_ID('dbo.fx_rates', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.fx_rates (
            id INT IDENTITY(1,1) PRIMARY KEY,
            currency_code VARCHAR(10) NOT NULL,
            rate DECIMAL(20,8) NOT NULL,
            base_currency VARCHAR(10) NOT NULL,
            source_timestamp VARCHAR(50),
            raw_s3_key VARCHAR(1000),
            processed_at DATETIME2 DEFAULT GETUTCDATE()
        )
    END
    """)

    conn.commit()
    cursor.close()


def insert_rates(conn, raw_data, raw_key):
    rates = raw_data["data"]["rates"]
    base_currency = raw_data["data"].get("base", "USD")
    source_timestamp = raw_data["metadata"].get("timestamp")

    cursor = conn.cursor()

    for currency, rate in rates.items():
        cursor.execute(
            """
            INSERT INTO dbo.fx_rates
            (
                currency_code,
                rate,
                base_currency,
                source_timestamp,
                raw_s3_key
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                currency,
                rate,
                base_currency,
                source_timestamp,
                raw_key
            )
        )

    conn.commit()
    cursor.close()

    return len(rates)


def lambda_handler(event, context):

    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=5,
        WaitTimeSeconds=1
    )

    messages = response.get("Messages", [])

    processed_messages = 0
    inserted_rates = 0

    for message in messages:
        try:
            routed_event = json.loads(message["Body"])

            if routed_event.get("source") != "fx":
                print("Skipping non-fx message")
                continue

            bucket = routed_event["bucket"]
            key = routed_event["key"]

            print(f"Processing: s3://{bucket}/{key}")

            obj = s3.get_object(
                Bucket=bucket,
                Key=key
            )

            raw_data = json.loads(
                obj["Body"].read().decode("utf-8")
            )

            # Create application database if needed
            ensure_database()

            # Connect to application database
            conn = get_connection(DATABASE_NAME)

            # Create FX table if needed
            ensure_table(conn)

            # Insert six currency rates
            count = insert_rates(
                conn,
                raw_data,
                key
            )

            conn.close()

            # Delete SQS message only after SQL success
            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=message["ReceiptHandle"]
            )

            processed_messages += 1
            inserted_rates += count

            print(
                f"SUCCESS: inserted {count} FX rates "
                f"from {key}"
            )

        except Exception as e:
            print(
                f"ERROR processing message "
                f"{message.get('MessageId')}: {str(e)}"
            )

    return {
        "statusCode": 200,
        "processed_messages": processed_messages,
        "inserted_rates": inserted_rates,
        "processed_at": datetime.now(
            timezone.utc
        ).isoformat()
    }