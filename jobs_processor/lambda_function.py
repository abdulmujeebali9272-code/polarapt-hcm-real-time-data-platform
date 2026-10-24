import boto3
import json
import os
from datetime import datetime, timezone
import snowflake.connector

sqs = boto3.client("sqs")
s3 = boto3.client("s3")
secretsmanager = boto3.client("secretsmanager")

QUEUE_URL = os.environ["QUEUE_URL"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
SECRET_NAME = "polarapt-hcm/snowflake"


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_snowflake_connection():
    response = secretsmanager.get_secret_value(
        SecretId=SECRET_NAME
    )

    secret = json.loads(response["SecretString"])

    return snowflake.connector.connect(
        user=secret["user"],
        password=secret["password"],
        account=secret["account"],
        warehouse=secret["warehouse"],
        database=secret["database"],
        schema=secret["schema"]
    )


def upsert_jobs_to_snowflake(jobs):
    if not jobs:
        return 0

    # Remove duplicate JOB_IDs inside the batch.
    unique_jobs = {}

    for job in jobs:
        if job.get("job_id"):
            unique_jobs[str(job["job_id"])] = job

    jobs = list(unique_jobs.values())

    if not jobs:
        return 0

    conn = None
    cursor = None

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TEMPORARY TABLE JOBS_STAGE (
                JOB_ID VARCHAR,
                POSITION_ID VARCHAR,
                JOB_TITLE VARCHAR,
                ORGANIZATION VARCHAR,
                DEPARTMENT VARCHAR,
                LOCATION VARCHAR,
                SALARY_MIN NUMBER(18,2),
                SALARY_MAX NUMBER(18,2),
                SALARY_INTERVAL VARCHAR,
                OPEN_DATE TIMESTAMP_TZ,
                CLOSE_DATE TIMESTAMP_TZ,
                JOB_URL VARCHAR
            )
        """)

        insert_sql = """
            INSERT INTO JOBS_STAGE (
                JOB_ID,
                POSITION_ID,
                JOB_TITLE,
                ORGANIZATION,
                DEPARTMENT,
                LOCATION,
                SALARY_MIN,
                SALARY_MAX,
                SALARY_INTERVAL,
                OPEN_DATE,
                CLOSE_DATE,
                JOB_URL
            )
            VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
        """

        rows = [
            (
                job.get("job_id"),
                job.get("position_id"),
                job.get("job_title"),
                job.get("organization"),
                job.get("department"),
                job.get("location"),
                job.get("salary_min"),
                job.get("salary_max"),
                job.get("salary_interval"),
                job.get("open_date"),
                job.get("close_date"),
                job.get("job_url")
            )
            for job in jobs
        ]

        cursor.executemany(insert_sql, rows)

        cursor.execute("""
            MERGE INTO POLARAPT_HCM.RAW.JOBS AS T
            USING JOBS_STAGE AS S
                ON T.JOB_ID = S.JOB_ID

            WHEN MATCHED THEN UPDATE SET
                T.POSITION_ID = S.POSITION_ID,
                T.JOB_TITLE = S.JOB_TITLE,
                T.ORGANIZATION = S.ORGANIZATION,
                T.DEPARTMENT = S.DEPARTMENT,
                T.LOCATION = S.LOCATION,
                T.SALARY_MIN = S.SALARY_MIN,
                T.SALARY_MAX = S.SALARY_MAX,
                T.SALARY_INTERVAL = S.SALARY_INTERVAL,
                T.OPEN_DATE = S.OPEN_DATE,
                T.CLOSE_DATE = S.CLOSE_DATE,
                T.JOB_URL = S.JOB_URL,
                T.INGESTED_AT = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                JOB_ID,
                POSITION_ID,
                JOB_TITLE,
                ORGANIZATION,
                DEPARTMENT,
                LOCATION,
                SALARY_MIN,
                SALARY_MAX,
                SALARY_INTERVAL,
                OPEN_DATE,
                CLOSE_DATE,
                JOB_URL,
                INGESTED_AT
            )
            VALUES (
                S.JOB_ID,
                S.POSITION_ID,
                S.JOB_TITLE,
                S.ORGANIZATION,
                S.DEPARTMENT,
                S.LOCATION,
                S.SALARY_MIN,
                S.SALARY_MAX,
                S.SALARY_INTERVAL,
                S.OPEN_DATE,
                S.CLOSE_DATE,
                S.JOB_URL,
                CURRENT_TIMESTAMP()
            )
        """)

        conn.commit()

        print(
            f"SNOWFLAKE SUCCESS: {len(jobs)} unique jobs upserted"
        )

        return len(jobs)

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


def lambda_handler(event, context):

    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=5,
        WaitTimeSeconds=1,
        MessageAttributeNames=["All"],
        AttributeNames=["All"]
    )

    messages = response.get("Messages", [])

    if not messages:
        print("No jobs messages available.")

        return {
            "statusCode": 200,
            "processed_messages": 0,
            "total_jobs": 0,
            "snowflake_jobs": 0
        }

    processed_messages = 0
    total_jobs = 0
    total_snowflake_jobs = 0

    for message in messages:

        try:
            routed_event = json.loads(message["Body"])

            if routed_event.get("source") != "jobs":
                print("Skipping non-jobs message")
                continue

            raw_bucket = routed_event["bucket"]
            raw_key = routed_event["key"]

            print(
                f"Processing: s3://{raw_bucket}/{raw_key}"
            )

            obj = s3.get_object(
                Bucket=raw_bucket,
                Key=raw_key
            )

            raw_data = json.loads(
                obj["Body"].read().decode("utf-8")
            )

            jobs = raw_data.get("data", [])
            transformed_jobs = []

            for item in jobs:

                descriptor = item.get(
                    "MatchedObjectDescriptor", {}
                )

                remuneration = descriptor.get(
                    "PositionRemuneration", []
                )

                salary_min = None
                salary_max = None
                salary_interval = None

                if remuneration:
                    salary_min = safe_float(
                        remuneration[0].get(
                            "MinimumRange"
                        )
                    )

                    salary_max = safe_float(
                        remuneration[0].get(
                            "MaximumRange"
                        )
                    )

                    salary_interval = remuneration[0].get(
                        "Description"
                    )

                transformed_jobs.append({
                    "job_id":
                        item.get("MatchedObjectId"),

                    "position_id":
                        descriptor.get("PositionID"),

                    "job_title":
                        descriptor.get("PositionTitle"),

                    "organization":
                        descriptor.get("OrganizationName"),

                    "department":
                        descriptor.get("DepartmentName"),

                    "location":
                        descriptor.get(
                            "PositionLocationDisplay"
                        ),

                    "salary_min": salary_min,
                    "salary_max": salary_max,
                    "salary_interval": salary_interval,

                    "open_date":
                        descriptor.get(
                            "PublicationStartDate"
                        ),

                    "close_date":
                        descriptor.get(
                            "ApplicationCloseDate"
                        ),

                    "job_url":
                        descriptor.get("PositionURI")
                })

            now = datetime.now(timezone.utc)

            output = {
                "metadata": {
                    "source": "jobs",
                    "processed_timestamp":
                        now.isoformat(),

                    "raw_s3_key": raw_key,

                    "record_count":
                        len(transformed_jobs),

                    "processor":
                        "lambda_jobs_processor",

                    "target":
                        "Snowflake POLARAPT_HCM.RAW.JOBS",

                    "glue_fallback": True,

                    "glue_fallback_reason":
                        "AWS account-level Glue CreateJob AccessDenied"
                },

                "data": transformed_jobs
            }

            request_id = message["MessageId"]

            processed_key = (
                f"processed/jobs/"
                f"{now:%Y/%m/%d}/"
                f"{now:%H%M%S}_{request_id}.json"
            )

            # Save transformed copy to processed S3.
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=processed_key,
                Body=json.dumps(
                    output,
                    default=str
                ),
                ContentType="application/json",
                Metadata={
                    "source": "jobs",
                    "record_count":
                        str(len(transformed_jobs)),
                    "processor":
                        "lambda_jobs_processor"
                }
            )

            # Snowflake must succeed before deleting SQS.
            snowflake_count = (
                upsert_jobs_to_snowflake(
                    transformed_jobs
                )
            )

            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=
                    message["ReceiptHandle"]
            )

            processed_messages += 1
            total_jobs += len(transformed_jobs)
            total_snowflake_jobs += snowflake_count

            print(
                f"SUCCESS: "
                f"{len(transformed_jobs)} jobs -> S3; "
                f"{snowflake_count} -> Snowflake"
            )

        except Exception as error:
            print(
                f"ERROR processing message "
                f"{message.get('MessageId')}: "
                f"{str(error)}"
            )

            # Failed messages remain in SQS.
            continue

    return {
        "statusCode": 200,
        "processed_messages":
            processed_messages,
        "total_jobs":
            total_jobs,
        "snowflake_jobs":
            total_snowflake_jobs
    }