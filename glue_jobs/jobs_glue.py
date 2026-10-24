import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_INPUT_PATH"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3_input_path = args["S3_INPUT_PATH"]

# Read raw USAJOBS JSON files from S3
raw_df = spark.read.option("multiLine", "true").json(s3_input_path)

rows = []

for file_row in raw_df.collect():

    metadata = file_row["metadata"]
    jobs = file_row["data"]

    ingestion_timestamp = metadata["timestamp"]

    if jobs:
        for item in jobs:

            descriptor = item["MatchedObjectDescriptor"]

            remuneration = descriptor["PositionRemuneration"]

            salary_min = None
            salary_max = None

            if remuneration and len(remuneration) > 0:
                salary_min = remuneration[0]["MinimumRange"]
                salary_max = remuneration[0]["MaximumRange"]

            rows.append(
                Row(
                    job_id=item["MatchedObjectId"],
                    position_id=descriptor["PositionID"],
                    job_title=descriptor["PositionTitle"],
                    organization=descriptor["OrganizationName"],
                    department=descriptor["DepartmentName"],
                    location=descriptor["PositionLocationDisplay"],
                    salary_min=float(salary_min) if salary_min else None,
                    salary_max=float(salary_max) if salary_max else None,
                    open_date=descriptor["PublicationStartDate"],
                    close_date=descriptor["ApplicationCloseDate"],
                    job_url=descriptor["PositionURI"],
                    ingestion_timestamp=ingestion_timestamp
                )
            )

jobs_df = spark.createDataFrame(rows)

print("===== TRANSFORMED USAJOBS =====")
jobs_df.show(20, truncate=False)

print("TOTAL RECORDS:", jobs_df.count())

# Temporary processed output.
# Snowflake write will be added after connection details are configured.
processed_path = "s3://data-hackathon-smit-taha/processed/jobs/"

jobs_df.write.mode("append").parquet(processed_path)

print("Processed jobs written to:", processed_path)

job.commit()