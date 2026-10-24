\# 🚀 Polarapt HCM Real-Time Data Platform



\*\*SMIT Cloud Data Engineering Hackathon — October 2026\*\*



An event-driven, near-real-time HR data platform built on AWS that ingests data from multiple external sources every minute, stores raw data in Amazon S3, routes events using SNS/SQS, and processes each source independently into its target system.



\---



\## 📌 Project Overview



Polarapt HCM requires a scalable external data backbone for HR analytics.



This solution integrates three external data sources:



1\. \*\*USAJOBS API\*\* — Job postings, organizations, locations, salary ranges, and opening/closing dates.

2\. \*\*BLS Highest Paying Occupations\*\* — Top occupations and median annual salaries.

3\. \*\*Open Exchange Rates\*\* — USD exchange rates for PKR, SAR, AED, GBP, EUR, and INR.



The platform uses an event-driven architecture where ingestion and processing pipelines are decoupled through Amazon SNS and Amazon SQS FIFO queues.



\---



\## 🏗️ Architecture



```text

&#x20;                        Amazon EventBridge

&#x20;                   (Scheduled every 1 minute)

&#x20;                             |

&#x20;            +----------------+----------------+

&#x20;            |                |                |

&#x20;            v                v                v

&#x20;      Jobs Ingest      BLS Scraper       FX Ingest

&#x20;        Lambda            Lambda            Lambda

&#x20;            |                |                |

&#x20;            +----------------+----------------+

&#x20;                             |

&#x20;                             v

&#x20;                    Amazon S3 Raw Zone

&#x20;                             |

&#x20;                    ObjectCreated Event

&#x20;                             |

&#x20;                             v

&#x20;                   Standard SNS Topic

&#x20;                     hcm-raw-events

&#x20;                             |

&#x20;                             v

&#x20;                   Contract Router Lambda

&#x20;                             |

&#x20;                             v

&#x20;                      SNS FIFO Topic

&#x20;                 hcm-routed-events.fifo

&#x20;                             |

&#x20;              Message Attribute: source

&#x20;                             |

&#x20;         +-------------------+-------------------+

&#x20;         |                   |                   |

&#x20;         v                   v                   v

&#x20;jobs-queue.fifo   benchmarks-queue.fifo    fx-queue.fifo

&#x20;         |                   |                   |

&#x20;         v                   v                   v

&#x20;  Jobs Processor     Benchmarks Processor    FX Processor

&#x20;     Lambda                 Lambda              Lambda

&#x20;         |                   |                   |

&#x20;         v                   v                   v

&#x20;    Snowflake          S3 Processed        SQL Server RDS

```



\---



\## 🔄 Data Flow



\### 1. USAJOBS Pipeline



```text

EventBridge

&#x20;  ↓

lambda\_jobs\_ingest

&#x20;  ↓

USAJOBS API

&#x20;  ↓

S3 raw/jobs/

&#x20;  ↓

SNS Standard

&#x20;  ↓

contract\_router

&#x20;  ↓

SNS FIFO

&#x20;  ↓

jobs-queue.fifo

&#x20;  ↓

lambda\_jobs\_processor

&#x20;  ↓

Snowflake

```



The ingestion Lambda fetches USAJOBS records and stores the API response in the S3 raw zone with contract metadata.



The jobs processor reads routed messages from the FIFO queue, extracts and transforms the required job fields, and loads them into Snowflake.



The Snowflake implementation uses an idempotent \*\*MERGE/upsert strategy\*\*, preventing repeated minute-by-minute ingestion from creating duplicate job records.



\### Snowflake Target



```text

Database: POLARAPT\_HCM

Schema:   RAW

Table:    JOBS

Warehouse: HCM\_WH

```



Key fields include:



\- JOB\_ID

\- POSITION\_ID

\- JOB\_TITLE

\- ORGANIZATION

\- DEPARTMENT

\- LOCATION

\- SALARY\_MIN

\- SALARY\_MAX

\- SALARY\_INTERVAL

\- OPEN\_DATE

\- CLOSE\_DATE

\- JOB\_URL

\- INGESTED\_AT



\---



\## 💼 2. BLS Salary Benchmark Pipeline



```text

EventBridge

&#x20;  ↓

lambda\_benchmarks\_scraper

&#x20;  ↓

BLS Highest Paying Occupations

&#x20;  ↓

S3 raw/benchmarks/

&#x20;  ↓

SNS → Router → SNS FIFO

&#x20;  ↓

benchmarks-queue.fifo

&#x20;  ↓

lambda\_benchmarks\_processor

&#x20;  ↓

S3 processed/benchmarks/

```



The processor transforms benchmark records into structured salary information including:



\- Rank

\- Occupation

\- Median annual salary

\- Estimated monthly salary

\- Salary category



\### BLS Resilience Strategy



The official BLS web page was reachable through a normal browser, but automated requests from the cloud environment were rejected by the site's Akamai protection with HTTP 403.



To keep the pipeline operational during the hackathon, the scraper:



1\. Attempts the live BLS request.

2\. Detects a blocked response.

3\. Uses a verified snapshot of the BLS Top 10 Highest Paying Occupations dataset as a fallback.

4\. Continues the same downstream S3 → SNS → SQS → processing workflow.



This fallback is explicitly documented rather than treating the snapshot as a live scrape.



\---



\## 💱 3. Foreign Exchange Pipeline



```text

EventBridge

&#x20;  ↓

lambda\_fx\_ingest

&#x20;  ↓

Open Exchange Rates API

&#x20;  ↓

S3 raw/fx/

&#x20;  ↓

SNS → Router → SNS FIFO

&#x20;  ↓

fx-queue.fifo

&#x20;  ↓

lambda\_fx\_processor

&#x20;  ↓

Amazon RDS SQL Server

```



The FX ingestion pipeline collects USD-based exchange rates for:



```text

PKR

SAR

AED

GBP

EUR

INR

```



The processor writes the records into:



```text

Database: HCM\_DB

Table: dbo.fx\_rates

```



Each record includes the currency code, exchange rate, base currency, source timestamp, originating S3 key, and processing timestamp.



\---



\## 📦 S3 Data Lake Structure



Bucket:



```text

data-hackathon-smit-taha

```



Structure:



```text

data-hackathon-smit-taha/

│

├── raw/

│   ├── jobs/

│   │   └── YYYY/MM/DD/

│   ├── benchmarks/

│   │   └── YYYY/MM/DD/

│   └── fx/

│       └── YYYY/MM/DD/

│

├── processed/

│   ├── jobs/

│   └── benchmarks/

│

├── scripts/

│   └── jobs\_glue.py

│

└── layers/

```



Raw objects follow date-based partitioning and contain contract metadata.



\---



\## 📑 Data Contract



Each ingestion Lambda writes metadata with the raw object, including information such as:



```json

{

&#x20; "timestamp": "UTC timestamp",

&#x20; "source": "jobs | benchmarks | fx",

&#x20; "record\_count": 25,

&#x20; "response\_status": 200

}

```



The source is also stored in S3 object metadata.



The Contract Router reads this metadata and uses the `source` value to route the event to the appropriate FIFO queue.



\---



\## 📨 Event Routing



Amazon S3 publishes object-created events to the \*\*Standard SNS topic\*\*:



```text

hcm-raw-events

```



The `contract\_router` Lambda reads the object's metadata and republishes a normalized event to:



```text

hcm-routed-events.fifo

```



The source is passed as an SNS message attribute.



SNS subscription filter policies then route messages to:



```text

jobs-queue.fifo

benchmarks-queue.fifo

fx-queue.fifo

```



This keeps the ingestion layer independent from downstream processors.



\---



\## 🔁 Why FIFO Queues?



FIFO queues are used to provide:



\- Ordered processing within each source.

\- Message grouping.

\- Deduplication support.

\- Independent buffering for each pipeline.

\- Decoupling between routing and processing.



Each source can therefore be processed independently without blocking the other data feeds.



\---



\## ⏱️ Scheduling



Six Amazon EventBridge rules run every minute:



```text

hcm-jobs-ingest-every-minute

hcm-jobs-processor-every-minute



hcm-benchmarks-ingest-every-minute

hcm-benchmarks-processor-every-minute



hcm-fx-ingest-every-minute

hcm-fx-processor-every-minute

```



This provides continuous ingestion and processing for all three pipelines.



\---



\## ⚡ AWS Lambda Functions



The solution uses the following Lambda functions:



```text

lambda\_jobs\_ingest

lambda\_jobs\_processor



lambda\_benchmarks\_scraper

lambda\_benchmarks\_processor



lambda\_fx\_ingest

lambda\_fx\_processor



contract\_router

```



Each function has a single responsibility, making the architecture easier to maintain and troubleshoot.



\---



\## 🧊 Snowflake Integration



The jobs processor connects securely to Snowflake and loads transformed USAJOBS records.



Snowflake credentials are stored in \*\*AWS Secrets Manager\*\* rather than hardcoded into the application source.



A dedicated service user is used by the Lambda integration.



The MERGE-based load provides idempotency, so repeated ingestion of the same USAJOBS postings updates existing records instead of generating duplicates.



\---



\## 🗄️ SQL Server Integration



FX data is persisted to Amazon RDS for SQL Server.



Target:



```text

RDS SQL Server

&#x20;  ↓

HCM\_DB

&#x20;  ↓

dbo.fx\_rates

```



The FX processor only removes an SQS message after successful downstream processing. Failed messages therefore remain available for retry according to SQS visibility behavior.



\---



\## 🧩 AWS Glue Implementation Note



The requested architecture specified:



```text

jobs-queue.fifo

&#x20;      ↓

AWS Glue

&#x20;      ↓

Snowflake

```



A PySpark Glue ETL script was prepared:



```text

glue\_jobs/jobs\_glue.py

```



and uploaded to:



```text

s3://data-hackathon-smit-taha/scripts/jobs\_glue.py

```



A dedicated Glue IAM role was also configured.



However, AWS rejected both crawler and job creation at the \*\*account level\*\* with:



```text

AccessDeniedException:

Account 619183773524 is denied access.

```



This restriction also occurred while testing with elevated account permissions, indicating an account/service-level limitation rather than a normal Glue IAM policy issue.



AWS Support case:



```text

179282312200930

```



To keep the hackathon pipeline fully operational, `lambda\_jobs\_processor` was implemented as the processing fallback and successfully loads transformed records into Snowflake.



The Glue PySpark implementation remains included in the repository to demonstrate the intended Glue processing design.



\---



\## 🛡️ Reliability Design



The architecture includes several reliability mechanisms:



\- S3 provides durable raw data storage.

\- SNS decouples object creation from routing.

\- FIFO SQS queues buffer messages between components.

\- Processors delete queue messages only after successful processing.

\- Snowflake MERGE prevents duplicate job records.

\- Source-specific queues isolate failures.

\- BLS includes a documented fallback strategy.

\- Raw data remains available for reprocessing.



\---



\## 🔐 Security



Sensitive credentials are not hardcoded in the repository.



The implementation uses:



\- Lambda environment variables for runtime configuration.

\- AWS Secrets Manager for Snowflake credentials.

\- IAM roles for AWS service access.

\- `.gitignore` to exclude credentials, local dependency builds, deployment packages, and temporary files.



The repository excludes:



```text

.env

.env.\*

\*secret\*.json

\*credentials\*.json

\*.pem

\*.key

package/

layer\_build/

\*.zip

\*-response.json

```



> \*\*Note:\*\* Hackathon infrastructure was optimized for rapid implementation. Production deployment should apply least-privilege IAM policies, private database networking, Secrets Manager for all database credentials, DLQs, monitoring alarms, and stricter security-group rules.



\---



\## 🧰 Technologies Used



| Category | Technologies |

|---|---|

| Cloud | AWS |

| Scheduling | Amazon EventBridge |

| Compute | AWS Lambda |

| Storage | Amazon S3 |

| Messaging | Amazon SNS, Amazon SQS FIFO |

| ETL | Python, AWS Glue / PySpark implementation |

| Data Warehouse | Snowflake |

| Database | Amazon RDS SQL Server |

| Secrets | AWS Secrets Manager |

| APIs | USAJOBS, Open Exchange Rates |

| Web Extraction | Requests / BeautifulSoup |

| Monitoring | Amazon CloudWatch |

| CLI | AWS CLI |



\---



\## 📂 Repository Structure



```text

polarapt-hcm-hackathon/

│

├── jobs\_ingest/

│   └── lambda\_function.py

│

├── benchmarks\_scraper/

│   └── lambda\_function.py

│

├── fx\_ingest/

│   └── lambda\_function.py

│

├── contract\_router/

│   └── lambda\_function.py

│

├── jobs\_processor/

│   └── lambda\_function.py

│

├── benchmarks\_processor/

│   └── lambda\_function.py

│

├── fx\_processor/

│   └── lambda\_function.py

│

├── glue\_jobs/

│   └── jobs\_glue.py

│

├── screenshots/

│

├── \*.json

├── .gitignore

└── README.md

```



\---



\## 📸 Implementation Evidence



\### EventBridge — Every-Minute Scheduling



!\[EventBridge Rules](screenshots/01-eventbridge-rules-part1.png)



!\[EventBridge Jobs](screenshots/02-eventbridge-jobs.png)



\### AWS Lambda Functions



!\[Lambda Functions](screenshots/06-lambda-functions.png)



\### S3 Raw Data



!\[S3 Raw Zone](screenshots/03-s3-raw.png)



\### S3 Processed Data



!\[S3 Processed Zone](screenshots/04-s3-processed.png)



\### Lambda Layers / S3 Supporting Files



!\[S3 Layers](screenshots/05-s3-layers.png)



\### SNS Topics



!\[SNS Topics](screenshots/07-sns-topics.png)



\### SQS FIFO Queues



!\[SQS Queues](screenshots/08-sqs-queues.png)



\---



\## ✅ Implemented Features



\- \[x] USAJOBS ingestion

\- \[x] BLS benchmark ingestion with resilient fallback

\- \[x] Open Exchange Rates ingestion

\- \[x] One-minute EventBridge schedules

\- \[x] S3 raw data lake

\- \[x] Contract metadata

\- \[x] Standard SNS raw-event topic

\- \[x] Contract Router Lambda

\- \[x] SNS FIFO routing topic

\- \[x] Source-based SNS filtering

\- \[x] Three SQS FIFO queues

\- \[x] Jobs processing

\- \[x] Benchmarks processing

\- \[x] FX processing

\- \[x] Snowflake integration

\- \[x] SQL Server RDS integration

\- \[x] S3 processed benchmark output

\- \[x] Snowflake idempotent MERGE

\- \[x] CloudWatch execution logging

\- \[x] Glue PySpark ETL implementation prepared

\- \[x] Secure secret handling for Snowflake



\---



\## 🚧 Production Improvements



Given additional production time, the next improvements would include:



\- Dead-letter queues for failed events.

\- CloudWatch alarms and centralized monitoring.

\- Least-privilege IAM policies.

\- All credentials stored in Secrets Manager.

\- Private RDS networking.

\- Infrastructure as Code using Terraform.

\- Automated deployment pipeline.

\- Automated schema validation.

\- Retry/backoff logic for external APIs.

\- Glue deployment once AWS account access is restored.



\---



\## 🎯 Key Outcome



The project demonstrates a complete event-driven data engineering architecture where three independent external HR data feeds are ingested continuously, stored in a raw data lake, routed through a metadata-driven messaging layer, and processed into different downstream systems.



```text

USAJOBS → Snowflake

BLS → Processed S3

Open Exchange Rates → SQL Server

```



The architecture is modular, asynchronous, source-aware, and designed for extension to additional HCM data feeds.



\---



\## 👤 Author



\*\*Muhammad Taha Khan\*\*



SMIT Cloud Data Engineering  

Data Engineering Hackathon — October 2026

