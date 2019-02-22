# Dataflow Transactions Demo

This is a demo that will guide you through setting up a Dataflow pipeline for simulating streaming mock banking transactions to BigQuery. The demo includes de-identification of these transactions utilising Google's DLP API.

It exists for education purposes only with no warranty for use.

## Getting Started

These instructions will help setup the pipeline in streaming mode and run an initial set of data through it. Ensure pipelines are torn down once no longer needed to control costs.

### Prerequisites

What things you need to install the software and how to install them

```
Install Google Cloud SDK
Install Python 2.7
Create a Virtual Environment (Optional)
Install packages as per requirements.txt (pip install -r requirements.txt)
```

To run the pipeline you must perform the following:

```
Create a Google Cloud Project
Enable the following APIs: PubSub, BigQuery, Dataflow & DLP API
Create a Service Account with the following roles: Dataflow Admin, Pub/Sub Admin and BigQuery Admin
Create a temp and staging GCS bucket for the Dataflow Pipeline (these can be the same bucket)
Create a PubSub Topic for to receive the raw transactions
Create a BigQuery Dataset to receive processed transactions
```

### Installing

A step by step series of examples that tell you how to get the pipeline running

Export Service Account Keys for a Service Account that has Dataflow Admin, Pub/Sub Admin and BigQuery Admin and set the path to the key as an environment variable.

```
export GOOGLE_APPLICATION_CREDENTIALS=<path to key>
```

Start the Dataflow Pipeline. Note, you do not need to create the table, the pipeline will create it automatically.

```
python transaction_processor.py --input_topic=projects/<PROJECT_ID>/topics/<TOPIC_NAME> --temp_location gs://<TEMP_GCS_BUCKET>/ --staging_location gs://<STAGING_GCS_BUCKET/ --output_table "<PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>" --project "<PROJECT_ID>" --runner DataflowRunner --job_name transaction-processor-`date '+%Y%m%d%H%M%S'`
```

Now stream the mock dataset into PubSub

```
python datapump.py --project "<PROJECT_ID>" --input_topic=projects/<PROJECT_ID>/topics/<TOPIC_NAME>
```

## Running the tests

Once the data has been streamed in, you can run some BigQuery queries to do some basic analysis on the processed transactions

List all transactions (limit 1000)

```
#standardSQL
SELECT * FROM `<DATASET_NAME>.<TABLE_NAME>` 
LIMIT 1000
```

List all the total value of transactions that happened in the United States, per city

```
#standardSQL
SELECT 
  city,
  SUM(transaction_amount) AS total_transaction_value
FROM <DATASET_NAME>.<TABLE_NAME>
GROUP BY country, city
HAVING country = 'United States'
```

List all transactions that happened in 2018

```
#standardSQL
SELECT * 
FROM `<DATASET_NAME>.<TABLE_NAME>` 
WHERE transaction_time
      BETWEEN "2018-01-01" AND "2018-12-31"
```

List the total transaction value per month in 2018, highest to lowest

```
#standardSQL
SELECT FORMAT_DATETIME('%B', DATETIME(transaction_time)) AS transaction_month, SUM(transaction_amount) as total_transaction_value
FROM `<DATASET_NAME>.<TABLE_NAME>`
WHERE transaction_time
      BETWEEN "2018-01-01" AND "2018-12-31"
GROUP BY transaction_month
ORDER BY total_transaction_value DESC
```

List which first name performs the highest number of transactions

```
#standardSQL
SELECT first_name, COUNT(first_name) as first_name_count
FROM `transactions.deposits`
GROUP BY first_name
ORDER BY first_name_count DESC
```

## Built With

* [Apache Beam](https://beam.apache.org/) - Batch and streaming data processing and execution engine
* [Google Cloud DLP API](https://cloud.google.com/dlp/) - Google's Data Loss Prevention API

## Authors

* **Sourced Group** - *Initial release* - [Sourced Group](https://www.sourcedgroup.com)

## License

This project is licensed under the Apache License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Mockaroo - https://mockaroo.com/
