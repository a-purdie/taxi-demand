# taxi-demand

This project is a work in progress. The plan is to forecast ride demand in NYC using public data the city publishes on its website every month. A full CI/CD implementation that automatically ingests new data as it's published and retrains accordingly, all deployed on Google Cloud Platform, is the end goal.

## What's implemented
- A data ingestion script that downloads the `.parquet` files from the TLC Trip Record Data page and uploads those files to a local Postgres instance. It's set up this way just for now for local model development, but eventually this will be modified to use GCP's BigQuery.

## What's coming next
- A notebook with a model trained on a small slice of the dataset (it's huge and messy). Will probably test out a few different methodologies and compare.
