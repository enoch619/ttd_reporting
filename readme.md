# TTD reporting
When running performance campaign and ad served on DCM, DSP metrics have to combine with DCM conversion.  These scripts utilise our existing pipeline on Airflow and to create a merged TTD and DCM report for analytics and report use on Datorama and Tableau.

## Environment:
Airflow on Cloud Compute Engine

## Task Sequence:
1. uaf_ttd_api.py: TTD report through API to Cloud storage
2. uaf_main.py: clean and merge, re-upload to Cloud storage

## Requirement:
1. TTD auth token
2. TTD scheduled report ID
3. Google Cloud client secret json
4. Airflow dags
