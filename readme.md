# TTD reporting
When running performance campaign and ad served on DCM, DSP metrics have to combine with DCM conversion.  These scripts utilise our existing pipeline on Airflow and to create a merged TTD and DCM report for analytics and report use on Datorama and Tableau.

## Environment:
Airflow on Cloud Compute Engine

## Tasks:
1. uaf_ttd_api.py: TTD report through API to Cloud storage
2. uaf_main.py: clean and merge

## Requirement:
1. TTD auth token
2. TTD scheduled report ID
3. DCM client secret json
4. Airflow dags
