# TTD reporting
Create a merged TTD and DCM report with campaign metrics and conversion count for Tableau and Datorama use

## Environment:
Airflow on Cloud Compute Engine

## Tasks:
1. uaf_ttd_api.py: TTD report through API to Cloud storage
2. uaf_main.py: clean and merge

## Requirement:
1. TTD auth token
2. DCM client secret json
3. Airflow dags
