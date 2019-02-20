# TTD reporting
Create a merged TTD and DCM report with campaign metrics and conversion count for Tableau and Datorama use

## Steps:
1. Download TTD report through API to Cloud storage with uaf_ttd_api.py
2. Download, clean and merge with uaf_main.py
3. Put them on airflow for automation

## Requirement:
1. TTD auth token
2. DCM client secret json
3. Airflow dags
