# DBT Template

__Data Science Team, Lancashire Teaching Hospitals NHS Foundation Trust__

## Introduction

The Data Science Team and LAncs Teaching Hospitals has settled on using the data build tool (dbt) for structuring ETL (Extract, Transfrom, Load) workflows.

## Requirements

## Instructions

1. Clone the repository.

2. Create a ```.dbt``` folder in your user profile and create a file called ```profiles.yml```

3. Copy the following into the file:

```
dbt_template:
  target: dev
  outputs:
    dev:
      type: sqlserver
      driver: 'ODBC Driver 17 for SQL Server' # (The ODBC Driver installed on your system)
      server: YOURSQLSERVER
      port: 1433
      database: dbt_template
      schema: dbo
      windows_login: True
      trust_cert: True
      threads: 4
```

4. Run ```dbt deps``` 