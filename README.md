# Neurology Informatics

__Data Science Team, Lancashire Teaching Hospitals NHS Foundation Trust__

## Introduction

Reproducible neurology informatics research centered around OMOP and related data sources.


## Getting started

1. Clone the repository
2. Create a new virtual environment using either `conda`, `pipenv` or similar and activate that environment.
3. Create a `.dbt` directory in your user profile directory (typically C:/Users/User.Name) and copy `profiles_sample.yml` into `.dbt` and rename it to `profiles.yml`. Modify the file with confguration provided by the data science team.
4. Run `pip install -r requirements.txt` followed by `dbt deps`
5. Install the [Power User for dbt Core extension for VS Code](https://marketplace.visualstudio.com/items?itemName=innoverio.vscode-dbt-power-user)
