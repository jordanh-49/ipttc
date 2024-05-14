<br/>
<p align="center">
  <h3 align="center">Para Table-Tennis Pipeline</h3>

  <p align="left">
    This repository hosts the Para Table-Tennis Pipeline, leveraging Dagster for orchestration, Python for extraction, dbt Core for data transformations, and Snowflake as the data warehousing solution procesing match results and player profiles data from the IPTTC website.
    <br/>
    <br/>
  </p>
</p>

<p align="center">
  <img src="https://img.shields.io/github/downloads/jordanh-49/ipttc/total">
  <img src="https://img.shields.io/github/stars/jordanh-49/ipttc?style=social">
  <img src="https://img.shields.io/github/license/jordanh-49/ipttc">
</p>

## Tech Stack
- **Dagster**: Orchestrates workflows and manages dependencies between tasks.
- **Python**: Extracts data from sources.
- **dbt Core**: Transforms data within Snowflake, preparing it for analytics.
- **Snowflake**: Serves as the scalable cloud data warehouse.

## Dagster Assets and dbt Models

### Styling Guide

- `r_` - DBT raw models
- `t_` - DBT transform models
- `_python_raw_` - Dagster raw asset

### Asset Graph
![Asset Graph](assets/images/Job_all_assets.svg)

### dbt Models for Production
- `player_profiles`: This model produces a tbl with complete player profiles inc. impairment and demographic details.
- `match_info`: This model produces a tbl with all the relevant match details inc. phase, time, location, set scores and members.
- `match_by_player_by_set_wide`: This model produces a tbl with all the match info + a column for each set result and outcome by player by team.
- `match_by_team_by_set_long`: This model produces a tbl with all the match info and results by team by set.
- `match_by_team_by_set_wide`: This model produces a tbl with all the match info + a column for each set result and outcome by team.


## Getting Started

### Prerequisites
- Python 3.10 or higher
- Snowflake account
- dbt Core installed locally

### Setup

#### 1. Clone the Repository

Clone this repository to your local machine to get started:

```bash
git clone https://github.com/jordanh-49/ipttc.git
cd template
```

#### 2. Install PDM
PDM (Python Development Master) is used for managing project dependencies.
Install PDM if you haven't already:

```bash
pip install pdm
```

#### 3. Create a Virtual Environment and Install Dependencies
Use PDM to create a new virtual environment for this project and install the required libraries/dependencies defined in pyproject.toml (follow prompts in console):
```bash
pdm install
```

#### 4. Activate Virtual environment
Activate the virtual environment with the following command:
```bash
source .venv/bin/activate
```

#### 5. Set Env variables
Create a `.env` file using the `env.sample` as reference (use personal Snowflake account for local dev)
```
SNOWFLAKE_ACCOUNT="xy12345.ap-southeast-2"
SNOWFLAKE_USER="<service_account_user>"
SNOWFLAKE_PASSWORD="<service_account_password>"
SNOWFLAKE_WAREHOUSE="your_default_wh"
SNOWFLAKE_AUTHENTICATOR="<optional_authenticator>"
SNOWFLAKE_ROLE="<your_role>"
DBT_PROFILE_DIR="dbt_project/config/profiles.yml"
DBT_TARGET="dev"
```

### Running the Pipeline locally

1. To launch the Dagster control panel, execute the following command in the console:
 ```bash
dagster dev
 ```
2. To access the control panel, navigate to **http://localhost:3000** in your web browser (default port)

## License

Distributed under the MIT License. See [LICENSE](https://github.com/jordanh-49/portfolio/blob/main/LICENSE.md) for more information.

## Acknowledgements

* [IPTTC Website](https://www.ipttc.org/)

