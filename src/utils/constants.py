""" Module to store the constants used in dagster project.
"""
import os
from dagster._utils import file_relative_path

DBT_PROJECT_DIR = file_relative_path(__file__, "../../dbt_project")
DBT_PROFILES_DIR = DBT_PROJECT_DIR + "/config/"

DBT_CONFIG = {
    "project_dir": DBT_PROJECT_DIR, 
    "profiles_dir": DBT_PROFILES_DIR, 
    "target": "prod"
}

#overide defaults with env vars:
if 'DBT_TARGET' in os.environ:
    DBT_CONFIG["target"] = os.environ.get('DBT_TARGET')

SNOWFLAKE_TRANSFORM_CONFIG = {
                            "account": {"env": "SNOWFLAKE_ACCOUNT"},
                            "user": {"env": "SNOWFLAKE_USER"},
                            "authenticator":{"env":"SNOWFLAKE_AUTHENTICATOR"},
                            "password": {"env": "SNOWFLAKE_PASSWORD"},
                            "warehouse": {"env": "SNOWFLAKE_WAREHOUSE"},
                            "role": {"env": "SNOWFLAKE_ROLE"},
                            "database": "TRANSFORM"
                        }

SNOWFLAKE_RAW_CONFIG = {
                            "account": {"env": "SNOWFLAKE_ACCOUNT"},
                            "user": {"env": "SNOWFLAKE_USER"},
                            "authenticator":{"env":"SNOWFLAKE_AUTHENTICATOR"},
                            "password": {"env": "SNOWFLAKE_PASSWORD"},
                            "warehouse": {"env": "SNOWFLAKE_WAREHOUSE"},
                            "role": {"env": "SNOWFLAKE_ROLE"},
                            "database": "RAW",
                        }

# Base event types
event_types_senior = [
    "M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8", "M9", "M10", "M11",
    "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10", "F11"
]

event_types_junior = [
    "M1_junior", "M2_junior", "M3_junior", 
    "M4_junior", "M5_junior", "M6_junior", "M7_junior", "M8_junior", "M9_junior", "M10_junior", "M11_junior", 
    "F1_junior", "F2_junior", "F3_junior", 
    "F4_junior", "F5_junior", "F6_junior", "F7_junior", "F8_junior", "F9_junior", "F10_junior", "F11_junior"
]

# Simplified structure
disciplines = {
    "phase1": {
        "Senior": event_types_senior
    },
    "phase2": {
        "Senior": event_types_senior,
        "Junior": event_types_junior
    },
    "phase3": {
        "Senior": event_types_senior,
        "Junior": event_types_junior
    },
    "phase4": {
        "Senior Singles": ["singles/" + et for et in event_types_senior],
        "Senior Doubles": ["doubles/" + et for et in event_types_senior],
        "Senior Mixed Doubles": ["mixed_doubles/" + et for et in event_types_senior],
        "Junior Singles": ["singles/" + et for et in event_types_junior]  # Assuming Junior event types remain the same for simplicity
    }
}