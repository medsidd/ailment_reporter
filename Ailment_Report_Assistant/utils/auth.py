import streamlit as st
from google.api_core.exceptions import PermissionDenied, NotFound


def verify_bq_access(bq_client, project_id, tables):
    """
    Verify that the user has access to the specified project, datasets, and tables.

    Args:
        bq_client: BigQuery client
        project_id: GCP project ID
        tables: List of dictionaries with dataset and table names

    Returns:
        Dictionary with success flag and error message if any
    """
    try:
        # Check project access first
        bq_client.get_service_account_email()

        # Check access to each dataset and table
        for item in tables:
            dataset_id = item["dataset"]
            table_id = item["table"]

            # Try to get dataset
            try:
                bq_client.get_dataset(f"{project_id}.{dataset_id}")
            except NotFound:
                return {
                    "success": False,
                    "error": f"Dataset {dataset_id} not found in project {project_id}."
                }
            except PermissionDenied:
                return {
                    "success": False,
                    "error": f"Permission denied for dataset {dataset_id} in project {project_id}."
                }

            # Try to get table
            try:
                bq_client.get_table(f"{project_id}.{dataset_id}.{table_id}")
            except NotFound:
                return {
                    "success": False,
                    "error": f"Table {table_id} not found in dataset {dataset_id}."
                }
            except PermissionDenied:
                return {
                    "success": False,
                    "error": f"Permission denied for table {table_id} in dataset {dataset_id}."
                }

        # All checks passed
        return {
            "success": True,
            "error": None
        }

    except Exception as e:
        return {
            "success": False,
            "error": f"Error verifying access: {str(e)}"
        }
