import streamlit as st
from google.cloud import bigquery


def extract_schema(bq_client, tables):
    """
    Extract schema information for the specified tables using INFORMATION_SCHEMA.

    Args:
        bq_client: BigQuery client
        tables: List of dictionaries with dataset and table names

    Returns:
        Dictionary with table schemas and metadata
    """
    schema_info = {}

    for item in tables:
        dataset_id = item["dataset"]
        table_id = item["table"]
        full_table_id = f"{dataset_id}.{table_id}"

        try:
            # Get table metadata
            table_ref = bq_client.get_table(f"{dataset_id}.{table_id}")

            # Extract basic table info with fully qualified name
            table_info = {
                "project_id": bq_client.project,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "full_name": f"{bq_client.project}.{dataset_id}.{table_id}",
                "num_rows": table_ref.num_rows,
                "created": table_ref.created.isoformat() if table_ref.created else None,
                "description": table_ref.description or "",
                "columns": []
            }

            # Get column information directly from the table schema
            for field in table_ref.schema:
                column_info = {
                    "name": field.name,
                    "type": field.field_type,
                    "nullable": not field.mode or field.mode == "NULLABLE",
                    "default": None,
                    "description": field.description or ""
                }
                table_info["columns"].append(column_info)

            try:
                sample_query = f"""
                SELECT * FROM `{bq_client.project}.{dataset_id}.{table_id}` LIMIT 5
                """
                sample_job = bq_client.query(sample_query)
                sample_results = sample_job.result()

                sample_data = []
                for row in sample_results:
                    sample_data.append(dict(row.items()))

                table_info["sample_data"] = sample_data
            except Exception as e:
                st.warning(f"Could not fetch sample data for {full_table_id}: {str(e)}")
                table_info["sample_data"] = []

            # Store in schema_info dictionary
            schema_info[full_table_id] = table_info

        except Exception as e:
            st.error(f"Error fetching schema for {full_table_id}: {str(e)}")

    return schema_info


def format_schema_for_prompt(schema_info, project_id):
    """
    Format schema information into a string for inclusion in the prompt.

    Args:
        schema_info: Dictionary with table schemas
        project_id: GCP project ID

    Returns:
        Formatted schema string
    """
    schema_text = f"Available tables and their schemas in project '{project_id}':\n\n"

    for table_name, table_info in schema_info.items():
        schema_text += f"Table: {table_info['full_name']}\n"
        schema_text += f"  - Project: {table_info['project_id']}\n"
        schema_text += f"  - Dataset: {table_info['dataset_id']}\n"
        schema_text += f"  - Table: {table_info['table_id']}\n"
        schema_text += f"  - Row count: {table_info['num_rows']}\n"

        if table_info['description']:
            schema_text += f"Description: {table_info['description']}\n"

        schema_text += "Columns:\n"

        for column in table_info['columns']:
            schema_text += f"  - {column['name']} ({column['type']})"

            if column['description']:
                schema_text += f": {column['description']}"

            schema_text += "\n"

        schema_text += "\nSample data (first 5 rows):\n"

        if table_info['sample_data']:
            col_widths = {}
            for col in table_info['columns']:
                col_name = col['name']
                col_widths[col_name] = len(col_name)

                for row in table_info['sample_data']:
                    if col_name in row:
                        col_widths[col_name] = max(col_widths[col_name], len(str(row[col_name])))

            header = "  "
            for col in table_info['columns']:
                col_name = col['name']
                header += col_name.ljust(col_widths.get(col_name, 15) + 2)

            schema_text += header + "\n"

            separator = "  " + "-" * (sum(col_widths.values()) + len(col_widths) * 2) + "\n"
            schema_text += separator

            for row in table_info['sample_data']:
                row_text = "  "
                for col in table_info['columns']:
                    col_name = col['name']
                    if col_name in row:
                        row_text += str(row[col_name]).ljust(col_widths.get(col_name, 15) + 2)
                    else:
                        row_text += "NULL".ljust(col_widths.get(col_name, 15) + 2)

                schema_text += row_text + "\n"
        else:
            schema_text += "  (No sample data available)\n"

        schema_text += "\n\n"

    return schema_text