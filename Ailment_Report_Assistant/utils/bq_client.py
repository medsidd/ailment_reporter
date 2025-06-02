import streamlit as st
import pandas as pd
import numpy as np
from google.api_core.exceptions import GoogleAPIError


def execute_bigquery_query(query, return_error=False):
    """
    Execute a BigQuery SQL query and return the results.

    Args:
        query: SQL query string
        return_error: If True, return error details instead of raising exception

    Returns:
        Dictionary with query results or error information
    """
    try:
        # Get BigQuery client from session state
        if not st.session_state.bq_client:
            return {
                "success": False,
                "error": "BigQuery client not initialized",
                "data": None
            }

        # Execute query
        query_job = st.session_state.bq_client.query(query)

        # Wait for query to finish
        results = query_job.result()

        # Convert results to DataFrame
        df = results.to_dataframe()

        # Convert DataFrame to dictionary with special handling for non-serializable objects
        data_dict = []
        for _, row in df.iterrows():
            row_dict = {}
            for col in df.columns:
                value = row[col]
                # Convert timedelta to string
                if hasattr(value, 'total_seconds'):
                    row_dict[col] = str(value)
                # Convert NumPy types to Python native types
                elif isinstance(value, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64)):
                    row_dict[col] = int(value)
                elif isinstance(value, (np.float_, np.float16, np.float32, np.float64)):
                    row_dict[col] = float(value)
                elif isinstance(value, np.bool_):
                    row_dict[col] = bool(value)
                else:
                    row_dict[col] = value
            data_dict.append(row_dict)

        # Calculate execution time in seconds
        execution_time_seconds = (query_job.ended - query_job.started).total_seconds()

        # Return success with data
        return {
            "success": True,
            "error": None,
            "data": data_dict,
            "columns": list(df.columns),
            "stats": {
                "bytes_processed": query_job.total_bytes_processed,
                "bytes_billed": query_job.total_bytes_billed,
                "execution_time_ms": execution_time_seconds,
                "slot_millis": query_job.slot_millis,
                "rows": results.total_rows,
                "columns": len(df.columns) if not df.empty else 0
            }
        }

    except GoogleAPIError as e:
        # Handle BigQuery-specific errors
        error_message = str(e)

        if return_error:
            return {
                "success": False,
                "error": error_message,
                "data": None
            }
        else:
            raise

    except Exception as e:
        # Handle general errors
        error_message = str(e)

        if return_error:
            return {
                "success": False,
                "error": error_message,
                "data": None
            }
        else:
            raise


def format_query_results(results):
    """
    Format query results for display.

    Args:
        results: Dictionary with query results

    Returns:
        Formatted results string
    """
    if not results["success"]:
        return f"❌ Query Error: {results['error']}"

    # Create DataFrame from data and columns
    if "data" in results and results["data"]:
        df = pd.DataFrame(results["data"], columns=results.get("columns", None))
    else:
        df = pd.DataFrame()

    stats = results["stats"]

    stats_text = (
        f"📊 **Query Statistics**\n"
        f"- Rows: {stats['rows']}\n"
        f"- Columns: {stats['columns']}\n"
        f"- Bytes processed: {stats['bytes_processed']:,} bytes\n"
        f"- Execution time: {stats['execution_time_ms']:.2f} seconds\n"
    )

    if df.empty:
        data_text = "Query returned no results."
    else:
        # Limit to reasonable number of rows for display
        if len(df) > 100:
            display_df = df.head(100)
            data_text = f"Showing first 100 of {len(df)} rows:"
        else:
            display_df = df
            data_text = f"Results ({len(df)} rows):"

    return {
        "statistics": stats_text,
        "data_info": data_text,
        "dataframe": df
    }