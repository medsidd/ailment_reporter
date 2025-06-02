import os
import json
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, PermissionDenied, NotFound
import google.generativeai as genai

from utils.auth import verify_bq_access
from utils.schema import extract_schema
from utils.bq_client import execute_bigquery_query
from utils.gemini import initialize_gemini_model
from components.chat_interface import display_chat_history, display_user_input
from components.config_form import display_config_form

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="AIlment Reports",
    page_icon="ascension_logo.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if "initialized" not in st.session_state:
    st.session_state.initialized = False
    st.session_state.gcp_project = ""
    st.session_state.tables = [{"dataset": "", "table": ""}]
    st.session_state.schema_info = {}
    st.session_state.chat_history = []
    st.session_state.bq_client = None
    st.session_state.gemini_model = None
    st.session_state.error_count = 0
    st.session_state.current_query = ""

# Add view mode flag
if "view_mode" not in st.session_state:
    st.session_state.view_mode = False

# App header
st.title("🏥📋 AIlment Report Assistant")
st.markdown("""
Ask questions about your clinical data in natural language.
This assistant will generate SQL, execute it, and explain the results.
""")

# Sidebar for configuration
with st.sidebar:
    st.header("Configuration")

    # Display configuration form
    display_config_form()

    # Initialize chat button
    if st.button("Start New Chat", type="primary"):
        # Reset view mode
        st.session_state.view_mode = False

        # Save previous chat history if exists
        if len(st.session_state.chat_history) > 0:
            # Create chats directory if it doesn't exist
            chats_dir = "chats"
            if not os.path.exists(chats_dir):
                os.makedirs(chats_dir)

            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            history_file = os.path.join(chats_dir, f"chat_history_{timestamp}.json")


            # Custom JSON encoder to handle DataFrame serialization
            class CustomJSONEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, pd.DataFrame):
                        return {
                            "__dataframe__": True,
                            "columns": list(obj.columns),
                            "data": obj.to_dict(orient="records")
                        }
                    # Handle datetime objects
                    if hasattr(obj, 'isoformat'):
                        return obj.isoformat()
                    # Handle timedelta objects
                    if isinstance(obj, timedelta):
                        return str(obj)
                    # Handle numpy numeric types
                    if isinstance(obj, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32,
                                        np.int64, np.uint8, np.uint16, np.uint32, np.uint64)):
                        return int(obj)
                    if isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
                        return float(obj)
                    if isinstance(obj, (np.complex_, np.complex64, np.complex128)):
                        return {'real': obj.real, 'imag': obj.imag}
                    if isinstance(obj, np.ndarray):
                        return obj.tolist()
                    if isinstance(obj, np.bool_):
                        return bool(obj)
                    # Let the default encoder handle the rest
                    return super().default(obj)


            # Add metadata to history
            metadata_entry = {
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "project_id": st.session_state.gcp_project,
                    "tables": st.session_state.tables
                }
            }
            st.session_state.chat_history.insert(0, metadata_entry)

            # Save chat history
            with open(history_file, "w") as f:
                json.dump(st.session_state.chat_history, f, cls=CustomJSONEncoder)

            st.sidebar.success(f"Chat history saved to {history_file}")

            # Reset chat history
            st.session_state.chat_history = []

        # Validate inputs
        if not st.session_state.gcp_project:
            st.sidebar.error("Please enter a GCP Project ID")
        elif any(not item["dataset"] or not item["table"] for item in st.session_state.tables):
            st.sidebar.error("Please enter all dataset and table names")
        else:
            with st.spinner("Validating access and fetching schema..."):
                try:
                    # Initialize BQ client
                    st.session_state.bq_client = bigquery.Client(project=st.session_state.gcp_project)

                    # Verify access
                    access_result = verify_bq_access(
                        st.session_state.bq_client,
                        st.session_state.gcp_project,
                        st.session_state.tables
                    )

                    if access_result["success"]:
                        # Extract schema
                        with st.spinner("Fetching schema information..."):
                            st.session_state.schema_info = extract_schema(
                                st.session_state.bq_client,
                                st.session_state.tables
                            )

                        # Initialize Gemini model
                        with st.spinner("Initializing AI model..."):
                            st.session_state.gemini_model = initialize_gemini_model()
                            if st.session_state.gemini_model is None:
                                st.sidebar.error("Failed to initialize Gemini model. Check your API key.")
                                st.stop()

                        st.session_state.initialized = True
                        st.sidebar.success("Chat initialized successfully!")
                    else:
                        st.sidebar.error(f"Access verification failed: {access_result['error']}")
                except Exception as e:
                    import traceback
                    st.sidebar.error(f"Error initializing chat: {str(e)}")
                    st.sidebar.code(traceback.format_exc())

    # Load previous chat
    if not st.session_state.initialized:
        st.sidebar.divider()
        st.sidebar.header("Previous Chats")

        # Create chats directory if it doesn't exist
        chats_dir = "chats"
        if not os.path.exists(chats_dir):
            os.makedirs(chats_dir)

        # List chat history files
        try:
            chat_files = [f for f in os.listdir(chats_dir) if f.startswith("chat_history_") and f.endswith(".json")]
            chat_files.sort(reverse=True)  # Most recent first

            if chat_files:
                selected_chat = st.sidebar.selectbox(
                    "Load a previous chat:",
                    ["Select a chat..."] + chat_files,
                    format_func=lambda x: x if x == "Select a chat..." else x.replace("chat_history_", "").replace(
                        ".json", "")
                )

                if selected_chat != "Select a chat..." and st.sidebar.button("Load Selected Chat"):
                    # Load the selected chat history
                    try:
                        with open(os.path.join(chats_dir, selected_chat), "r") as f:
                            loaded_history = json.load(f)

                        # Process loaded history (convert dictionaries back to DataFrames if needed)
                        for entry in loaded_history:
                            if "assistant" in entry and "query_result" in entry["assistant"]:
                                query_result = entry["assistant"]["query_result"]
                                if "data" in query_result and isinstance(query_result["data"], list):
                                    pass

                        st.session_state.chat_history = loaded_history

                        # Set initialized flag to true to display chat interface
                        if not st.session_state.initialized:
                            project_info = None
                            tables_info = []

                            # Look for any metadata in the chat history
                            for entry in loaded_history:
                                if "metadata" in entry:
                                    if "project_id" in entry["metadata"]:
                                        project_info = entry["metadata"]["project_id"]
                                    if "tables" in entry["metadata"]:
                                        tables_info = entry["metadata"]["tables"]

                            # Display metadata if found
                            if project_info:
                                st.sidebar.info(f"Project: {project_info}")
                            if tables_info:
                                table_info_str = "\n".join([f"{t['dataset']}.{t['table']}" for t in tables_info])
                                st.sidebar.info(f"Tables:\n{table_info_str}")

                            # Set viewing mode (can display history but not add new messages)
                            st.session_state.view_mode = True

                        st.sidebar.success(f"Loaded chat history from {selected_chat}")
                        st.rerun()
                    except Exception as e:
                        st.sidebar.error(f"Error loading chat history: {str(e)}")
            else:
                st.sidebar.info("No previous chats found.")
        except Exception as e:
            st.sidebar.error(f"Error listing chat history files: {str(e)}")

# Main chat area
if st.session_state.initialized or (st.session_state.view_mode and st.session_state.chat_history):
    # Display chat history
    display_chat_history()

    # Only show input area if fully initialized (not just viewing mode)
    if st.session_state.initialized and not st.session_state.view_mode:
        display_user_input()
    elif st.session_state.view_mode:
        st.info(
            "This is a loaded chat history. Configure BigQuery settings in the sidebar and click 'Start New Chat' to begin a new conversation.")
else:
    st.info("Please configure your BigQuery settings in the sidebar and click 'Start New Chat' to begin.")

# Footer
st.markdown("---")
st.markdown("AIlment Reports - CoLabathon Project")
