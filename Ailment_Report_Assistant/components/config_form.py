import streamlit as st


def display_config_form():
    """
    Display the configuration form in the sidebar.
    """
    # Project ID input
    st.session_state.gcp_project = st.text_input(
        "GCP Project ID",
        value=st.session_state.gcp_project,
        help="The Google Cloud project ID that contains your BigQuery datasets and tables"
    )

    st.markdown("---")

    # Display existing dataset/table inputs
    st.write("### Datasets and Tables")
    st.write("Enter at least one dataset and table name:")

    for i, item in enumerate(st.session_state.tables):
        # Add some vertical spacing between entries
        if i > 0:
            st.markdown("---")

        # Dataset label and input
        dataset_label = f"Dataset {i + 1}"
        table_label = f"Table {i + 1}"

        # Dataset input
        st.text_input(
            dataset_label,
            value=item["dataset"],
            key=f"dataset_{i}",
            on_change=lambda idx=i, field="dataset": update_table(idx, field)
        )

        # Table input
        st.text_input(
            table_label,
            value=item["table"],
            key=f"table_{i}",
            on_change=lambda idx=i, field="table": update_table(idx, field)
        )

        # Remove button (not for the first table)
        if i > 0:
            st.button("🗑️ Remove", key=f"remove_{i}", on_click=remove_table, args=(i,))

    st.button("➕ Add another table", on_click=add_table)

    # Add a separator
    st.markdown("---")

    # API key input (with warning about security)
    gemini_api_key = st.text_input(
        "Gemini API Key",
        type="password",
        help="Your Gemini API key. The entered key will override the default key set in the .env file."
    )

    if gemini_api_key:
        st.warning(
            "⚠️ It's safer to set your API key in the .env file rather than entering it here. "
        )
        # Set environment variable (will not persist between sessions)
        import os
        os.environ["GEMINI_API_KEY"] = gemini_api_key

    # Display schema info if available
    if st.session_state.initialized and st.session_state.schema_info:
        st.markdown("---")
        with st.expander("Schema Information", expanded=False):
            # Display table information
            for table_name, table_info in st.session_state.schema_info.items():
                st.subheader(f"📊 {table_name}")
                st.caption(f"Rows: {table_info['num_rows']:,}")

                if table_info['description']:
                    st.write(table_info['description'])

                # Display columns
                cols_df = {
                    "Column": [],
                    "Type": [],
                }

                for col in table_info['columns']:
                    cols_df["Column"].append(col['name'])
                    cols_df["Type"].append(col['type'])

                st.dataframe(cols_df, hide_index=True)
                st.markdown("---")


def update_table(index, field):
    """
    Update a table entry in session state when input changes.
    """
    if field == "dataset":
        st.session_state.tables[index]["dataset"] = st.session_state[f"dataset_{index}"]
    elif field == "table":
        st.session_state.tables[index]["table"] = st.session_state[f"table_{index}"]


def add_table():
    """
    Add a new table entry to the list.
    """
    st.session_state.tables.append({"dataset": "", "table": ""})


def remove_table(index):
    """
    Remove a table entry from the list.
    """
    if index < len(st.session_state.tables) and index > 0:
        st.session_state.tables.pop(index)