import streamlit as st
import pandas as pd
from utils.gemini import process_user_query


def display_chat_history():
    """
    Display the chat history in the main area.
    """
    chat_container = st.container()

    with chat_container:
        for i, entry in enumerate(st.session_state.chat_history):
            if "metadata" in entry:
                continue

            # Display user message
            if "user" in entry:
                st.chat_message("user").write(entry["user"])

            # Display assistant response
            if "assistant" in entry:
                assistant_msg = st.chat_message("assistant")

                # Display understanding
                if "understanding" in entry["assistant"] and entry["assistant"]["understanding"]:
                    assistant_msg.write(entry["assistant"]["understanding"])

                # Display SQL query in code block
                if "sql" in entry["assistant"] and entry["assistant"]["sql"]:
                    with assistant_msg:
                        st.code(entry["assistant"]["sql"], language="sql", line_numbers=True)

                # Display query results if available
                if "query_result" in entry["assistant"] and entry["assistant"]["query_result"]["success"]:
                    with assistant_msg:
                        result = entry["assistant"]["query_result"]

                        # Show query statistics
                        stats = result["stats"]
                        st.success(
                            f"✅ Query executed successfully\n\n"
                            f"- Rows: {stats['rows']:,}\n"
                            f"- Execution time: {stats['execution_time_ms']:.2f} seconds\n"
                            f"- Bytes processed: {stats['bytes_processed']:,}"
                        )

                        # Show result dataframe
                        if result["data"]:
                            if isinstance(result["data"], list):
                                df = pd.DataFrame(result["data"], columns=result.get("columns", None))
                            else:
                                df = pd.DataFrame(result["data"])

                            if len(df) > 100:
                                st.write(f"Showing first 100 of {len(df)} rows:")
                                st.dataframe(df.head(100), use_container_width=True)
                            else:
                                st.dataframe(df, use_container_width=True)
                        else:
                            st.info("The query returned no results.")

                # Display error if query failed
                if "error" in entry["assistant"] and entry["assistant"]["error"]:
                    with assistant_msg:
                        st.error(f"❌ Query Error: {entry['assistant']['error']}")

                # Display explanation
                if "explanation" in entry["assistant"] and entry["assistant"]["explanation"]:
                    assistant_msg.write(entry["assistant"]["explanation"])


def display_user_input():
    """
    Display the user input area and process new messages.
    """
    # Initialize session state for storing query process state
    if "processing_query" not in st.session_state:
        st.session_state.processing_query = False

    if "current_input" not in st.session_state:
        st.session_state.current_input = ""

    # Check if we need to process a query from a previous interaction
    if st.session_state.processing_query:
        with st.spinner("Generating response..."):
            query_text = st.session_state.current_input

            # Reset state
            st.session_state.current_input = ""
            st.session_state.processing_query = False

            # Add user message to chat history
            st.session_state.chat_history.append({"user": query_text})

            assistant_response = process_user_query(query_text)

            st.session_state.chat_history.append({"assistant": assistant_response})

            st.rerun()

    with st.form(key="query_form", clear_on_submit=True):
        # Input field
        user_input = st.text_area(
            "Ask a question about your BigQuery data:",
            height=100,
            key="user_input_field",
            help="Enter a question in natural language about your data"
        )

        # Submit button
        submit_button = st.form_submit_button("Send", use_container_width=True, type="primary")

        if submit_button and user_input:
            st.session_state.current_input = user_input
            st.session_state.processing_query = True

            st.rerun()

    # Display example questions
    with st.expander("Example questions", expanded=False):
        examples = [
            "How many rows are in each table?",
            "Which stages have the best/worst survival rates?",
            "How many patients were diagnosed with each stage?",
            "Find patients who are over 65 years old with Stage III cancer."
        ]

        example_cols = st.columns(2)

        for i, example in enumerate(examples):
            with example_cols[i % 2]:
                if st.button(example, key=f"example_{i}"):
                    st.session_state.current_input = example
                    st.session_state.processing_query = True
                    st.rerun()