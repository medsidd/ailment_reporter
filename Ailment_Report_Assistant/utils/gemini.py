import os
import traceback
import streamlit as st
import pandas as pd
import google.generativeai as genai
from utils.schema import format_schema_for_prompt
from utils.bq_client import execute_bigquery_query


def initialize_gemini_model():
    """
    Initialize and configure the Gemini model with function calling.

    Returns:
        Initialized Gemini model or None if initialization fails
    """
    try:
        # Get API key from environment
        api_key = os.getenv("GEMINI_API_KEY")

        if not api_key:
            st.error("Gemini API key not found. Please set the GEMINI_API_KEY environment variable.")
            return None

        # Configure Gemini
        genai.configure(api_key=api_key)

        # Initialize model
        model = genai.GenerativeModel(
            model_name="gemini-2.0-flash",
            generation_config={
                "temperature": 0.2,
                "top_p": 0.95,
                "top_k": 40,
                "max_output_tokens": 8192,
            }
        )

        return model

    except Exception as e:
        st.error(f"Error initializing Gemini model: {str(e)}")
        st.error(traceback.format_exc())
        return None


def get_system_prompt(schema_info, project_id):
    """
    Generate system prompt with schema information.

    Args:
        schema_info: Dictionary with table schemas
        project_id: GCP project ID

    Returns:
        System prompt string
    """
    # Format schema information for the prompt
    schema_text = format_schema_for_prompt(schema_info, project_id)

    system_prompt = f"""You are an expert SQL assistant for Google BigQuery. Your role is to help users query their BigQuery tables using natural language.

I'll provide you with schema information about the available tables, and you'll need to:
1. Understand the user's natural language question
2. Translate it into a valid BigQuery SQL query
3. Generate a well-formatted SQL query that I will execute for you
4. Explain the results in a clear, helpful way

{schema_text}

When generating SQL:
- ALWAYS use fully qualified table names in the format `{project_id}.dataset_id.table_id`
- The current project ID is: {project_id}
- Include helpful comments in the SQL
- Be mindful of potential JOIN conditions between tables
- Use appropriate aggregations (SUM, AVG, COUNT, etc.) when needed
- Handle NULL values appropriately
- For time-based queries, use proper TIMESTAMP functions
- Optimize queries to minimize data processed when possible

In your responses:
1. First, briefly explain your understanding of the question
2. Show the SQL query you're generating with proper SQL syntax and formatting
3. After I execute the query, I'll share the results with you
4. Then provide a clear explanation of the output results from the SQL execution in simple terms. Break down the results and do an analysis to interpret the results if needed
5. Do not be overly verbose in your answers

Never make up or assume schema details that weren't provided. If you need additional information, ask the user for clarification.

Please handle the user's question step by step and think carefully about the SQL logic.
"""

    return system_prompt


def process_user_query(user_query):
    """
    Process a user query, generate SQL, and execute it.

    Args:
        user_query: User's natural language query

    Returns:
        Response from Gemini with SQL and results
    """
    try:
        if not st.session_state.gemini_model:
            return "Error: Gemini model not initialized"

        # Update current query for error tracking
        st.session_state.current_query = user_query
        st.session_state.error_count = 0

        system_prompt = get_system_prompt(st.session_state.schema_info, st.session_state.gcp_project)

        # Format chat history for context
        chat_history_text = ""
        if len(st.session_state.chat_history) > 1:
            for i, entry in enumerate(st.session_state.chat_history[-3:]):  # Last 3 exchanges
                if "user" in entry:
                    chat_history_text += f"User: {entry['user']}\n\n"
                if "assistant" in entry:
                    # Include only text portions of assistant response, not SQL or results
                    if "understanding" in entry["assistant"]:
                        chat_history_text += f"Assistant's understanding: {entry['assistant']['understanding']}\n\n"
                    if "explanation" in entry["assistant"]:
                        chat_history_text += f"Assistant's explanation: {entry['assistant']['explanation']}\n\n"

        # Prepare context including history (if any)
        if chat_history_text:
            context = f"{system_prompt}\n\nRecent conversation history:\n{chat_history_text}\n\nUser's new question: {user_query}"
        else:
            context = f"{system_prompt}\n\nUser's question: {user_query}"

        response = st.session_state.gemini_model.generate_content(context)

        result = process_text_response(response)

        # If SQL was generated, execute it
        if result["sql"]:
            sql_query = result["sql"]
            query_result = execute_bigquery_query(sql_query, return_error=True)

            if query_result["success"]:
                result["query_result"] = query_result

                result_context = (
                    f"SQL Query: {result["sql"]}\n\n"
                    f"The SQL query was executed successfully. Here are the results:\n\n"
                    f"Rows returned: {query_result['stats']['rows']}\n"
                    f"Execution time: {query_result['stats']['execution_time_ms']:.2f} seconds\n\n"
                )

                data = query_result["data"]
                columns = query_result["columns"]
                if data:
                    df = pd.DataFrame(data, columns=columns)

                    sample_rows = df.to_string(index=False)
                    result_context += f"Results:\n{sample_rows}\n\n"
                else:
                    result_context += "The query returned no results.\n\n"

                result_context += "Please provide a clear explanation of these results in relation to the user's question."

                # Get explanation from Gemini
                explanation_response = st.session_state.gemini_model.generate_content(result_context)
                explanation_text = explanation_response.text

                if explanation_text:
                    result["explanation"] = explanation_text
            else:
                st.session_state.error_count += 1

                if st.session_state.error_count <= 1:
                    # First failure - ask Gemini to fix the query
                    error_context = (
                        f"The SQL query failed with the following error:\n\n"
                        f"{query_result['error']}\n\n"
                        f"Please fix the SQL query and provide a corrected version."
                    )

                    error_response = st.session_state.gemini_model.generate_content(error_context)

                    corrected_result = process_text_response(error_response)

                    if corrected_result["sql"]:
                        corrected_sql = corrected_result["sql"]
                        corrected_query_result = execute_bigquery_query(corrected_sql, return_error=True)

                        if corrected_query_result["success"]:
                            result["sql"] = corrected_sql
                            result["query_result"] = corrected_query_result

                            result_context = (
                                f"The corrected SQL query was executed successfully. Here are the results:\n\n"
                                f"Rows returned: {corrected_query_result['stats']['rows']}\n"
                                f"Execution time: {corrected_query_result['stats']['execution_time_ms']:.2f} seconds\n\n"
                            )

                            data = corrected_query_result["data"]
                            columns = corrected_query_result["columns"]
                            if data:
                                df = pd.DataFrame(data, columns=columns)

                                sample_rows = df.head(5).to_string(index=False)
                                result_context += f"Sample results:\n{sample_rows}\n\n"
                            else:
                                result_context += "The query returned no results.\n\n"

                            result_context += "Please provide a clear explanation of these results in relation to the user's question."

                            # Get explanation from Gemini
                            explanation_response = st.session_state.gemini_model.generate_content(result_context)
                            explanation_text = explanation_response.text

                            if explanation_text:
                                result["explanation"] = explanation_text
                        else:
                            result["error"] = corrected_query_result["error"]
                            result["explanation"] = "I wasn't able to run a successful query even after attempting to fix it. Could you please rephrase your question or provide more details?"
                    else:
                        result["error"] = query_result["error"]
                        result["explanation"] = "I couldn't generate a corrected SQL query. Could you please rephrase your question or provide more details?"
                else:
                    result["error"] = query_result["error"]
                    result["explanation"] = "I wasn't able to run a successful query. Could you please rephrase your question or provide more details?"

        return result

    except Exception as e:
        st.error(f"Error in process_user_query: {str(e)}")
        st.error(traceback.format_exc())
        return {
            "understanding": "I encountered an error while processing your query.",
            "explanation": f"Error: {str(e)}",
            "error": str(e)
        }


def process_text_response(response):
    """
    Process a text response from Gemini.

    Args:
        response: Text response from Gemini

    Returns:
        Structured response
    """
    # Default structured response
    result = {
        "understanding": "",
        "sql": "",
        "explanation": "",
        "error": None
    }

    if hasattr(response, 'text'):
        text = response.text
    else:
        text = str(response)

    parts = text.split("\n\n")

    for part in parts:
        if "understanding" in part.lower() or "i understand" in part.lower() or "understand that" in part.lower():
            result["understanding"] = part
        elif "sql query:" in part.lower() or "```sql" in part.lower():
            if "```sql" in part.lower():
                sql_parts = part.split("```sql")
                if len(sql_parts) > 1:
                    sql_code = sql_parts[1].split("```")[0].strip()
                    result["sql"] = sql_code
            else:
                sql_parts = part.split("SQL Query:")
                if len(sql_parts) > 1:
                    result["sql"] = sql_parts[1].strip()
        elif "explanation:" in part.lower() or "results show" in part.lower() or "analysis:" in part.lower():
            result["explanation"] = part

    if not any(result.values()):
        result["explanation"] = text

    return result