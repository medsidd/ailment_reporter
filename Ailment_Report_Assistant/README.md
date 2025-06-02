# Ailment Report Assistant

## Installation

### Prerequisites

- Python 3.9+
- Google Cloud account with BigQuery access
- Gemini API key

### Setup

1. Clone this repository:
   ```bash
   git clone ---
   cd ailment-report-assistant
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   Mac: source venv/bin/activate
   Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create a `.env` file from the template:
   ```bash
   mk .env
   ```

5. Edit the `.env` file and add your Gemini API key.\
   ```bash
   # Gemini API key
   GEMINI_API_KEY=---

   ```

6. Set up Google Cloud Authentication:
   ```bash
   gcloud auth application-default login
   ```

## Usage

1. Start the Streamlit application:
   ```bash
   streamlit run app.py
   ```

2. Open your browser at `http://localhost:8501`

3. Enter your Google Cloud project ID, dataset, and table names

4. Click "Start New Chat" to initialize

5. Ask questions about your BigQuery tables in natural language

## Project Structure

```
ailment_report-assistant/
├── app.py                  # Main Streamlit application
├── utils/                  # Utility modules
│   ├── auth.py             # Authentication helpers
│   ├── bq_client.py        # BigQuery operations
│   ├── gemini.py           # Gemini API integration
│   └── schema.py           # Schema handling
├── components/             # UI components
│   ├── chat_interface.py   # Chat display and input
│   └── config_form.py      # Configuration form
├── .env                    # Environment variables
└── requirements.txt        # Dependencies
```
