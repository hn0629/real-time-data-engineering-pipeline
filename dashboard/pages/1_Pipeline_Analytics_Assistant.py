from pathlib import Path
import sys

import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from llm.assistant import answer


st.set_page_config(
    page_title="Pipeline Analytics Assistant",
    page_icon="🤖",
    layout="wide",
)

st.title("🤖 Pipeline Analytics Assistant")

st.caption(
    "Read-only assistant for pipeline health and local Analytics "
    "Parquet summaries. It cannot modify data, control services, "
    "run arbitrary SQL, or provide financial advice."
)

with st.expander("Supported questions", expanded=True):
    st.markdown(
        """
- `Summarize the latest pipeline output`
- `What is the latest price for AAPL?`
- `Show latest prices for AAPL, MSFT, and NVDA`
- `Give me AAPL summary for 2026-09-15`
        """
    )

if "assistant_messages" not in st.session_state:
    st.session_state.assistant_messages = []

for message in st.session_state.assistant_messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

question = st.chat_input(
    "Ask about pipeline health or stock-price analytics"
)

if question:
    st.session_state.assistant_messages.append(
        {
            "role": "user",
            "content": question,
        }
    )

    with st.chat_message("user"):
        st.markdown(question)

    response = answer(question)

    st.session_state.assistant_messages.append(
        {
            "role": "assistant",
            "content": response,
        }
    )

    with st.chat_message("assistant"):
        st.markdown(response)

left_column, right_column = st.columns([1, 4])

with left_column:
    if st.button("Clear chat", use_container_width=True):
        st.session_state.assistant_messages = []
        st.rerun()

with right_column:
    st.caption(
        "Data source: local monitoring status and Analytics Parquet "
        "output. Responses are informational only."
    )