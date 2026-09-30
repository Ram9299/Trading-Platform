import streamlit as st
import pandas as pd
import json
import time
import threading
from collections import deque
from confluent_kafka import Consumer, KafkaError
import plotly.graph_objects as go

st.set_page_config(
    page_title="Multi-Agent Trading Platform",
    page_icon="📈",
    layout="wide"
)

# Title & Status Header
st.title("⚡ Industrial Multi-Agent Futures Trading Platform")
st.caption("Live Observability Dashboard: Ingestion ➔ Indicator Engine ➔ Macro Sentiment ➔ Synthesizer ➔ Paper Execution")

# Thread-safe global message buffers (Max 200 items retained)
if "ticks_buffer" not in st.session_state:
    st.session_state.ticks_buffer = deque(maxlen=200)
if "signals_buffer" not in st.session_state:
    st.session_state.signals_buffer = deque(maxlen=50)
if "orders_buffer" not in st.session_state:
    st.session_state.orders_buffer = deque(maxlen=50)

# Sidebar Controls
st.sidebar.header("⚙️ System Configuration")
broker_url = st.sidebar.text_input("Kafka Broker", "localhost:9092")
auto_refresh = st.sidebar.checkbox("Auto-Refresh Feed (3s)", value=True)
selected_commodity = st.sidebar.selectbox("Commodity Focus", ["CL=F", "GC=F", "NG=F"])

# -------------------------------------------------------------
# Background Kafka Subscriber Thread (Persists Across Reruns)
# -------------------------------------------------------------
@st.cache_resource
def start_kafka_background_listener(broker: str):
    """
    Runs a single, continuous background consumer thread.
    This prevents Kafka offset desyncs on Streamlit UI reruns.
    """
    shared_data = {
        "ticks": deque(maxlen=200),
        "signals": deque(maxlen=50),
        "orders": deque(maxlen=50)
    }

    def kafka_worker():
        # Unique consumer group name with timestamp to always read from earliest available
        consumer_group = f"streamlit_ui_listener_{int(time.time())}"
        consumer = Consumer({
            'bootstrap.servers': broker,
            'group.id': consumer_group,
            'auto.offset.reset': 'earliest'
        })
        
        consumer.subscribe(['market-ticks', 'quant-signals', 'news-signals', 'execution-orders'])

        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    pass
                continue

            try:
                topic = msg.topic()
                payload = json.loads(msg.value().decode('utf-8'))

                if topic == 'market-ticks':
                    shared_data["ticks"].append(payload)
                elif topic in ['quant-signals', 'news-signals']:
                    shared_data["signals"].append(payload)
                elif topic == 'execution-orders':
                    shared_data["orders"].append(payload)
            except Exception:
                pass

    thread = threading.Thread(target=kafka_worker, daemon=True)
    thread.start()
    return shared_data

# Initialize Background Listener
live_data = start_kafka_background_listener(broker_url)

# Copy thread-safe deque snapshots to session state
ticks_list = list(live_data["ticks"])
signals_list = list(live_data["signals"])
orders_list = list(live_data["orders"])

# -------------------------------------------------------------
# Top Metric Cards
# -------------------------------------------------------------
m1, m2, m3, m4 = st.columns(4)

df_ticks = pd.DataFrame(ticks_list)
latest_price = 0.0

if not df_ticks.empty and 'symbol' in df_ticks.columns:
    symbol_df = df_ticks[df_ticks['symbol'] == selected_commodity]
    if not symbol_df.empty:
        latest_price = symbol_df.iloc[-1]['price']

m1.metric("Selected Commodity", selected_commodity, f"${latest_price:.2f}")
m2.metric("Total Market Ticks Ingested", len(ticks_list))
m3.metric("Agent Signals Generated", len(signals_list))
m4.metric("Executed Paper Orders", len(orders_list))

st.divider()

# -------------------------------------------------------------
# Main Section: Charts & Agent Feed
# -------------------------------------------------------------
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader(f"📊 Real-Time Price Action ({selected_commodity})")
    if not df_ticks.empty and 'symbol' in df_ticks.columns:
        filtered_df = df_ticks[df_ticks['symbol'] == selected_commodity]
        if not filtered_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=filtered_df['timestamp'],
                y=filtered_df['price'],
                mode='lines+markers',
                name='Price',
                line=dict(color='#00FFAA', width=2)
            ))
            fig.update_layout(
                template="plotly_dark",
                height=400,
                xaxis_title="Time",
                yaxis_title="Price ($)",
                margin=dict(l=20, r=20, t=30, b=20)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"Waiting for incoming price ticks for {selected_commodity}...")
    else:
        st.info("Waiting for market ticks stream from Agent 1A...")

with col_right:
    st.subheader("🤖 Recent Agent Signals")
    if signals_list:
        df_sig = pd.DataFrame(signals_list).tail(5)
        for _, row in df_sig.iterrows():
            badge = "🟢 BUY" if row.get("action") == "BUY" else "🔴 SELL"
            st.markdown(f"**{row.get('agent_id')}** | {row.get('commodity')} {badge}")
            st.caption(f"Reasoning: {row.get('reasoning')}")
            st.divider()
    else:
        st.info("No signals generated yet.")

# -------------------------------------------------------------
# Bottom Section: Execution Order Book
# -------------------------------------------------------------
st.subheader("⚡ Filled Execution Orders (Paper Trading)")
if orders_list:
    st.dataframe(
        pd.DataFrame(orders_list)[
            ["order_id", "commodity", "action", "quantity", "stop_loss", "take_profit", "synthesized_confidence", "reasoning"]
        ],
        use_container_width=True
    )
else:
    st.info("No execution orders triggered by Core Agent 3 yet.")

# Auto-refresh loop
if auto_refresh:
    time.sleep(3)
    st.rerun()