import streamlit as st
import pandas as pd
import json
import time
from confluent_kafka import Consumer, KafkaError
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(
    page_title="Multi-Agent Trading Platform",
    page_icon="📈",
    layout="wide"
)

# Title & Status Header
st.title("⚡ Industrial Multi-Agent Futures Trading Platform")
st.caption("Live Observability Dashboard: Ingestion ➔ Indicator Engine ➔ Macro Sentiment ➔ Synthesizer ➔ Paper Execution")

# Define In-Memory Cache for Dashboard Display
if "ticks" not in st.session_state:
    st.session_state.ticks = []
if "signals" not in st.session_state:
    st.session_state.signals = []
if "orders" not in st.session_state:
    st.session_state.orders = []

# Sidebar Controls
st.sidebar.header("⚙️ System Configuration")
broker_url = st.sidebar.text_input("Kafka Broker", "localhost:9092")
auto_refresh = st.sidebar.checkbox("Auto-Refresh Feed", value=True)
selected_commodity = st.sidebar.selectbox("Commodity Focus", ["CL=F", "GC=F", "NG=F"])

# -------------------------------------------------------------
# Kafka Consumer Worker (Non-blocking metric fetcher)
# -------------------------------------------------------------
def fetch_kafka_updates():
    consumer = Consumer({
        'bootstrap.servers': broker_url,
        'group.id': 'streamlit_dashboard_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['market-ticks', 'quant-signals', 'news-signals', 'execution-orders'])

    # Poll up to 10 messages per refresh cycle
    for _ in range(10):
        msg = consumer.poll(0.1)
        if msg is None:
            break
        if msg.error():
            continue

        topic = msg.topic()
        payload = json.loads(msg.value().decode('utf-8'))

        if topic == 'market-ticks':
            st.session_state.ticks.append(payload)
            if len(st.session_state.ticks) > 100:
                st.session_state.ticks.pop(0)
        elif topic in ['quant-signals', 'news-signals']:
            st.session_state.signals.append(payload)
        elif topic == 'execution-orders':
            st.session_state.orders.append(payload)

    consumer.close()

# Poll updates
try:
    fetch_kafka_updates()
except Exception as e:
    st.sidebar.warning(f"Kafka Feed Notice: {e}")

# -------------------------------------------------------------
# Top Metric Cards
# -------------------------------------------------------------
m1, m2, m3, m4 = st.columns(4)

df_ticks = pd.DataFrame(st.session_state.ticks)
latest_price = 0.0
if not df_ticks.empty and 'symbol' in df_ticks.columns:
    symbol_df = df_ticks[df_ticks['symbol'] == selected_commodity]
    if not symbol_df.empty:
        latest_price = symbol_df.iloc[-1]['price']

m1.metric("Selected Commodity", selected_commodity, f"${latest_price:.2f}")
m2.metric("Total Market Ticks", len(st.session_state.ticks))
m3.metric("Agent Signals Generated", len(st.session_state.signals))
m4.metric("Executed Paper Orders", len(st.session_state.orders))

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
            st.info(f"Waiting for live price ticks for {selected_commodity}...")
    else:
        st.info("Waiting for market ticks stream from Agent 1A...")

with col_right:
    st.subheader("🤖 Recent Agent Signals")
    if st.session_state.signals:
        df_sig = pd.DataFrame(st.session_state.signals).tail(5)
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
if st.session_state.orders:
    st.dataframe(
        pd.DataFrame(st.session_state.orders)[
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