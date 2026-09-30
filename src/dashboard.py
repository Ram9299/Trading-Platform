import streamlit as st
import pandas as pd
import json
import time
import threading
from collections import deque
from confluent_kafka import Consumer, KafkaError
import plotly.graph_objects as go

from src.core.schemas import AgentSignal, ExecutionOrder
from src.core.kafka_client import EventBroker

st.set_page_config(
    page_title="Multi-Agent Trading Platform",
    page_icon="📈",
    layout="wide"
)

# Title & Status Header
st.title("⚡ Industrial Multi-Agent Futures Trading Platform")
st.caption("Live Observability Dashboard: Ingestion ➔ Indicator Engine ➔ Macro Sentiment ➔ Synthesizer ➔ Paper Execution")

# Global Emergency State
if "kill_switch_active" not in st.session_state:
    st.session_state.kill_switch_active = False

# Sidebar Controls
st.sidebar.header("⚙️ System Configuration")
broker_url = st.sidebar.text_input("Kafka Broker", "localhost:9092")
auto_refresh = st.sidebar.checkbox("Auto-Refresh Feed (3s)", value=True)
selected_commodity = st.sidebar.selectbox("Commodity Focus", ["CL=F", "GC=F", "NG=F"])

st.sidebar.divider()
st.sidebar.header("🎛️ Manual Override Controls")

# Manual Trade Trigger Form
with st.sidebar.form("manual_order_form"):
    st.write("Publish Manual Override Signal")
    manual_commodity = st.selectbox("Symbol", ["CL=F", "GC=F", "NG=F"], key="manual_sym")
    manual_action = st.radio("Action", ["BUY", "SELL"], horizontal=True)
    manual_confidence = st.slider("Confidence", 0.5, 1.0, 0.9)
    submit_order = st.form_submit_button("⚡ Execute Manual Signal")

    if submit_order:
        if st.session_state.kill_switch_active:
            st.error("Cannot execute: Global Emergency Kill Switch is ACTIVE!")
        else:
            broker = EventBroker(broker_url=broker_url)
            manual_signal = AgentSignal(
                agent_id="human_trader_override",
                commodity=manual_commodity,
                action=manual_action,
                confidence=manual_confidence,
                reasoning=f"[Manual Dashboard Override] Trader executed manual {manual_action}."
            )
            broker.publish_event("quant-signals", manual_signal.model_dump())
            broker.flush()
            st.success(f"Published manual {manual_action} signal for {manual_commodity}!")

st.sidebar.divider()
st.sidebar.header("🚨 Emergency Controls")

# Emergency Kill Switch Button
if not st.session_state.kill_switch_active:
    if st.sidebar.button("🔴 ACTIVATE GLOBAL KILL SWITCH", type="primary"):
        st.session_state.kill_switch_active = True
        
        # Broadcast HALT command to Kafka
        broker = EventBroker(broker_url=broker_url)
        broker.publish_event("system-commands", {"command": "ACTIVATE_KILL_SWITCH"})
        broker.flush()
        
        st.sidebar.error("KILL SWITCH ACTIVATED! All automated trading suspended.")
        st.rerun()
else:
    st.sidebar.error("⚠️ SYSTEM HALTED BY KILL SWITCH")
    if st.sidebar.button("🟢 RESUME AUTOMATED TRADING"):
        st.session_state.kill_switch_active = False
        
        # Broadcast RESUME command to Kafka
        broker = EventBroker(broker_url=broker_url)
        broker.publish_event("system-commands", {"command": "DEACTIVATE_KILL_SWITCH"})
        broker.flush()
        
        st.sidebar.success("Trading resumed.")
        st.rerun()

# -------------------------------------------------------------
# Background Kafka Subscriber Thread
# -------------------------------------------------------------
@st.cache_resource
def start_kafka_background_listener(broker: str):
    shared_data = {
        "ticks": deque(maxlen=200),
        "signals": deque(maxlen=50),
        "orders": deque(maxlen=50)
    }

    def kafka_worker():
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

live_data = start_kafka_background_listener(broker_url)

ticks_list = list(live_data["ticks"])
signals_list = list(live_data["signals"])
orders_list = list(live_data["orders"])

# Top Metric Cards
m1, m2, m3, m4 = st.columns(4)

df_ticks = pd.DataFrame(ticks_list)
latest_price = 0.0

if not df_ticks.empty and 'symbol' in df_ticks.columns:
    symbol_df = df_ticks[df_ticks['symbol'] == selected_commodity]
    if not symbol_df.empty:
        latest_price = symbol_df.iloc[-1]['price']

status_label = "🔴 SYSTEM HALTED" if st.session_state.kill_switch_active else "🟢 AUTOMATED"

m1.metric("Selected Commodity", selected_commodity, f"${latest_price:.2f}")
m2.metric("Execution Mode", status_label)
m3.metric("Agent Signals Generated", len(signals_list))
m4.metric("Executed Paper Orders", len(orders_list))

st.divider()

# Main Section: Charts & Agent Feed
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
            st.plotly_chart(fig, width="stretch")
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

# Bottom Section: Execution Order Book
st.subheader("⚡ Filled Execution Orders (Paper Trading)")
if orders_list:
    st.dataframe(
        pd.DataFrame(orders_list)[
            ["order_id", "commodity", "action", "quantity", "stop_loss", "take_profit", "synthesized_confidence", "reasoning"]
        ],
        width="stretch"
    )
else:
    st.info("No execution orders triggered by Core Agent 3 yet.")

if auto_refresh:
    time.sleep(3)
    st.rerun()