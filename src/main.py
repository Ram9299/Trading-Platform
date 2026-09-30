import subprocess
import sys
import time
import signal
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [SystemLauncher] - %(levelname)s - %(message)s")
logger = logging.getLogger("Launcher")

# List of modules to run concurrently as microservices
AGENT_MODULES = [
    "src.agents.agent_1_quant.data_ingestion",
    "src.agents.agent_1_quant.indicator_engine",
    "src.agents.agent_2_macro.sentiment_engine",
    "src.agents.agent_3_synthesizer.portfolio_manager",
    "src.agents.execution.paper_executor",
]

processes = []

def stop_all_agents(signum=None, frame=None):
    logger.info("Stopping all multi-agent services...")
    for proc, module in processes:
        logger.info(f"Terminating {module} [PID: {proc.pid}]...")
        proc.terminate()
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, stop_all_agents)
    signal.signal(signal.SIGTERM, stop_all_agents)

    logger.info("🚀 Launching Multi-Agent Trading System...")

    for module in AGENT_MODULES:
        logger.info(f"Starting sub-agent process: {module}")
        # Launch using Python module notation (-m) from root directory
        proc = subprocess.Popen([sys.executable, "-m", module])
        processes.append((proc, module))
        time.sleep(2) # Stagger start to allow Kafka topic creation

    logger.info("✅ All core agents are operational and running!")
    logger.info("Press Ctrl+C to gracefully shut down the platform.")

    # Keep launcher alive
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()