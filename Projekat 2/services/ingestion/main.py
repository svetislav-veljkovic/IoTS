import asyncio, logging, os
from dotenv import load_dotenv
from data_generator import IoTDataGenerator
from mqtt_publisher import MqttPublisher
from kafka_publisher import KafkaPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("main")


async def main():
    load_dotenv()
    broker = os.getenv("BROKER_TYPE", "mqtt").lower()
    mode   = os.getenv("SIMULATION_MODE", "normal").lower()
    devs_n = int(os.getenv("DEVICE_COUNT", "100"))
    rate   = float(os.getenv("MESSAGES_PER_SECOND", "10"))
    dur    = int(os.getenv("SIMULATION_DURATION_SECONDS", "60"))
    ds     = os.getenv("DATASET_PATH", "/app/data/data_set.csv")
    br     = float(os.getenv("BURST_RATE", "5.0"))
    bd     = int(os.getenv("BURST_DURATION", "15"))

    logger.info("POKRETANJE Ingestion (broker=%s mode=%s devs=%d)", broker, mode, devs_n)
    gen = IoTDataGenerator(devs_n, ds)
    devs = gen.get_device_ids()
    logger.info("Pripremljeno %d paralelnih async taskova.", len(devs))

    pub = MqttPublisher() if broker == "mqtt" else KafkaPublisher()
    try:
        if mode == "burst":
            await pub.run_burst(devs, gen, rate, br, bd)
        else:
            await pub.run_simulation(devs, gen, rate, dur)
    except KeyboardInterrupt:
        logger.info("Zaustavljeno.")

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
