import asyncio
import logging
import os
from dotenv import load_dotenv

from data_generator import IoTDataGenerator
from mqtt_publisher import MqttPublisher
from kafka_publisher import KafkaPublisher

# Konfiguracija logovanja za lep i pregledan ispis u Dockeru
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("main")

async def main():
  
    load_dotenv()

    
    broker_type = os.getenv("TARGET_BROKER", "mqtt").lower()
    sim_mode = os.getenv("SIMULATION_MODE", "normal").lower()
    device_count = int(os.getenv("DEVICE_COUNT", "100"))
    msg_per_sec = float(os.getenv("MSG_PER_SEC", "1.0"))  
    duration = int(os.getenv("DURATION_SECONDS", "60"))
    dataset_path = os.getenv("DATASET_PATH", "/app/data/data_set.csv")

    
    burst_rate = float(os.getenv("BURST_RATE", "5.0"))
    burst_duration = int(os.getenv("BURST_DURATION", "15"))


    logger.info("POKRETANJE IoT SIMULACIONOG SERVIS-A")
    logger.info("==============================================")
    
  
    generator = IoTDataGenerator(device_count=device_count, dataset_path=dataset_path)
    device_ids = generator.get_device_ids()


    if broker_type == "mqtt":
        logger.info("Target Broker: MQTT (Mosquitto)")
        publisher = MqttPublisher()
        
        if sim_mode == "burst":
            await publisher.run_burst_simulation(
                device_ids=device_ids,
                generator=generator,
                initial_rate=msg_per_sec,
                burst_rate=burst_rate,
                burst_duration=burst_duration
            )
        else:
            await publisher.run_simulation(
                device_ids=device_ids,
                generator=generator,
                messages_per_second=msg_per_sec,
                duration_seconds=duration
            )

    elif broker_type == "kafka":
        logger.info("Target Broker: Apache Kafka")
        publisher = KafkaPublisher()
        
        if sim_mode == "burst":
            await publisher.run_burst_simulation(
                device_ids=device_ids,
                generator=generator,
                initial_rate=msg_per_sec,
                burst_rate=burst_rate,
                burst_duration=burst_duration
            )
        else:
            await publisher.run_simulation(
                device_ids=device_ids,
                generator=generator,
                messages_per_second=msg_per_sec,
                duration_seconds=duration
            )
            
    else:
        logger.error(f"Nepoznat TARGET_BROKER: '{broker_type}'. Izaberi 'mqtt' ili 'kafka'.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Simulacija rucno zaustavljena.")