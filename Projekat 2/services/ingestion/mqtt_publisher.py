

import asyncio
import logging
import os
import time
from typing import List
import aiomqtt

logger = logging.getLogger(__name__)


class MqttPublisher:


    def __init__(self):
        self.host = os.getenv("MQTT_HOST", "mosquitto")
        self.port = int(os.getenv("MQTT_PORT", "1883"))
        self.topic = os.getenv("MQTT_TOPIC", "iot/sensors")
        self.qos = int(os.getenv("MQTT_QOS", "1"))
        
        # Statistike
        self.messages_sent = 0
        self.messages_failed = 0
        self.start_time = None

    async def publish_single(self, client: aiomqtt.Client, payload: str, 
                              device_id: str) -> bool:
       
        try:
            topic = f"{self.topic}/{device_id}"
            await client.publish(
                topic=topic,
                payload=payload.encode("utf-8"),
                qos=self.qos,
                retain=False
            )
            self.messages_sent += 1
            return True
        except Exception as e:
            self.messages_failed += 1
            logger.debug(f"MQTT publish greska za {device_id}: {e}")
            return False

    async def run_simulation(self, device_ids: List[str], 
                              generator, 
                              messages_per_second: float,
                              duration_seconds: int):
   
        self.start_time = time.time()
        delay = 1.0 / messages_per_second if messages_per_second > 0 else 0.1
        total_messages = int(messages_per_second * duration_seconds * len(device_ids))
        
        logger.info(
            f"[MQTT] Startovanje simulacije:\n"
            f"  Uredjaja:          {len(device_ids)}\n"
            f"  QoS:              {self.qos}\n"
            f"  Target msg/s:     {messages_per_second * len(device_ids):.0f}\n"
            f"  Trajanje:         {duration_seconds}s\n"
            f"  Broker:           {self.host}:{self.port}\n"
            f"  Topic pattern:    {self.topic}/<device_id>"
        )


        client_id = "ingestion-service-main"
        
        try:
            async with aiomqtt.Client(
                hostname=self.host,
                port=self.port,
                identifier=client_id,
                clean_session=False,  
                keepalive=60,
            ) as client:
                logger.info("[MQTT] Konekcija uspostavljena!")
                
                end_time = time.time() + duration_seconds
                
                while time.time() < end_time:
                    batch_start = time.time()
                    
                   
                    tasks = []
                    for device_id in device_ids:
                        payload = generator.generate_message(device_id)
                        tasks.append(
                            self.publish_single(client, payload, device_id)
                        )
                    
                
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                   
                    if self.messages_sent % 1000 == 0 and self.messages_sent > 0:
                        elapsed = time.time() - self.start_time
                        current_tps = self.messages_sent / elapsed
                        logger.info(
                            f"[MQTT] Poslato: {self.messages_sent:,} | "
                            f"Greške: {self.messages_failed} | "
                            f"Throughput: {current_tps:.1f} msg/s"
                        )
                    
                    
                    batch_elapsed = time.time() - batch_start
                    sleep_time = max(0, delay - batch_elapsed)
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.info("[MQTT] Simulacija prekinuta ")
        except Exception as e:
            logger.error(f"[MQTT] Fatalna greska: {e}", exc_info=True)
        finally:
            elapsed = time.time() - self.start_time if self.start_time else 0
            logger.info(
                f"\n[MQTT] === REZULTATI SIMULACIJE ===\n"
                f"  Ukupno poslato:   {self.messages_sent:,}\n"
                f"  Greške:           {self.messages_failed}\n"
                f"  Trajanje:         {elapsed:.2f}s\n"
                f"  Avg throughput:   {self.messages_sent/elapsed:.1f} msg/s\n"
            )

    async def run_burst_simulation(self, device_ids: List[str],
                                    generator,
                                    initial_rate: float,
                                    burst_rate: float,
                                    burst_duration: int):
     
        logger.info(
            f"[MQTT] BURST SIMULACIJA:\n"
            f"  Normalna brzina:  {initial_rate * len(device_ids):.0f} msg/s\n"
            f"  Burst brzina:     {burst_rate * len(device_ids):.0f} msg/s\n"
            f"  Burst trajanje:   {burst_duration}s"
        )
        
        async with aiomqtt.Client(
            hostname=self.host,
            port=self.port,
            identifier="ingestion-burst",
            clean_session=False,
            keepalive=60,
        ) as client:
            
           
            logger.info("[MQTT] Faza 1: Normalna brzina (30s)...")
            await self._send_at_rate(client, device_ids, generator, initial_rate, 30)
            
           
            logger.info(f"[MQTT]   Povećanje na {burst_rate * len(device_ids):.0f} msg/s")
            await self._send_at_rate(client, device_ids, generator, burst_rate, burst_duration)
            
        
            logger.info("[MQTT]  Recovery ..")
            await self._send_at_rate(client, device_ids, generator, initial_rate, 30)
            
            logger.info("[MQTT] Burst simulacija zavrsena.")

    async def _send_at_rate(self, client, device_ids, generator, rate_per_device, duration):
        
        delay = 1.0 / rate_per_device if rate_per_device > 0 else 0.001
        end_time = time.time() + duration
        
        while time.time() < end_time:
            tasks = [
                self.publish_single(client, generator.generate_message(did), did)
                for did in device_ids
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(max(0, delay))
