

import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List

from confluent_kafka import Producer, KafkaError

logger = logging.getLogger(__name__)


def delivery_report(err, msg, stats: dict):
  
    if err is not None:
        stats["failed"] += 1
        logger.debug(f"[Kafka] Delivery failure: {err}")
    else:
        stats["success"] += 1
        if msg.latency() is not None:
            stats["latency_sum"] += msg.latency()
            stats["latency_count"] += 1


class KafkaPublisher:


    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "iot-sensors")
        self.acks = os.getenv("KAFKA_ACKS", "1")
        
        # Statistike za merenje
        self.stats = {
            "success": 0,
            "failed": 0,
            "latency_sum": 0.0,
            "latency_count": 0,
        }
        self.start_time = None

    def _create_producer(self) -> Producer:
       
        

        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": self.acks,
            
           
            "linger.ms": 5,          
            "batch.size": 65536,      
            "compression.type": "lz4",
            
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.kbytes": 65536,
            
            "retries": 3,
            "retry.backoff.ms": 100,
            

            "statistics.interval.ms": 5000,
        }
        
     
        if self.acks == "all":
            config["delivery.timeout.ms"] = 30000
            config["request.timeout.ms"] = 5000
        
        return Producer(config)

    async def run_simulation(self, device_ids: List[str],
                              generator,
                              messages_per_second: float,
                              duration_seconds: int):
      
        self.start_time = time.time()
        delay = 1.0 / messages_per_second if messages_per_second > 0 else 0.01
        
        logger.info(
            f"[Kafka] Startovanje simulacije:\n"
            f"  Uređaja:          {len(device_ids)}\n"
            f"  acks:             {self.acks}\n"
            f"  Target msg/s:     {messages_per_second * len(device_ids):.0f}\n"
            f"  Trajanje:         {duration_seconds}s\n"
            f"  Broker:           {self.bootstrap_servers}\n"
            f"  Topic:            {self.topic}"
        )
        
        producer = self._create_producer()
        end_time = time.time() + duration_seconds
        total_produced = 0
        
        try:
            while time.time() < end_time:
                batch_start = time.time()
                
                for device_id in device_ids:
                    payload = generator.generate_message(device_id)
                    
                  
                    producer.produce(
                        topic=self.topic,
                        key=device_id.encode("utf-8"),
                        value=payload.encode("utf-8"),
                     
                        headers={"source": b"ingestion-service"},
                        on_delivery=lambda err, msg: delivery_report(err, msg, self.stats)
                    )
                    total_produced += 1
                
               
                producer.poll(0)
                
              
                if total_produced % 1000 == 0 and total_produced > 0:
                    elapsed = time.time() - self.start_time
                    tps = total_produced / elapsed
                    avg_lat = (
                        self.stats["latency_sum"] / self.stats["latency_count"] * 1000
                        if self.stats["latency_count"] > 0 else 0
                    )
                    logger.info(
                        f"[Kafka] Produkovano: {total_produced:,} | "
                        f"Potvrdjeno: {self.stats['success']:,} | "
                        f"Greske: {self.stats['failed']} | "
                        f"Throughput: {tps:.1f} msg/s | "
                        f"Avg latencija: {avg_lat:.2f}ms"
                    )
                
                
                batch_elapsed = time.time() - batch_start
                sleep_time = max(0, delay - batch_elapsed)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.info("[Kafka] Simulacija prekinuta.")
        except Exception as e:
            logger.error(f"[Kafka] Fatalna greska: {e}", exc_info=True)
        finally:
           
            logger.info("[Kafka] Flushing preostalih poruka...")
            producer.flush(timeout=30)
            
            elapsed = time.time() - self.start_time if self.start_time else 0
            avg_lat_ms = (
                self.stats["latency_sum"] / self.stats["latency_count"] * 1000
                if self.stats["latency_count"] > 0 else 0
            )
            
            logger.info(
                f"\n[Kafka]  REZULTATI SIMULACIJE\n"
                f"  Ukupno produkovano: {total_produced:,}\n"
                f"  Potvrdjeno (acks):   {self.stats['success']:,}\n"
                f"  Greske:             {self.stats['failed']}\n"
                f"  Trajanje:           {elapsed:.2f}s\n"
                f"  Avg throughput:     {total_produced/elapsed:.1f} msg/s\n"
                f"  Avg latencija:      {avg_lat_ms:.2f}ms\n"
            )

    async def run_burst_simulation(self, device_ids: List[str],
                                    generator,
                                    initial_rate: float,
                                    burst_rate: float,
                                    burst_duration: int):
       
        producer = self._create_producer()
        
        logger.info(
            f"[Kafka] BURST SIMULACIJA:\n"
            f"  Normalna brzina:  {initial_rate * len(device_ids):.0f} msg/s\n"
            f"  Burst brzina:     {burst_rate * len(device_ids):.0f} msg/s"
        )
        
       
        logger.info("[Kafka] Faza 1: Normalna brzina (30s)...")
        await self._kafka_send_at_rate(producer, device_ids, generator, initial_rate, 30)
        
      
        logger.info(f"[Kafka]  Povecanje na {burst_rate * len(device_ids):.0f} msg/s")
        await self._kafka_send_at_rate(producer, device_ids, generator, burst_rate, burst_duration)
        
     
        logger.info("[Kafka] Faza 3: Recovery...")
        await self._kafka_send_at_rate(producer, device_ids, generator, initial_rate, 30)
        
        producer.flush(30)
        logger.info("[Kafka] Burst simulacija zavrsena.")

    async def _kafka_send_at_rate(self, producer, device_ids, generator, rate, duration):
        
        delay = 1.0 / rate if rate > 0 else 0.001
        end_time = time.time() + duration
        
        while time.time() < end_time:
            start = time.time()
            for device_id in device_ids:
                payload = generator.generate_message(device_id)
                producer.produce(
                    topic=self.topic,
                    key=device_id.encode(),
                    value=payload.encode(),
                    on_delivery=lambda e, m: delivery_report(e, m, self.stats)
                )
            producer.poll(0)
            elapsed = time.time() - start
            await asyncio.sleep(max(0, delay - elapsed))