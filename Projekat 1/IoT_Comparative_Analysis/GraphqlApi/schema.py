import strawberry
from typing import List, Optional
import asyncpg
import os
from datetime import datetime


@strawberry.type
class SensorReading:
    id: int
    device_id: str
    timestamp: datetime
    temperature: float
    humidity: float
    co2_level: int
    voltage: float
    gps_lat: float
    gps_lng: float


@strawberry.type
class AggregatedData:
    avg_temperature: float
    max_co2: int
    min_voltage: float

@strawberry.type
class Query:
  
    @strawberry.field
    async def get_latest_readings(self, device_id: str, limit: int = 10) -> List[SensorReading]:
        conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
        try:
            rows = await conn.fetch(
                "SELECT * FROM sensor_readings WHERE device_id = $1 ORDER BY timestamp DESC LIMIT $2",
                device_id, limit
            )
            return [SensorReading(
                id=r['id'], device_id=r['device_id'], timestamp=r['timestamp'],
                temperature=float(r['temperature']), humidity=float(r['humidity']),
                co2_level=r['co2_level'], voltage=float(r['voltage']),
                gps_lat=float(r['gps_lat']), gps_lng=float(r['gps_lng'])
            ) for r in rows]
        finally:
            await conn.close()

    
    @strawberry.field
    async def get_aggregated_data(self, device_id: str, days: int = 30) -> AggregatedData:
        conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
        try:
            row = await conn.fetchrow(
                """SELECT AVG(temperature) as avg_temp, MAX(co2_level) as max_co2, MIN(voltage) as min_volt 
                   FROM sensor_readings 
                   WHERE device_id = $1 AND timestamp >= NOW() - INTERVAL '1 day' * $2""",
                device_id, days
            )
            return AggregatedData(
                avg_temperature=float(row['avg_temp'] or 0),
                max_co2=row['max_co_2'] or 0,
                min_voltage=float(row['min_volt'] or 0)
            )
        finally:
            await conn.close()

@strawberry.type
class Mutation:
    
    @strawberry.mutation
    async def ingest_reading(
        self, device_id: str, temperature: float, humidity: float, 
        co2_level: int, voltage: float, gps_lat: float, gps_lng: float
    ) -> str:
        conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
        try:
            await conn.execute(
                """INSERT INTO sensor_readings (device_id, timestamp, temperature, humidity, co2_level, voltage, gps_lat, gps_lng)
                   VALUES ($1, NOW(), $2, $3, $4, $5, $6, $7)""",
                device_id, temperature, humidity, co2_level, voltage, gps_lat, gps_lng
            )
            return "Data ingested via GraphQL"
        finally:
            await conn.close()

schema = strawberry.Schema(query=Query, mutation=Mutation)