import json
import random
import csv  
from datetime import datetime, timezone
from typing import Generator, Dict, Any
import os


class IoTDataGenerator:

    LOCATIONS = [
        "warehouse-A", "warehouse-B", "factory-floor-1",
        "factory-floor-2", "office-1st", "office-2nd",
        "server-room", "parking-lot", "rooftop", "basement"
    ]

    def __init__(self, device_count: int = 100, dataset_path: str = None):
        self.device_count = device_count
        self.dataset_path = dataset_path
        self.dataset_rows = []  
        self.current_index = 0
        self._load_dataset()

    def _load_dataset(self):
        """Učitava CSV dataset koristeći ugrađeni csv modul."""
        if self.dataset_path and os.path.exists(self.dataset_path):
            try:
                with open(self.dataset_path, mode='r', encoding='utf-8') as f:
                   
                    reader = csv.DictReader(f)
                    
                  
                    if reader.fieldnames:
                        reader.fieldnames = [field.lower().strip() for field in reader.fieldnames]
                    
                    self.dataset_rows = list(reader)
                    
                print(f"[DataGenerator] Dataset uspesno ucitan: {len(self.dataset_rows)} redova.")
            except Exception as e:
                print(f"[DataGenerator] Greska pri ucitavanju dataseta: {e}. Koristim random mode.")
                self.dataset_rows = []
        else:
            print(f"[DataGenerator] Dataset nije pronađen na {self.dataset_path}. Koristim random mode.")

    def _get_dataset_row(self) -> Dict[str, Any]:
        if not self.dataset_rows:
            return self._get_random_row()
        
       
        row = self.dataset_rows[self.current_index % len(self.dataset_rows)]
        self.current_index += 1
        
       
        def to_float(val, fallback):
            try:
                return float(val) if val is not None and val != "" else fallback
            except ValueError:
                return fallback

        def to_int(val, fallback):
            try:
                return int(float(val)) if val is not None and val != "" else fallback
            except ValueError:
                return fallback

        return {
            "temperature": to_float(row.get("temperature"), random.uniform(15, 55)),
            "humidity": to_float(row.get("humidity"), random.uniform(20, 90)),
            "pressure": to_float(row.get("pressure"), random.uniform(980, 1050)),
            "light": to_int(row.get("light"), random.randint(0, 1000)),
            "sound": to_int(row.get("sound"), random.randint(20, 100)),
            "motion": to_int(row.get("motion"), random.randint(0, 1)),
            "battery": to_float(row.get("battery"), random.uniform(20, 100)),
            "location": str(row.get("location")) if row.get("location") else random.choice(self.LOCATIONS),
        }

    def _get_random_row(self) -> Dict[str, Any]:
        return {
            "temperature": round(random.uniform(15.0, 75.0), 2),
            "humidity": round(random.uniform(20.0, 95.0), 2),
            "pressure": round(random.uniform(950.0, 1050.0), 2),
            "light": random.randint(0, 1000),
            "sound": random.randint(20, 110),
            "motion": random.randint(0, 1),
            "battery": round(random.uniform(10.0, 100.0), 2),
            "location": random.choice(self.LOCATIONS),
        }

    def generate_message(self, device_id: str) -> str:
        now = datetime.now(timezone.utc)
        
        if self.dataset_rows:
            sensor_data = self._get_dataset_row()
        else:
            sensor_data = self._get_random_row()

        payload = {
            "device_id": device_id,
            "timestamp": now.isoformat(),
            "ingested_at": now.timestamp(),  
            **sensor_data
        }
        
        return json.dumps(payload)

    def generate_critical_message(self, device_id: str) -> str:
        now = datetime.now(timezone.utc)
        
        payload = {
            "device_id": device_id,
            "timestamp": now.isoformat(),
            "ingested_at": now.timestamp(),
            "temperature": round(random.uniform(51.0, 80.0), 2),  
            "humidity": round(random.uniform(20.0, 95.0), 2),
            "pressure": round(random.uniform(950.0, 1050.0), 2),
            "light": random.randint(0, 1000),
            "sound": random.randint(20, 110),
            "motion": random.randint(0, 1),
            "battery": round(random.uniform(10.0, 100.0), 2),
            "location": random.choice(self.LOCATIONS),
        }
        
        return json.dumps(payload)

    def get_device_ids(self) -> list:
        return [f"device-{i:06d}" for i in range(1, self.device_count + 1)]