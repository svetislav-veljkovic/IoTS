import csv
import json
import os
import random
from datetime import datetime, timezone
from typing import Any, Dict, List


class IoTDataGenerator:
    LOCATIONS = [
        "warehouse-A", "warehouse-B", "factory-floor-1",
        "factory-floor-2", "office-1st", "office-2nd",
        "server-room", "parking-lot", "rooftop", "basement",
    ]

    def __init__(self, device_count: int = 100, dataset_path: str | None = None):
        self.device_count = device_count
        self.dataset_path = dataset_path
        self.dataset_rows: List[Dict[str, str]] = []
        self.current_index = 0
        self._load_dataset()

    def _load_dataset(self):
        if self.dataset_path and os.path.exists(self.dataset_path):
            try:
                with open(self.dataset_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames:
                        reader.fieldnames = [x.lower().strip() for x in reader.fieldnames]
                    self.dataset_rows = list(reader)
                print(f"[DataGenerator] Dataset ucitan: {len(self.dataset_rows)} redova.")
            except Exception as e:
                print(f"[DataGenerator] Greska: {e}. Koristim random.")
        else:
            print(f"[DataGenerator] Dataset '{self.dataset_path}' ne postoji. Koristim random.")

    def _row_from_ds(self) -> Dict[str, Any]:
        if not self.dataset_rows:
            return self._random_row()
        r = self.dataset_rows[self.current_index % len(self.dataset_rows)]
        self.current_index += 1

        def f(v, fb):
            try: return float(v) if v is not None and str(v).strip() else fb
            except: return fb
        def i(v, fb):
            try: return int(float(v)) if v is not None and str(v).strip() else fb
            except: return fb
        return {
            "temperature": f(r.get("temperature"), random.uniform(15, 55)),
            "humidity":    f(r.get("humidity"),    random.uniform(20, 90)),
            "pressure":    f(r.get("pressure"),    random.uniform(980, 1050)),
            "light":       i(r.get("light"),       random.randint(0, 1000)),
            "sound":       i(r.get("sound"),       random.randint(20, 100)),
            "motion":      i(r.get("motion"),      random.randint(0, 1)),
            "battery":     f(r.get("battery"),     random.uniform(20, 100)),
            "location":    (str(r.get("location")).strip() if r.get("location") else random.choice(self.LOCATIONS)),
        }

    @staticmethod
    def _random_row() -> Dict[str, Any]:
        return {
            "temperature": round(random.uniform(15.0, 49.0), 2),
            "humidity":    round(random.uniform(20.0, 95.0), 2),
            "pressure":    round(random.uniform(950.0, 1050.0), 2),
            "light":       random.randint(0, 1000),
            "sound":       random.randint(20, 110),
            "motion":      random.randint(0, 1),
            "battery":     round(random.uniform(10.0, 100.0), 2),
            "location":    random.choice(IoTDataGenerator.LOCATIONS),
        }

    @staticmethod
    def _critical_row() -> Dict[str, Any]:
        return {
            "temperature": round(random.uniform(51.0, 80.0), 2),
            "humidity":    round(random.uniform(20.0, 95.0), 2),
            "pressure":    round(random.uniform(950.0, 1050.0), 2),
            "light":       random.randint(0, 1000),
            "sound":       random.randint(20, 110),
            "motion":      random.randint(0, 1),
            "battery":     round(random.uniform(10.0, 100.0), 2),
            "location":    random.choice(IoTDataGenerator.LOCATIONS),
        }

    def generate_message(self, device_id: str, critical: bool = False) -> str:
        now = datetime.now(timezone.utc)
        sensor = self._critical_row() if critical else (self._row_from_ds() if self.dataset_rows else self._random_row())
        return json.dumps({
            "device_id": device_id,
            "timestamp": now.isoformat(),
            "ingested_at": now.timestamp(),
            **sensor,
        })

    def get_device_ids(self) -> List[str]:
        return [f"device-{i:06d}" for i in range(1, self.device_count + 1)]
