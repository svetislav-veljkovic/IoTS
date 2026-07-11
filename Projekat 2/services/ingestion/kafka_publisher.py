import asyncio, logging, os, random, time
from confluent_kafka import Producer, KafkaException

logger = logging.getLogger(__name__)


class KafkaPublisher:
    def __init__(self):
        self.bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "iot-kafka:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "iot-sensors")
        self.acks_raw = os.getenv("KAFKA_ACKS", "1")
        self.cr = float(os.getenv("CRITICAL_RATIO", "0.05"))
        self.st = {"ok":0, "fail":0, "lat_s":0.0, "lat_n":0, "crit":0, "prod":0}
        self.t0 = None

    def _mk(self):
        a = self.acks_raw if self.acks_raw == "all" else int(self.acks_raw)
        return Producer({
            "bootstrap.servers": self.bootstrap, "acks": a,
            "linger.ms": 5, "batch.size": 65536, "compression.type": "lz4",
            "socket.keepalive.enable": True, "message.send.max.retries": 5,
            "retry.backoff.ms": 200, "queue.buffering.max.messages": 1_000_000,
        })

    def _on_deliv(self, err, _m):
        if err is None: self.st["ok"] += 1
        else: self.st["fail"] += 1

    async def _dev_loop(self, p, did, gen, rps, stop):
        d = 1.0 / rps if rps > 0 else 0.001
        try:
            while not stop.is_set():
                t0 = time.perf_counter()
                crit = self.cr > 0 and random.random() < self.cr
                pay = gen.generate_message(did, crit).encode("utf-8")
                try:
                    p.produce(self.topic, key=did.encode(), value=pay,
                              headers={"source": b"ingestion"},
                              on_delivery=self._on_deliv)
                    p.poll(0); self.st["prod"] += 1
                    if crit: self.st["crit"] += 1
                except (KafkaException, BufferError):
                    self.st["fail"] += 1; p.poll(5); await asyncio.sleep(0.01); continue
                await asyncio.sleep(max(0, d - (time.perf_counter()-t0)))
        except asyncio.CancelledError: return

    async def run_simulation(self, devs, gen, rate, dur):
        self.t0 = time.time(); self.st = {"ok":0,"fail":0,"lat_s":0.0,"lat_n":0,"crit":0,"prod":0}
        p = self._mk()
        tot = len(devs)*rate
        logger.info("[Kafka] PARALELNO: devices=%d acks=%s total=%.0f msg/s dur=%ds",
                    len(devs), self.acks_raw, tot, dur)
        try:
            stop = asyncio.Event()
            tasks = [asyncio.create_task(self._dev_loop(p, d, gen, rate, stop)) for d in devs]
            rep = asyncio.create_task(self._report(stop))
            await asyncio.sleep(dur); stop.set()
            for t in tasks: t.cancel(); rep.cancel()
            await asyncio.gather(*tasks, rep, return_exceptions=True)
        except Exception as e:
            logger.error("[Kafka] Fatal: %s", e, exc_info=True)
        finally:
            p.flush(30); self._print_stats()

    async def run_burst(self, devs, gen, init, burst, burst_dur):
        self.t0 = time.time(); self.st = {"ok":0,"fail":0,"lat_s":0.0,"lat_n":0,"crit":0,"prod":0}
        p = self._mk()
        logger.info("[Kafka] BURST init=%.0f burst=%.0f dur=%ds",
                    init*len(devs), burst*len(devs), burst_dur)
        try:
            await self._phase(p, devs, gen, init, 30)
            await self._phase(p, devs, gen, burst, burst_dur)
            await self._phase(p, devs, gen, init, 30)
        finally:
            p.flush(30); self._print_stats()

    async def _phase(self, p, devs, gen, rate, dur):
        stop = asyncio.Event()
        tasks = [asyncio.create_task(self._dev_loop(p, d, gen, rate, stop)) for d in devs]
        await asyncio.sleep(dur); stop.set()
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _report(self, stop):
        lp, lt = 0, time.time()
        while not stop.is_set():
            await asyncio.sleep(5)
            pr, ok, fl = self.st["prod"], self.st["ok"], self.st["fail"]
            n = time.time(); el = n-(self.t0 or n)
            w = (pr-lp)/max(n-lt, 1e-3); lp,lt=pr,n
            logger.info(f"[Kafka] Prod={pr:,} ACK={ok:,} Fail={int(fl)} Avg={pr/el:.1f} Win5s={w:.1f}")

    def _print_stats(self):
        if not self.t0: return
        el = time.time()-self.t0; s = self.st
        tot = s["ok"]+s["fail"]; loss = s["fail"]/tot*100 if tot else 0
        logger.info(
            "\n[Kafka] === REZULTATI ===\n"
            f"  Produce calls: {int(s['prod']):,}\n  ACKed: {int(s['ok']):,}\n"
            f"  Failed: {int(s['fail'])}\n  Loss: {loss:.2f}%\n"
            f"  Krit: {int(s['crit'])}\n  Trajanje: {el:.2f}s\n"
            f"  Throughput: {s['ok']/max(el,1e-3):.1f} msg/s")
