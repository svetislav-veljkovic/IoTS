import asyncio, logging, os, random, time
import aiomqtt

logger = logging.getLogger(__name__)


class MqttPublisher:
    def __init__(self):
        self.host = os.getenv("MQTT_HOST", "iot-mosquitto")
        self.port = int(os.getenv("MQTT_PORT", "1883"))
        self.topic = os.getenv("MQTT_TOPIC", "iot/sensors")
        self.qos = int(os.getenv("MQTT_QOS", "1"))
        self.cr = float(os.getenv("CRITICAL_RATIO", "0.05"))
        self._lk = asyncio.Lock()
        self.sent = self.fail = self.crit = 0
        self.t0 = None

    async def _dev_loop(self, client, did, gen, rps, stop):
        d = 1.0 / rps if rps > 0 else 0.1
        try:
            while not stop.is_set():
                t0 = time.perf_counter()
                crit = self.cr > 0 and random.random() < self.cr
                pay = gen.generate_message(did, crit)
                try:
                    await client.publish(
                        f"{self.topic}/{did}", pay.encode("utf-8"),
                        qos=self.qos, retain=False)
                    async with self._lk:
                        self.sent += 1
                        if crit: self.crit += 1
                except Exception:
                    async with self._lk:
                        self.fail += 1
                el = time.perf_counter() - t0
                await asyncio.sleep(max(0, d - el))
        except asyncio.CancelledError: return

    async def run_simulation(self, devs, gen, rate, dur):
        self.t0 = time.time(); self.sent = self.fail = self.crit = 0
        tot = len(devs) * rate
        logger.info("[MQTT] PARALELNO: devices=%d qos=%d rate/dev=%.1f total=%.0f msg/s dur=%ds",
                    len(devs), self.qos, rate, tot, dur)
        try:
            async with aiomqtt.Client(self.host, port=self.port, identifier="ingestion-svc",
                                      clean_session=False, keepalive=60) as c:
                logger.info("[MQTT] Konektovan.")
                stop = asyncio.Event()
                tasks = [asyncio.create_task(self._dev_loop(c, d, gen, rate, stop)) for d in devs]
                rep = asyncio.create_task(self._report(stop))
                await asyncio.sleep(dur)
                stop.set()
                for t in tasks: t.cancel()
                rep.cancel()
                await asyncio.gather(*tasks, rep, return_exceptions=True)
        except Exception as e:
            logger.error("[MQTT] Fatal: %s", e, exc_info=True)
        finally:
            self._print_stats()

    async def run_burst(self, devs, gen, init, burst, burst_dur):
        self.t0 = time.time(); self.sent = self.fail = self.crit = 0
        logger.info("[MQTT] BURST init=%.0f burst=%.0f dur=%ds",
                    init*len(devs), burst*len(devs), burst_dur)
        async with aiomqtt.Client(self.host, port=self.port, identifier="ingestion-burst",
                                  clean_session=False, keepalive=60) as c:
            await self._phase(c, devs, gen, init, 30)
            await self._phase(c, devs, gen, burst, burst_dur)
            await self._phase(c, devs, gen, init, 30)
        self._print_stats()

    async def _phase(self, c, devs, gen, rate, dur):
        stop = asyncio.Event()
        tasks = [asyncio.create_task(self._dev_loop(c, d, gen, rate, stop)) for d in devs]
        await asyncio.sleep(dur); stop.set()
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _report(self, stop):
        last_s, last_t = 0, time.time()
        while not stop.is_set():
            await asyncio.sleep(5)
            async with self._lk: s,f,cr = self.sent, self.fail, self.crit
            n = time.time(); el = n - (self.t0 or n)
            wt = (s-last_s)/max(n-last_t, 1e-3)
            last_s, last_t = s, n
            logger.info(f"[MQTT] Sent={s:,} Crit={cr} Fail={f} Avg={s/el:.1f} Win5s={wt:.1f}")

    def _print_stats(self):
        if not self.t0: return
        el = time.time()-self.t0
        tot = self.sent + self.fail
        loss = self.fail/tot*100 if tot else 0
        logger.info(
            "\n[MQTT] === REZULTATI ===\n"
            f"  Ukupno:     {tot:,}\n  Uspesno:    {self.sent:,}\n"
            f"  Neuspesno:  {self.fail}\n  Loss:       {loss:.2f}%\n"
            f"  Krit:       {self.crit}\n  Trajanje:   {el:.2f}s\n"
            f"  Throughput: {self.sent/max(el,1e-3):.1f} msg/s")
