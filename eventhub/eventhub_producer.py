"""
eventhub_producer.py
Streams fake Maputo chapa/bus ride events to Azure Event Hubs.

Setup:
    pip install azure-eventhub python-dotenv

.env file (create this in the same folder):
    EVENT_HUB_CONNECTION_STR=Endpoint=sb://...
    EVENT_HUB_NAME=maputo-transport
"""

import asyncio
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

from generator.fake_data_generator import generate_batch

load_dotenv()

CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "event-hub-maputo-transport")


# ── Config ────────────────────────────────────────────────────────────────────
EVENTS_PER_BATCH = 10       # How many ride events per send
INTERVAL_SECONDS = 5        # How often to send a batch (simulates real-time)
TOTAL_BATCHES   = 720       # 720 × 5s = 1 hour of data. Set to None for infinite.


async def send_batch(producer: EventHubProducerClient, batch_num: int):
    """Generate and send one batch of ride events."""
    events = generate_batch(n=EVENTS_PER_BATCH)

    async with producer:
        event_data_batch = await producer.create_batch()

        for ride in events:
            payload = json.dumps(ride).encode("utf-8")
            event_data_batch.add(EventData(payload))

        await producer.send_batch(event_data_batch)

    total_rev = sum(e["total_revenue_mzn"] for e in events)
    total_pax = sum(e["passengers"] for e in events)
    print(
        f"[Batch {batch_num:04d}] {datetime.utcnow().strftime('%H:%M:%S')} | "
        f"Sent {len(events)} events | "
        f"{total_pax} passengers | "
        f"{total_rev:,} MZN revenue"
    )


async def run_producer():
    print("🚌 Maputo Transport Stream — starting producer...")
    print(f"   Target: {EVENT_HUB_NAME}")
    print(f"   Rate:   {EVENTS_PER_BATCH} events every {INTERVAL_SECONDS}s\n")

    batch_num = 0
    while TOTAL_BATCHES is None or batch_num < TOTAL_BATCHES:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=CONNECTION_STR,
            eventhub_name=EVENT_HUB_NAME
        )
        await send_batch(producer, batch_num + 1)
        batch_num += 1
        await asyncio.sleep(INTERVAL_SECONDS)

    print("\n✅ Done — all batches sent.")


if __name__ == "__main__":
    asyncio.run(run_producer())