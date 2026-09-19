import asyncio
from services.ingestion_worker.db import DatabaseClient

async def test_bulk():
    db = DatabaseClient()
    await db.connect()

    # We can't really test bulk transition easily if it's not implemented yet.
    print(dir(db))

asyncio.run(test_bulk())
