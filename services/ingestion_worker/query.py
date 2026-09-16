import argparse
import asyncio
import json
from services.ingestion_worker.db import db
from services.ingestion_worker.embeddings import embedder

async def run_semantic_query(query: str, limit: int = 5):
    await db.connect()
    try:
        print(f"\n🔍 Searching logs semantically for: '{query}' (limit={limit})")
        results = await db.search_similar_logs(query, limit=limit)
        
        if not results:
            print("No matching logs found in pgvector.")
            return

        print(f"Found {len(results)} matches:\n")
        for i, row in enumerate(results, 1):
            sim = row.get('similarity', 0.0)
            print(f"[{i}] Similarity: {sim*100:.1f}% | Level: {row['level']} | Service: {row['service_name']}")
            print(f"    Message: {row['message']}")
            print(f"    Trace ID: {row.get('trace_id')} | Status: {row.get('http_status')}")
            print("-" * 60)
    finally:
        await db.disconnect()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aether Semantic Log Search")
    parser.add_argument("query", type=str, help="Error message or symptom to search")
    parser.add_argument("--limit", type=int, default=5, help="Number of results to return")
    args = parser.parse_args()

    asyncio.run(run_semantic_query(args.query, args.limit))
