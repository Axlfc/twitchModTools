import json
import os
import uuid
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from analyzer_ollama import OllamaAnalyzer
from config import config

def index_fallacies():
    print("🚀 Starting fallacy indexing...")

    # Initialize Ollama Analyzer
    analyzer = OllamaAnalyzer(config)

    # Initialize Qdrant Client
    client = QdrantClient(url=config.QDRANT_URL)
    collection_name = "fallacies_inventory"

    # Load fallacies
    with open('fallacies_inventory.json', 'r', encoding='utf-8') as f:
        fallacies = json.load(f)

    # Determine embedding dimension
    # Most models are 768 or 1024. Let's try to get one.
    test_emb = analyzer.get_embedding("test")
    if not test_emb:
        print("❌ Could not get test embedding from Ollama.")
        return

    dim = len(test_emb)
    print(f"📊 Embedding dimension: {dim}")

    # Create collection
    try:
        client.recreate_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )
        print(f"✅ Collection '{collection_name}' created/recreated.")
    except Exception as e:
        print(f"❌ Error creating collection: {e}")
        return

    points = []
    for entry in fallacies:
        # Combine name, description and example for a rich embedding
        text_to_embed = f"Fallacia: {entry['name']}. Descripción: {entry['description']}. Ejemplo: {entry['example']}"

        print(f"🔄 Indexing {entry['id']}: {entry['name']}...")
        embedding = analyzer.get_embedding(text_to_embed)

        if not embedding:
            print(f"⚠️ Failed to get embedding for {entry['id']}")
            continue

        points.append(PointStruct(
            id=str(uuid.uuid4()),
            vector=embedding,
            payload=entry
        ))

        # Batch upsert every 50 points
        if len(points) >= 50:
            client.upsert(collection_name=collection_name, points=points)
            points = []

    # Final upsert
    if points:
        client.upsert(collection_name=collection_name, points=points)

    print(f"✅ Indexed {len(fallacies)} fallacies into Qdrant.")

if __name__ == "__main__":
    index_fallacies()
