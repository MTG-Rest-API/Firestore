# Welcome to Cloud Functions for Firebase for Python!
# To get started, simply uncomment the below code or create your own.
# Deploy with `firebase deploy`

from concurrent.futures import wait
from datetime import datetime
from firebase_functions import options, scheduler_fn, https_fn, pubsub_fn
from firebase_admin import initialize_app, firestore
from google.cloud import pubsub_v1
import json
import time

from fetcher import fetch_cards
from transformer import transform

app = initialize_app()
options.set_global_options(max_instances=1, memory=options.MemoryOption.GB_4, cpu=2, timeout_sec=540)

def chunk_documents(documents, chunk_size = 500):      
    for i in range(0, len(documents), chunk_size):  
        yield documents[i:i + chunk_size]

@scheduler_fn.on_schedule(schedule="0 3 * * *")
def publish_cards(event):
    
    db = firestore.client()
    
    _, run = db.collection("run").add({"start": firestore.SERVER_TIMESTAMP})
        
    publisher = pubsub_v1.PublisherClient()
    data = fetch_cards("https://api.scryfall.com/bulk-data/default-cards")
    cards = transform(data)
    
    total = len(cards)
    futures = []
    count = 0
    for chunk in chunk_documents(cards, chunk_size=500):
        body = {
            "runId": run.id,
            "batch_count": len(chunk),
            "batch_total": total,
            "chunk": chunk,
            "count": count,
        }
        print(f'{datetime.now()}: Sending Chunk {count}')
        future = publisher.publish("projects/mtg-rest/topics/simple_topic", json.dumps(body).encode("utf8"))
        print(f'{datetime.now()}: Sent Chunk {count}')
        count += 1
        futures.append(future)
    wait(futures)
    run.update({"end": firestore.SERVER_TIMESTAMP})
    print("end -> ", datetime.now(), flush=True)
        
@pubsub_fn.on_message_published(topic="cards")
def on_cards_published(event: pubsub_fn.CloudEvent[pubsub_fn.MessagePublishedData]):
    
    start = time.time()
    data = event.data.message.json
    db = firestore.client()
        
    print(f'{datetime.now()}: Adding Count {data["count"]}')
    
    _, chunk = (db.collection("run")
                    .document(data["runId"])
                    .collection("chunks")
                    .add({
                        "start": firestore.SERVER_TIMESTAMP, 
                        "chunkId": data["count"], 
                        "batch_count": data["batch_count"]
                    }))
    

    cards = db.collection("cards")
    batch = db.batch()
    for doc in data["chunk"]:
        card = cards.document(doc["id"])
        batch.set(card, doc, merge=True)
    batch.commit()

    print(f'{datetime.now()} Added Count {data["count"]}')
    
    end = time.time()
    chunk.update({"end": firestore.SERVER_TIMESTAMP, "took": end - start})
