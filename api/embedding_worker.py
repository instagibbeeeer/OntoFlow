import os,time
from elasticsearch import Elasticsearch
from embeddings import get_embedder

es=Elasticsearch(os.getenv('ELASTICSEARCH_URL','http://elasticsearch:9200'))
embedder=get_embedder()
interval=int(os.getenv('EMBEDDING_POLL_SECONDS','5'))
print('OntoFlow embedding worker started')
while True:
    try:
        r=es.search(index='documents',query={'bool':{'must_not':[{'exists':{'field':'embedding'}}]}},size=100)
        actions = []

        for hit in r["hits"]["hits"]:
            source = hit["_source"]

            text = (
                (source.get("title") or "")
                + "\n"
                + (source.get("text") or "")
            ).strip()

            if not text:
                continue

            embedding = embedder.embed(text)

            actions.append({
                "_op_type": "update",
                "_index": "documents",
                "_id": hit["_id"],
                "doc": {
                    "embedding": embedding,
                },
            })

        if actions:
            helpers.bulk(es, actions)
            es.indices.refresh(index="documents")

            print(f"Embedded {len(actions)} documents")
    except Exception as e:
        print('embedding worker:',e)
    time.sleep(interval)
