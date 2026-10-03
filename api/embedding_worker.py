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
        for hit in r['hits']['hits']:
            src=hit['_source']; text=((src.get('title') or '')+'\n'+(src.get('text') or '')).strip()
            es.update(index='documents',id=hit['_id'],doc={'embedding':embedder.embed(text)})
        if r['hits']['hits']: es.indices.refresh(index='documents')
    except Exception as e:
        print('embedding worker:',e)
    time.sleep(interval)
