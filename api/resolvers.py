from elasticsearch import Elasticsearch
es=Elasticsearch('http://elasticsearch:9200')

def search_events(dq_min=0.7, machine_id=None, batch_id=None, limit=50):
    filters=[{'range':{'dq_score':{'gte':dq_min}}},{'term':{'valid':True}}]
    if machine_id: filters.append({'term':{'machine_id':machine_id}})
    if batch_id: filters.append({'term':{'batch_id':batch_id}})
    result=es.search(index='engineering_events',query={'bool':{'filter':filters}},size=limit,sort=[{'ingested_at':'desc'}])
    return [h['_source'] | {'id':h['_id']} for h in result['hits']['hits']]

def search_documents(text, dq_min=0.7, limit=5):
    result=es.search(index='documents',query={'bool':{'must':[{'multi_match':{'query':text,'fields':['title^2','text']}}], 'filter':[{'range':{'dq_score':{'gte':dq_min}}},{'term':{'valid':True}}]}},size=limit)
    return [h['_source'] | {'id':h['_id'],'score':h['_score']} for h in result['hits']['hits']]

def related_batch(batch_id,dq_min=0.7):
    return search_events(dq_min=dq_min,batch_id=batch_id,limit=100)
