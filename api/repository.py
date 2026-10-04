import os, re, json, datetime as dt
from collections import defaultdict
import requests
from elasticsearch import Elasticsearch
from ontology_runtime import OBJECTS, LINKS, ACTIONS, CONSTRAINTS, object_spec
from embeddings import get_embedder

ES_URL=os.getenv('ELASTICSEARCH_URL','http://elasticsearch:9200')
es=Elasticsearch(ES_URL)
embedder=get_embedder()


def _source(hit):
    d=dict(hit.get('_source', {})); d['id']=hit.get('_id'); return d


def _keyword_field(prop_spec, prop):
    # searchable text properties get a .keyword subfield to get a full match search
    return prop + '.keyword' if prop_spec.get('searchable') and not prop_spec.get('keyword') else prop


def get_object(object_type, object_id):
    spec=object_spec(object_type); pk=spec['primary_key']; idx=spec['index']
    q={'bool':{'filter':[{'term':{'object_type':object_type}},{'term':{_keyword_field(spec['properties'][pk],pk):object_id}}]}}
    r=es.search(index=idx, query=q, size=1)
    hits=r['hits']['hits']
    return _source(hits[0]) if hits else None


def query_objects(object_type, filters=None, limit=50):
    spec=object_spec(object_type); idx=spec['index']; clauses=[{'term':{'object_type':object_type}}]
    for k,v in (filters or {}).items():
        if k not in spec['properties']: raise ValueError(f'{k} is not a property of {object_type}')
        clauses.append({'term':{_keyword_field(spec['properties'][k],k):v}})
    r=es.search(index=idx, query={'bool':{'filter':clauses}}, size=min(limit,200))
    return [_source(h) for h in r['hits']['hits']]


def traverse_link(source_type, source_id, link_name):
    source=get_object(source_type, source_id)
    if not source: return []
    # >>>>>
    if link_name in LINKS and LINKS[link_name]['from']==source_type:
        link=LINKS[link_name]; value=source.get(link['from_property'])
        if value is None: return []
        target=link['to']; target_spec=object_spec(target)
        return query_objects(target,{link['to_property']:value})
    # <<<<<
    for canonical,link in LINKS.items():
        if link.get('reverse_name')==link_name and link['to']==source_type:
            value=source.get(link['to_property'])
            if value is None: return []
            return query_objects(link['from'],{link['from_property']:value})
    raise ValueError(f'Unknown link {link_name} for {source_type}')


def apply_action(action_name, object_id, inputs):
    if action_name not in ACTIONS: raise ValueError('Unknown action')
    action=ACTIONS[action_name]; object_type=action['object']; spec=object_spec(object_type)
    obj=get_object(object_type,object_id)
    if not obj: raise ValueError(f'{object_type} {object_id} not found')
    for name,inp in action.get('inputs',{}).items():
        if inp.get('required') and not inputs.get(name): raise ValueError(f'Missing action input: {name}')
    patch=dict(action.get('set',{}))
    patch['updated_at']=dt.datetime.now(dt.timezone.utc).isoformat()
    es.update(index=spec['index'], id=obj['id'], doc=patch, refresh='wait_for')
    audit={
        'action':action_name,'object_type':object_type,'object_id':object_id,
        'inputs':inputs,'timestamp':dt.datetime.now(dt.timezone.utc).isoformat()
    }
    es.index(index='action_audit', document=audit, refresh='wait_for')
    return get_object(object_type,object_id)


def _rrf(rankings, k=60):
    scores=defaultdict(float); docs={}
    for ranking in rankings:
        for rank,item in enumerate(ranking,1):
            key=item['id']; scores[key]+=1.0/(k+rank); docs[key]=item
    return sorted(docs.values(), key=lambda x:scores[x['id']], reverse=True)


def _ontology_context(query, limit=5):
    should=[]
    for obj_type,spec in OBJECTS.items():
        if spec['index']=='documents': continue
        fields=[]
        for p in spec.get('searchable',[]):
            if p in spec['properties']:
                fields.append(p)
        if fields:
            should.append({'bool':{'filter':[{'term':{'object_type':obj_type}}], 'must':[{'multi_match':{'query':query,'fields':fields}}]}})
    if not should: return []
    r=es.search(index='ontology_objects', query={'bool':{'should':should,'minimum_should_match':1}}, size=limit)
    roots=[_source(h) for h in r['hits']['hits']]
    expanded=[]; seen=set()
    for root in roots:
        key=(root.get('object_type'),root.get('id'))
        if key not in seen: expanded.append(root); seen.add(key)
        typ=root.get('object_type'); spec=OBJECTS.get(typ,{})
        pk=spec.get('primary_key'); oid=root.get(pk)
        if not oid: continue
        for lname,link in LINKS.items():
            if link['from']==typ:
                for n in traverse_link(typ,oid,lname)[:10]:
                    nk=(n.get('object_type'),n.get('id'))
                    if nk not in seen: expanded.append(n); seen.add(nk)
            elif link['to']==typ and link.get('reverse_name'):
                for n in traverse_link(typ,oid,link['reverse_name'])[:10]:
                    nk=(n.get('object_type'),n.get('id'))
                    if nk not in seen: expanded.append(n); seen.add(nk)
    return expanded


def _doc_filters(context, dq_min):
    filters=[{'range':{'dq_score':{'gte':dq_min}}},{'term':{'valid':True}},{'term':{'object_type':'EngineeringDocument'}}]
    should=[]
    for obj in context:
        for key in ('batch_id','machine_id','part_id'):
            if obj.get(key): should.append({'term':{key:obj[key]}})
    return filters, should


def hybrid_retrieve(query, dq_min=None, limit=6):
    dq_min=dq_min if dq_min is not None else CONSTRAINTS.get('rag_dq_min',0.75)
    context=_ontology_context(query)
    filters, related=_doc_filters(context,dq_min)
    lexical_query={'bool':{'must':[{'multi_match':{'query':query,'fields':['title^3','text']}}], 'filter':filters}}
    if related:
        lexical_query['bool']['should']=related; lexical_query['bool']['minimum_should_match']=0
    bm=es.search(index='documents',query=lexical_query,size=limit*2)
    bm_hits=[_source(h) for h in bm['hits']['hits']]
    vector=embedder.embed(query)
    knn_filter={'bool':{'filter':filters}}
    if related:
        knn_filter['bool']['should']=related; knn_filter['bool']['minimum_should_match']=0
    try:
        vr=es.search(index='documents', knn={'field':'embedding','query_vector':vector,'k':limit*2,'num_candidates':max(50,limit*10),'filter':knn_filter}, size=limit*2)
        vec_hits=[_source(h) for h in vr['hits']['hits']]
    except Exception:
        vec_hits=[]
    docs=_rrf([bm_hits,vec_hits])[:limit]
    return {'documents':docs,'context':context}

def _llm_answer(query,evidence):
    key=os.getenv('LLM_API_KEY'); model=os.getenv('LLM_MODEL'); base=os.getenv('LLM_BASE_URL','https://api.openai.com/v1')
    if not key or not model: return None
    prompt=("Answer only from the supplied engineering evidence. Cite evidence using [E1], [E2], etc. "
            "If evidence is insufficient, say so.\n\nQuestion: "+query+"\n\nEvidence:\n"+evidence)
    r=requests.post(base.rstrip('/')+'/chat/completions',headers={'Authorization':f'Bearer {key}','Content-Type':'application/json'},json={'model':model,'messages':[{'role':'user','content':prompt}],'temperature':0},timeout=30)
    r.raise_for_status(); return r.json()['choices'][0]['message']['content']


def rag(query,dq_min=None,limit=6):
    result=hybrid_retrieve(query,dq_min,limit); docs=result['documents']; context=result['context']
    evidence=[]; citations=[]
    for i,d in enumerate(docs,1):
        evidence.append(f"[E{i}] {d.get('title','Untitled')}: {(d.get('text') or '')[:900]}")
        citations.append({'id':d.get('document_id') or d['id'],'source':d.get('source_system'),'dq_score':d.get('dq_score'),'title':d.get('title')})
    # Include linked ontology facts so RAG is not merely document BM25.
    facts=[]
    for o in context[:12]:
        facts.append(f"{o.get('object_type')}: "+json.dumps({k:v for k,v in o.items() if k not in ('embedding','id') and v is not None},default=str))
    all_evidence='\n'.join(facts+evidence)
    if not docs and not context:
        return {'answer':'No quality-approved ontology objects or documents matched the question.','citations':[],'context':[]}
    answer=_llm_answer(query,all_evidence)
    if not answer:
        summary=' '.join((d.get('text') or d.get('title') or '')[:300] for d in docs[:3])
        answer=f"Ontology context: {len(context)} related objects. Retrieved evidence: {summary}".strip()
    return {'answer':answer,'citations':citations,'context':context}


