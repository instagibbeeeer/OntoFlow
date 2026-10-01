import strawberry
from typing import Optional, List
from resolvers import search_events, search_documents, related_batch

@strawberry.type
class EngineeringEvent:
    id: str
    event_id: Optional[str]
    entity_type: Optional[str]
    event_type: Optional[str]
    machine_id: Optional[str]
    station_id: Optional[str]
    part_id: Optional[str]
    batch_id: Optional[str]
    order_id: Optional[str]
    severity: Optional[str]
    timestamp: Optional[str]
    source_system: Optional[str]
    source_version: Optional[str]
    dq_score: float
    valid: bool

@strawberry.type
class Document:
    id: str
    document_id: Optional[str]
    title: Optional[str]
    text: Optional[str]
    source_system: Optional[str]
    owner: Optional[str]
    dq_score: float

@strawberry.type
class RAGResponse:
    answer: str
    citations: List[str]

def to_event(x):
    return EngineeringEvent(**{k:x.get(k) for k in EngineeringEvent.__annotations__})
def to_doc(x):
    return Document(**{k:x.get(k) for k in Document.__annotations__})

@strawberry.type
class Query:
    @strawberry.field
    def events(self,dq_min:float=0.7,machine_id:Optional[str]=None,batch_id:Optional[str]=None)->List[EngineeringEvent]:
        return [to_event(x) for x in search_events(dq_min,machine_id,batch_id)]
    @strawberry.field
    def batch_timeline(self,batch_id:str,dq_min:float=0.7)->List[EngineeringEvent]:
        return [to_event(x) for x in related_batch(batch_id,dq_min)]
    @strawberry.field
    def documents(self,query:str,dq_min:float=0.7)->List[Document]:
        return [to_doc(x) for x in search_documents(query,dq_min)]
    @strawberry.field
    def rag(self,query:str,dq_min:float=0.7)->RAGResponse:
        docs=search_documents(query,dq_min)
        if not docs: return RAGResponse(answer='No high-quality matching documents were found.',citations=[])
        snippets=[(d.get('text') or '')[:400] for d in docs]
        citations=[f"document_id={d.get('document_id')}; source={d.get('source_system')}; dq_score={d.get('dq_score')}" for d in docs]
        return RAGResponse(answer='Retrieved evidence: ' + ' '.join(snippets),citations=citations)
