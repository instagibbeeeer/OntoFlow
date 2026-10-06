"""GraphQL projection over the generated ontology models.

The scalar object models are generated from ontology/ontology.yaml. This file adds
relationship resolvers and action mutations that execute via the ontology runtime.
"""
import inspect

import strawberry
from typing import Optional, List
from generated_types import Machine, Part, Order, Batch, QualityFinding, MaintenanceAction, EngineeringDocument
from repository import get_object, query_objects, traverse_link, apply_action, rag


def _make(cls, d):
    if not d: return None
    fields=cls.__annotations__
    return cls(**{k:d.get(k) for k in fields})

@strawberry.type
class MachineView:
    id: str
    machine_id: Optional[str]=None
    name: Optional[str]=None
    status: Optional[str]=None
    production_line: Optional[str]=None
    @strawberry.field
    def batches(self)->List['BatchView']:
        return [batch_view(x) for x in traverse_link('Machine',self.machine_id,'batches')]
    @strawberry.field
    def quality_findings(self)->List['QualityFindingView']:
        return [finding_view(x) for x in traverse_link('Machine',self.machine_id,'quality_findings')]
    @strawberry.field
    def maintenance_actions(self)->List['MaintenanceActionView']:
        return [maintenance_view(x) for x in traverse_link('Machine',self.machine_id,'maintenance_actions')]
    @strawberry.field
    def documents(self)->List['DocumentView']:
        return [document_view(x) for x in traverse_link('Machine',self.machine_id,'documents')]

@strawberry.type
class PartView:
    id: str
    part_id: Optional[str]=None
    name: Optional[str]=None
    revision: Optional[str]=None
    @strawberry.field
    def quality_findings(self)->List['QualityFindingView']:
        return [finding_view(x) for x in traverse_link('Part',self.part_id,'quality_findings')]

@strawberry.type
class OrderView:
    id: str
    order_id: Optional[str]=None
    product: Optional[str]=None
    status: Optional[str]=None
    @strawberry.field
    def batches(self)->List['BatchView']:
        return [batch_view(x) for x in traverse_link('Order',self.order_id,'batches')]

@strawberry.type
class BatchView:
    id: str
    batch_id: Optional[str]=None
    machine_id: Optional[str]=None
    order_id: Optional[str]=None
    product: Optional[str]=None
    started_at: Optional[str]=None
    @strawberry.field
    def machine(self)->Optional[MachineView]:
        xs=traverse_link('Batch',self.batch_id,'runs_on'); return machine_view(xs[0]) if xs else None
    @strawberry.field
    def order(self)->Optional[OrderView]:
        xs=traverse_link('Batch',self.batch_id,'belongs_to'); return order_view(xs[0]) if xs else None
    @strawberry.field
    def quality_findings(self)->List['QualityFindingView']:
        return [finding_view(x) for x in traverse_link('Batch',self.batch_id,'quality_findings')]
    @strawberry.field
    def documents(self)->List['DocumentView']:
        return [document_view(x) for x in traverse_link('Batch',self.batch_id,'documents')]

@strawberry.type
class QualityFindingView:
    id: str
    finding_id: Optional[str]=None
    batch_id: Optional[str]=None
    part_id: Optional[str]=None
    machine_id: Optional[str]=None
    title: Optional[str]=None
    description: Optional[str]=None
    severity: Optional[str]=None
    status: Optional[str]=None
    timestamp: Optional[str]=None
    @strawberry.field
    def batch(self)->Optional[BatchView]:
        xs=traverse_link('QualityFinding',self.finding_id,'reported_for'); return batch_view(xs[0]) if xs else None
    @strawberry.field
    def affected_part(self)->Optional[PartView]:
        xs=traverse_link('QualityFinding',self.finding_id,'affects'); return part_view(xs[0]) if xs else None
    @strawberry.field
    def machine(self)->Optional[MachineView]:
        xs=traverse_link('QualityFinding',self.finding_id,'finding_machine'); return machine_view(xs[0]) if xs else None

@strawberry.type
class MaintenanceActionView:
    id: str
    maintenance_id: Optional[str]=None
    machine_id: Optional[str]=None
    action_type: Optional[str]=None
    description: Optional[str]=None
    timestamp: Optional[str]=None
    @strawberry.field
    def machine(self)->Optional[MachineView]:
        xs=traverse_link('MaintenanceAction',self.maintenance_id,'performed_on'); return machine_view(xs[0]) if xs else None

@strawberry.type
class DocumentView:
    id: str
    document_id: Optional[str]=None
    title: Optional[str]=None
    text: Optional[str]=None
    machine_id: Optional[str]=None
    batch_id: Optional[str]=None
    part_id: Optional[str]=None
    source_system: Optional[str]=None
    dq_score: Optional[float]=None
    valid: Optional[bool]=None

@strawberry.type
class Citation:
    id: str
    title: Optional[str]=None
    source: Optional[str]=None
    dq_score: Optional[float]=None

@strawberry.type
class RAGResponse:
    answer: str
    citations: List[Citation]
    context_object_ids: List[str]

import inspect

def _build_view(cls, data):
    params = inspect.signature(cls).parameters
    return cls(**{
        k: data.get(k)
        for k in params
        if k in data or k == "id"
    })

def machine_view(d):
    return _build_view(MachineView, d)

def part_view(d):
    return _build_view(PartView, d)

def order_view(d):
    return _build_view(OrderView, d)

def batch_view(d):
    return _build_view(BatchView, d)

def finding_view(d):
    return _build_view(QualityFindingView, d)

def maintenance_view(d):
    return _build_view(MaintenanceActionView, d)

def document_view(d):
    return _build_view(DocumentView, d)

    
@strawberry.type
class Query:
    @strawberry.field
    def machine(self,id:str)->Optional[MachineView]:
        d=get_object('Machine',id); return machine_view(d) if d else None
    @strawberry.field
    def batch(self,id:str)->Optional[BatchView]:
        d=get_object('Batch',id); return batch_view(d) if d else None
    @strawberry.field
    def quality_finding(self,id:str)->Optional[QualityFindingView]:
        d=get_object('QualityFinding',id); return finding_view(d) if d else None
    @strawberry.field
    def quality_findings(self,severity:Optional[str]=None,status:Optional[str]=None)->List[QualityFindingView]:
        f={k:v for k,v in {'severity':severity,'status':status}.items() if v is not None}
        return [finding_view(x) for x in query_objects('QualityFinding',f)]
    @strawberry.field
    def rag(self,query:str,dq_min:Optional[float]=None)->RAGResponse:
        r=rag(query,dq_min)
        return RAGResponse(answer=r['answer'],citations=[Citation(**c) for c in r['citations']],context_object_ids=[f"{o.get('object_type')}:{o.get('id')}" for o in r['context']])

@strawberry.type
class Mutation:
    @strawberry.mutation
    def acknowledge_finding(self,id:str,user:str)->QualityFindingView:
        return finding_view(apply_action('acknowledge_finding',id,{'user':user}))
    @strawberry.mutation
    def close_finding(self,id:str,user:str,resolution:str)->QualityFindingView:
        return finding_view(apply_action('close_finding',id,{'user':user,'resolution':resolution}))

schema=strawberry.Schema(query=Query,mutation=Mutation)
