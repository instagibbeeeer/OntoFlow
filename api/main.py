from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from strawberry.fastapi import GraphQLRouter
from generated_schema import schema
from repository import get_object, query_objects, traverse_link, apply_action, rag

app=FastAPI(title='OntoFlow Ontology Platform',version='2.0')
app.include_router(GraphQLRouter(schema),prefix='/graphql')

class QueryBody(BaseModel):
    filters: dict = {}
    limit: int = 50
class ActionBody(BaseModel):
    inputs: dict = {}
class RagBody(BaseModel):
    query: str
    dq_min: float | None = None

@app.get('/health')
def health(): return {'status':'ok','version':'2.0'}

@app.get('/objects/{object_type}/{object_id}')
def get_obj(object_type:str,object_id:str):
    try: obj=get_object(object_type,object_id)
    except (KeyError,ValueError) as e: raise HTTPException(400,str(e))
    if not obj: raise HTTPException(404,'Object not found')
    return obj

@app.post('/objects/{object_type}/query')
def query_obj(object_type:str,body:QueryBody):
    try: return query_objects(object_type,body.filters,body.limit)
    except (KeyError,ValueError) as e: raise HTTPException(400,str(e))

@app.get('/objects/{object_type}/{object_id}/links/{link_name}')
def linked(object_type:str,object_id:str,link_name:str):
    try: return traverse_link(object_type,object_id,link_name)
    except (KeyError,ValueError) as e: raise HTTPException(400,str(e))

@app.post('/actions/{action_name}/{object_id}')
def action(action_name:str,object_id:str,body:ActionBody):
    try: return apply_action(action_name,object_id,body.inputs)
    except (KeyError,ValueError) as e: raise HTTPException(400,str(e))

@app.post('/rag')
def rag_endpoint(body:RagBody):
    return rag(body.query,body.dq_min)
