from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
import strawberry
from schema import Query
app=FastAPI(title='OntoFlow Extended')
app.include_router(GraphQLRouter(strawberry.Schema(query=Query)),prefix='/graphql')
@app.get('/health')
def health(): return {'status':'ok'}
