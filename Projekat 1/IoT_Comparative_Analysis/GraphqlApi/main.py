from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from schema import schema

app = FastAPI(title="IoT GraphQL Service")


graphql_app = GraphQLRouter(schema)


app.include_router(graphql_app, prefix="/graphql")

@app.get("/")
async def root():
    return {"message": "GraphQL IoT servis je aktivan. Idite na /graphql za GraphiQL interfejs."}