from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello from Celeri!"}

@app.get("/hello")
def hello():
    return {"message": "Hello from my addon!"}
