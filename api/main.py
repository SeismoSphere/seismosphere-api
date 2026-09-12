from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers.health import router as health_router
from api.routers.earthquakes import router as earthquakes_router

app = FastAPI(
    title="SeismoSphere API",
    description="FastAPI service for earthquake data access and summaries.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(earthquakes_router)

@app.get("/")
def root() -> dict:
    return {
        "service": "SeismoSphere API",
        "status": "running",
        "api": "/earthquakes",
        "docs": "/docs",
        "redoc": "/redoc",
    }

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
