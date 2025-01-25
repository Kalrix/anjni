from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ✅ Import existing routers
from api.app.auth import router as auth_router
from api.app.data_api import router as data_router
from api.app.search import router as search_router
from api.app.option_chain import router as option_chain_router
from api.app.dhan_api_input import router as dhan_router

# ✅ Fix Import for `oca_live_tracker`
from api.analysis.oca_live_tracker import router as live_tracker_router

# ✅ Initialize FastAPI App
app = FastAPI()

# ✅ Enable CORS for API Requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Register API Routers
app.include_router(auth_router, prefix="/api")
app.include_router(data_router, prefix="/api")
app.include_router(search_router, prefix="/api")
app.include_router(option_chain_router, prefix="/api")
app.include_router(live_tracker_router, prefix="/api")  # ✅ Ensure this works
app.include_router(dhan_router, prefix="/api")

# ✅ API Health Check Route
@app.get("/api/status")
def status():
    return {"message": "✅ ANJNI Backend Running!"}
