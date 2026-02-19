import os
import sys

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Добавляем корень проекта в PYTHONPATH, чтобы использовать общий пакет hood_api
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from app.items import endpoints as items_endpoints
from app.orders import endpoints as orders_endpoints
from app.shopCategories import endpoints as shop_categories_endpoints


app = FastAPI(title="Hood Api Manager")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev server
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(items_endpoints.router, prefix="/items", tags=["Items"])
app.include_router(orders_endpoints.router, prefix="/orders", tags=["Orders"])
app.include_router(
    shop_categories_endpoints.router,
    prefix="/shopCategories",
    tags=["ShopCategories"],
)