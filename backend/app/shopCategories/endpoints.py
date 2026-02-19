from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from hood_api.config import ApiConfig
from hood_api.client import send_request
from hood_api.builders import (
    build_shop_categories_delete,
    build_shop_categories_insert,
    build_shop_categories_list,
    build_shop_categories_update,
)
from hood_api.api.parsers import (
    parse_shop_categories_list_response,
    parse_shop_category_mutation_response,
)


router = APIRouter()


@router.get("/list")
def list_shop_categories():
    """
    Обёртка над shopCategoriesList: список категорий магазина.
    """
    cfg = ApiConfig.from_env()
    xml_body = build_shop_categories_list(config=cfg)
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    return parse_shop_categories_list_response(response_xml)


class ShopCategoryInsertRequest(BaseModel):
    parent_id: str = "0"
    category_name: str


@router.post("/insert")
def insert_shop_category(payload: ShopCategoryInsertRequest):
    """
    Обёртка над shopCategoriesInsert: создание категории магазина.
    """
    cfg = ApiConfig.from_env()
    xml_body = build_shop_categories_insert(
        parent_id=payload.parent_id,
        category_name=payload.category_name,
        config=cfg,
    )
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    return parse_shop_category_mutation_response(response_xml)


class ShopCategoryUpdateRequest(BaseModel):
    category_id: str
    category_name: str


@router.post("/update")
def update_shop_category(payload: ShopCategoryUpdateRequest):
    """
    Обёртка над shopCategoriesUpdate: переименование категории.
    """
    cfg = ApiConfig.from_env()
    xml_body = build_shop_categories_update(
        category_id=payload.category_id,
        category_name=payload.category_name,
        config=cfg,
    )
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    return parse_shop_category_mutation_response(response_xml)


class ShopCategoryDeleteRequest(BaseModel):
    category_id: str


@router.post("/delete")
def delete_shop_category(payload: ShopCategoryDeleteRequest):
    """
    Обёртка над shopCategoriesDelete: удаление категории.
    """
    cfg = ApiConfig.from_env()
    xml_body = build_shop_categories_delete(
        category_id=payload.category_id,
        config=cfg,
    )
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    return parse_shop_category_mutation_response(response_xml)

