from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from hood_api.config import ApiConfig
from hood_api.client import send_request
from hood_api.builders import (
    build_order_list,
    build_rate_buyer,
    build_update_order_status,
)
from hood_api.api.parsers import (
    parse_order_list_response,
    parse_rate_buyer_response,
    parse_update_order_status_response,
)


router = APIRouter()


class OrderListQuery(BaseModel):
    start_date: str
    end_date: str
    list_mode: str = "details"
    order_id: Optional[str] = None


@router.post("/list")
def order_list(payload: OrderListQuery):
    """
    Обёртка над orderList: список заказов за период.
    Даты в формате DD/MM/YYYY.
    """
    cfg = ApiConfig.from_env()
    xml_body = build_order_list(
        start_date=payload.start_date,
        end_date=payload.end_date,
        list_mode=payload.list_mode,
        order_id=payload.order_id,
        config=cfg,
    )
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    return parse_order_list_response(response_xml)


class RateBuyerEntry(BaseModel):
    orderID: str
    rating: str  # positive | neutral | negative
    ratingText: str


class RateBuyerRequest(BaseModel):
    orders: List[RateBuyerEntry]


@router.post("/rate-buyer")
def rate_buyer(payload: RateBuyerRequest):
    """
    Обёртка над rateBuyer: оценка покупателя по заказам.
    """
    cfg = ApiConfig.from_env()
    orders = [o.dict() for o in payload.orders]
    xml_body = build_rate_buyer(orders=orders, config=cfg)
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    return parse_rate_buyer_response(response_xml)


class UpdateOrderStatusEntry(BaseModel):
    orderID: str
    statusAction: str
    trackingCode: Optional[str] = None
    carrier: Optional[str] = None
    messageText: Optional[str] = None


class UpdateOrderStatusRequest(BaseModel):
    orders: List[UpdateOrderStatusEntry]


@router.post("/update-status")
def update_order_status(payload: UpdateOrderStatusRequest):
    """
    Обёртка над updateOrderStatus: изменение статуса заказов.
    """
    cfg = ApiConfig.from_env()
    orders = [o.dict(exclude_none=True) for o in payload.orders]
    xml_body = build_update_order_status(orders=orders, config=cfg)
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    return parse_update_order_status_response(response_xml)

