# Hood API — справка по проекту

Официальная документация: **Hood API (V2.0.1 EN).pdf** (например, в `Downloads`).

## Подключение (из PDF)

- **URL:** `https://www.hood.de/api.htm`
- **Метод:** HTTP POST, тело — только XML.
- **Content-Type:** `text/xml; charset=UTF-8` (в проекте задаётся в `hood_api/client.py`).
- **Учётные данные:** API username и API password (MD5 hash). В проекте пароль передаётся как сырой MD5 hex; при необходимости можно перейти на формат `hash(MD5hex)` по документации.

## Функции API (из PDF)

| Функция | Назначение |
|--------|------------|
| **itemValidate** | Проверка XML без добавления товара, возвращает cost (комиссию). |
| **itemInsert** | Добавление одного товара. |
| **itemUpdate** | Обновление до 5 товаров; в запросе обязателен `<itemID>`. |
| **itemDelete** | Удаление товара по itemID. |
| **itemDetail** | Получение полной информации по товару по itemID. |
| **itemList** | Список товаров (running / sold / unsuccessful). Для пользователей **без магазина**. При наличии магазина заказы получают через **orderList**. |
| **itemStatus** | Детальная информация по товарам; itemID берут из ответа **itemList**. В запросе можно указать `<detailLevel>image,description</detailLevel>`. |
| **orderList** | Список заказов (для магазинов). |
| **updateOrderStatus** | Обновление статуса заказа. |
| **rateBuyer** | Оценка покупателя. |
| **shopCategoriesList / Insert / Update / Delete** | Категории магазина. |
| **categoriesBrowse** | Дерево категорий Hood.de (categoryID=0 — верхний уровень). |

## itemList и itemStatus (из PDF 3.3–3.6)

- **itemList** возвращает список товаров и их `<itemID>`.
- По этому **itemID** можно запрашивать детали через **itemStatus** (и при необходимости через **itemDetail**).
- Если при вызове **itemDetail** или **itemStatus** по ID из itemList приходит «Artikel nicht gefunden» или «globalError», возможны ограничения со стороны Hood (тип аккаунта, права, формат запроса). Имеет смысл уточнить в поддержке Hood: shop@hood.de.

## Скрипты в проекте

- **items/** — item_insert, item_validate, item_detail, item_list, item_status, item_update.
- **orders/** — order_list, updateOrderStatus, rateBuyer.
- **shopCategories/** — shopCategoriesList, shopCategoriesInsert, shopCategoriesUpdate, shopCategoriesDelete.
- **categoriesBrowse.py** — категории Hood.

Переменные окружения: `HOOD_API_USER`, `HOOD_API_PASSWORD`, при необходимости `HOOD_API_URL`. Для отладки: `HOOD_DEBUG=1`.
