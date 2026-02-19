import logging
import os

from app.config import settings


os.makedirs(settings.LOG_FOLDER, exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )
    file_handler = logging.FileHandler(
        os.path.join(settings.LOG_FOLDER, f"{name}.log"),
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)

    return logger


# Пример инициализации логгера (можно вызывать из других модулей)
logger = get_logger("items_upload")
logger.info("Logger initialized")