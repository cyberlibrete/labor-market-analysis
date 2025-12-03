from __future__ import annotations
from typing import Any, Dict, Optional, List
from .base_client import BaseClient

import logging
import requests

logger = logging.getLogger(__name__)

class SuperJobAPI(BaseClient):
    """
    Клиент для работы с SuperJob API.

    Базовая документация API:
    https://api.superjob.ru

    Данный класс реализует сбор вакансий
    """

    base_url: str = "https://api.superjob.ru/2.0/"

    def __init__(self, api_key: str, *, timeout: int = 10) -> None:
        """
        Инициализация клиента SuperJob.

        Args:
            api_key (str):
                Ключ доступа к API (X-Api-App-Id).
                Необходимо получить в личном кабинете SuperJob.
            timeout (int, optional):
                Таймаут HTTP-запросов в секундах. По умолчанию 10.
        """
        super().__init__(timeout=timeout)
        self.api_key = api_key

    
    def build_headers(self) -> Dict[str, str]:
        """
        Формирует HTTP-заголовки для запросов к SuperJob API.

        Returns:
            Dict[str, str]: Заголовки запросов, включая User-Agent и X-Api-App-Id.
        """
        if not self.api_key:
            raise ValueError("SuperJob API key is required (set via token).")
        
        return {
            "X-Api-App-Id": self.api_key,
            "User-Agent": "LaborMarketAnalyzer/1.0 (amaerto@ya.ru)",
        }
    
    def search(
        self,
        *,
        keyword: Optional[str] = None,
        page: int = 0,
        count: int = 100,
        town: Optional[int] = None,
        catalogues: Optional[int] = None,
    ) -> Any:
        """
        Выполняет поиск вакансий через API SuperJob.

        Args:
            keyword (Optional[str], optional):
                Поисковое слово или фраза. Например: "python developer".
            page (int, optional):
                Номер страницы (начиная с 0). По умолчанию 0.
            count (int, optional):
                Количество вакансий на странице (до 100). По умолчанию 100.
            town (Optional[int], optional):
                ID города (например: 4 — Москва, 3 — Санкт-Петербург).
            catalogues (Optional[int], optional):
                Категория профессии (например: 48 — IT, интернет, связь).

        Returns:
            Any: JSON-ответ API SuperJob с найденными вакансиями.

        Raises:
            RuntimeError: Ошибка при выполнении запроса.
        """
        params: Dict[str, Any] = {"page": page, "count": count}
        if keyword:
            params["keyword"] = keyword
        if town:
            params["town"] = town
        if catalogues:
            params["catalogues"] = catalogues

        return self._request("GET", "/vacancies/", params=params)
    

    def get_catalogues(self) -> Any:
        """
        Возвращает справочник профессий (каталоги).

        Returns:
            Any: Список каталогов профессий.

        Raises:
            RuntimeError: Ошибка при выполнении запроса.
        """
        return self._request("GET", "/catalogues/")


    def get_vacancy(self, vacancy_id: int) -> Any:
        """
        Получает детальную информацию о вакансии.

        Args:
            vacancy_id (int): Идентификатор вакансии.

        Returns:
            Any: Подробная информация о вакансии.

        Raises:
            RuntimeError: Ошибка при выполнении запроса.
        """
        return self._request("GET", f"/vacancies/{vacancy_id}/")

