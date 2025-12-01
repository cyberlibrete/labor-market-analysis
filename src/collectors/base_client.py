from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import requests
import time
import logging


class BaseClient(ABC):
    """
    Базовый клиент API для сервисов вакансий.
    Задает единый интерфейс и переиспользуемую логику запросов.
    """

    base_url:       str     = ""        # задается в наследниках
    max_retries:    int     = 3
    retry_delay:    float   = 1.0       # секунды

    
    def __init__(self, token: Optional[str] = None, timeout: int = 10):
        self.token      = token
        self.timeout    = timeout

        self.logger     = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)


    # ================================================================
    # Абстрактные методы - обязательны для всех клиетов
    # ================================================================

    @abstractmethod
    def build_headers(self) -> Dict[str, str]:
        """Собирает заголовки запросов."""
        pass

    def serach(self, query: str, **kwargs) -> Any:
        """Выполняет поиск вакансий (реализация в наследниках)."""
        pass


    # ================================================================
    # Универсальные методы для запросов
    # ================================================================

    def _requests(
            self,
            method: str,
            endpoint: str,
            params: Optional[dict] = None,
            data: Optional[dict] = None,
    ) -> Any:
        
        if not self.base_url:
            raise ValueError("base_url must be defined in the client class.")
        
        url = self.base_url + endpoint

        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Request: {method} {url}, params={params}")

                response = requests.request(
                    method=method,
                    url=url,
                    headers=self.build_headers(),
                    params=params,
                    json=data,
                    timeout=self.timeout,
                )

                # Ошибка HTTP
                if not response.ok:
                    self.logger.error(
                        f"HTTP error {response.status_code}: {response.text}"
                    )
                    response.raise_for_status()
                
                # JSON может быть битым - ловим
                try:
                    return response.json()
                except ValueError:
                    self.logger.error("Invalid JSON in response.")
                    raise RuntimeError("Response JSON decode error.")
                
            except (requests.ConnectionError, requests.Timeout) as e:
                self.logger.warning(
                    f"Connection error: {e}. Attemt {attempt} of {self.max_retries}"
                )
                if attempt == self.max_retries:
                    raise RuntimeError("Max retries exceeded.") from e
                
                time.sleep(self.retry_delay)

            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                raise

        raise RuntimeError("Unknown requests failure.") # теоретически недостижимо
    