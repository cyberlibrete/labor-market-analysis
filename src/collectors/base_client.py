from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import requests
import time
import logging


class BaseClient(ABC):
    """
    Базовый абстрактный клиент API для сервисов вакансий.

    Предоставляет единый интерфейс для:
    - генерации загаловков
    - выполнения HTTP-запросов
    - повторных попыток (retry)
    - логирования
    - таймаутов

    Наследникам необходимо определеить:
    - base_url
    - build_headers()
    - search()
    """

    base_url:       str     = ""
    max_retries:    int     = 3
    retry_delay:    float   = 1.0

    
    def __init__(self, token: Optional[str] = None, timeout: int = 10):
        """
        Инициализирует базовый клиент API.

        Args:
            token (Optional[str]):
                Токен доступа, если он необходим сервису.
                Для HeadHunter токен обычно не требуется.
            timeout (int):
                Таймаут HTTP-запросов в секундах.
        """
        self.token      = token
        self.timeout    = timeout

        self.logger     = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)


    # ================================================================
    # Абстрактные методы - обязательны для всех клиетов
    # ================================================================

    @abstractmethod
    def build_headers(self) -> Dict[str, str]:
        """
        Формирует HTTP-заголовки для запросов.

        Returns:
            Dict[str, str]:
                Заголовки запросов, включая User-Agent, токен (если требуется)
                и другие параметры, специфичные для API
        """
        pass

    def serach(self, query: str, **kwargs) -> Any:
        """
        Выполняет поиск вакансий.

        Args:
            query (str):
                Поисковая строка.
        
        Returns:
            Any:
                JSON-ответ API сервиса.
            
        Note:
            Должен быть реализован в классах-наследниках.
        """
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
        """
        Выполняет HTTP-запрос с поддержкой retry, логированием и обработкой ошибок.

        Args:
            method (str):
                HTTP-метод ("GET", "POST", "PUT", ...).
            endpoint (str):
                Путь относительного base_url (например: "/vacancies").
            params (Optional[dict]):
                Query-параметры для URL.
            data (Optional[dict]):
                Тело запроса (JSON).

        Returns:
            Any:
                Распарсенный JSON-ответ сервиса.
            
        Raises:
            ValueError:
                Если base_url не установлен.
            RuntimeError:
                - если превышено количество повторов
                - если сервер вернул некорректный JSON
                - HTTP-ошибка.
            requests.HTTPError:
                Если сервер вернул HTTP-код ошибки.
        """
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
    