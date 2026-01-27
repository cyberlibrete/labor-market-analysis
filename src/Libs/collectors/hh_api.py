from typing import Any, Dict, Optional
from .base_client import BaseClient


class HHClient(BaseClient):
    """
    Клиент для API HeadHunter (hh.ru).
    Поддерживает поиск вакансий, получение детальной информации,
    пагинацию и фильтрацию.
    """

    base_url = "https://api.hh.ru"

    def build_headers(self) -> Dict[str, str]:
        """
        Формирует HTTP-заголовки для запросов к API HH.

        Returns:
            Dict[str, str]: Заголовки, включающие корректный User-Agent, требуемый HH API
        """
        return {
            "User-Agent": "LaborMarketAnalyzer/1.0 (amaerto@yandex.ru)"
        }
    

    # Главная функция: поиск вакансий
    def serach(
            self,
            query: Optional[str] = None,
            page: int = 0,
            per_page: int = 100,
            area: Optional[int] = None,
            experience: Optional[str] = None,
            employment: Optional[str] = None,
            schedule: Optional[str] = None,
            only_with_salary: bool = False,
            date_from: Optional[str] = None,
            date_to: Optional[str] = None,
    ) -> Any:
        """
        Выполнение поиска вакансий через API hh.ru.

        Args:
            query (Optional[str], optional):
                Поисковый запрос (нпример: "python developer").

            page (int, optional):
                Номер страницы результата. Поумолчанию 0.

            per_page (int, optional):
                Количество вакансий на странице (максимум 100).

            area (Optional[int], optional):
                Код региона поиска (Москва = 1, Санкт-Петербург = 2 и тд).

            experience (Optional[str], optional):
                Фильтр по требуемому опыту.
                Возможные значения:
                - "noExperience"
                - "between1And3"
                - "between3And6"
                - "moreThan6"

            employment (Optional[str], optional):
                Тип занятости:
                - "full"
                - "part"
                - "project"
                - "volunteer"
                - "probation"

            schedule (Optional[str], optional):
                График работы:
                - "fullDay"
                - "shift"
                - "flexible"
                - "remote"
                - "flyInFlyOut"

            date_from (Optional[str], optional):
                Дата начала периода ('2024-01-01T00:00:00.00+03:00')

            date_to (Optional[str], optional):
                Дата конца периода ('2024-12-31T23:59:59.00+03:00')

            only_with_salary (bool, optional):
                Если True - возвращает вакансии, где указана зарплата.
            
        Returns:
            Any: JSON-ответ API HH, содержащий информацию о найденных вакансиях.
        
        Raises:
            RuntimeError:
                Если сервер HH недоступен или вернул ошибку.
        """

        params = {
            "text": query,
            "page": page,
            "per_page": per_page,
            "only_with_salary": only_with_salary,
        }

        if area is not None:
            params["area"] = area
        if experience:
            params["experience"] = experience
        if employment:
            params["employment"] = employment
        if schedule:
            params["schedule"] = schedule
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to
        
        return self._requests("GET", "/vacancies", params=params)


    # Получить детальную информацию о вакансии
    def get_area(self) -> Any:
        """
        Возвращает справочник регионов (areas) HH.

        Returns:
            Any: Полная древовидная стуктура регионов и городов HH
        
        Raises:
            RuntimeError: Ошибка при выполнении запроса.
        """
        return self._requests("GET", "/areas")


    # Получить информацию о работодателях
    def get_employer(self, employer_id: int) -> Any:
        """
        Получает информацию о работодателе.

        Args:
            employer_id (int):
                Идентификатор работодателя.
        
        Returns:
            Any: Данные о работодателе.

        Raises:
            RuntimeError: Если запрос к API завершился с ошибкой.
        """
        return self._requests("GET", f"/employers/{employer_id}")
    