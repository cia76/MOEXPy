import logging  # Будем вести лог
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # ВременнАя зона
from json import loads  # Отправляем запросы и получаем ответы в формае JSON

import keyring  # Безопасное хранение торгового токена
from requests import get  # Запросы через HTTP API


class MOEXPy:
    """Работа с Algopack API Московской Биржи https://moexalgo.github.io/docs/api из Python"""
    tz_msk = ZoneInfo('Europe/Moscow')  # Московская Биржа работает по московскому времени
    logger = logging.getLogger('MOEXPy')  # Будем вести лог

    def __init__(self, token=None):
        """Инициализация

        :param str token: Токен
        """
        self.api_server = 'https://apim.moex.com/iss'  # Информационно-статистический сервер запросов на Московской Бирже
        if token is None:  # Если торговый токен не указан
            self.token = self.get_long_token_from_keyring('MOEXPy', 'token')  # то получаем его из защищенного хранилища по частям
        else:  # Если указан торговый токен
            self.token = token  # Торговый токен
            self.set_long_token_to_keyring('MOEXPy', 'token', self.token)  # Сохраняем его в защищенное хранилище
        self.headers = {'Accept': 'application/json', 'Authorization': f'Bearer {self.token}'}  # Заголовки для запросов

    # Real-time market data - Акции - https://moexalgo.github.io/docs/api/real-time-market-data-акции
    # Real-time market data - Фьючерсы - https://moexalgo.github.io/docs/api/real-time-market-data-фьючерсы

    def get_all_tickers(self, market):
        """Торговая статистика за сегодня по всем инструментам

        param str market: Рынок. 'shares' - акции, 'futures' - фьючерсы
        """
        if market == 'shares':  # Если рынок акций
            url = f'{self.api_server}/engines/stock/markets/shares/boards/tqbr/securities.json'  # URL запроса
        elif market == 'futures':  # Если рынок фьючерсов
            url = f'{self.api_server}/engines/futures/markets/forts/boards/rfud/securities.json'  # URL запроса
        else:  # Если рынок неизвестен
            self.logger.error(f'Неизвестный рынок {market}')
            return None
        start = 0  # Начинаем получать данные с первой записи интервала
        all_data = None  # Накопленные данные
        while True:  # Пока не обработаем все периоды запроса
            params = {
                'start': start   # Номер первой записи с начала интервала
            }
            response = get(url, params=params, headers=self.headers)  # Отправляем запрос, получаем ответ
            content = loads(response.content.decode('utf-8'))  # Результат запроса в виде JSON
            data = content['securities']['data']  # Пришедшие данные
            if len(data) == 0:  # Если данных нет (достигнут конец выборки)
                break  # то выходим
            if all_data is None:  # Если это первые пришедшие данные
                all_data = content  # то сохраняем их полностью
            else:  # Если данные уже есть
                all_data['securities']['data'].extend(data)  # то добавляем к уже имеющимся
            start += len(data)  # Номер первой записи перемещаем за последнюю полученную
        return all_data

    def get_ticker(self, market, ticker):
        """Торговая статистика за сегодня по инструменту

        param str market: Рынок. 'shares' - акции, 'futures' - фьючерсы
        param str ticker: Тикер
        """
        if market == 'shares':  # Если рынок акций
            url = f'{self.api_server}/engines/stock/markets/shares/boards/tqbr/securities/{ticker}.json'  # URL запроса
        elif market == 'futures':  # Если рынок фьючерсов
            url = f'{self.api_server}/engines/futures/markets/forts/boards/rfud/securities/{ticker}.json'  # URL запроса
        else:  # Если рынок неизвестен
            self.logger.error(f'Неизвестный рынок {market}')
            return None
        response = get(url, headers=self.headers)  # Отправляем запрос, получаем ответ
        return loads(response.content.decode('utf-8'))  # Результат запроса в виде JSON

    def get_candles(self, market, ticker, dt_from, dt_till, interval):
        """Свечи по инструменту

        param str market: Рынок. 'shares' - акции, 'futures' - фьючерсы
        param str ticker: Тикер
        param datetime dt_from: Дата и время начала запроса
        param datetime dt_till: Дата и время окончания запроса
        param int interval: Временной интервал. 1 - 'M1', 10 - 'M10', 60 - 'M60', 24 - 'D1', 7 - 'W1', 31 - 'MN1', 4 - 'MN3'
        """
        if market == 'shares':  # Если рынок акций
            url = f'{self.api_server}/engines/stock/markets/shares/boards/tqbr/securities/{ticker}/candles.json'  # URL запроса
        elif market == 'futures':  # Если рынок фьючерсов
            url = f'{self.api_server}/engines/futures/markets/forts/boards/rfud/securities/{ticker}/candles.json'  # URL запроса
        else:  # Если рынок неизвестен
            self.logger.error(f'Неизвестный рынок {market}')
            return None
        all_data = None  # Накопленные данные
        while dt_from < dt_till:  # Пока не обработаем все периоды запроса
            params = {
                'from': dt_from,  # Дата и время начала запроса
                'till': dt_till,  # Дата и время окончания запроса
                'interval': interval  # Временной интервал
            }
            response = get(url, params=params, headers=self.headers)  # Отправляем запрос, получаем ответ
            content = loads(response.content.decode('utf-8'))  # Результат запроса в виде JSON
            data = content['candles']['data']  # Пришедшие данные
            if len(data) == 0:  # Если данных нет (достигнут конец выборки)
                break  # то выходим
            if all_data is None:  # Если это первые пришедшие данные
                all_data = content  # то сохраняем их полностью
            else:  # Если данные уже есть
                all_data['candles']['data'].extend(data)  # то добавляем к уже имеющимся
            dt_from = datetime.strptime(data[-1][-2], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=1)  # Дата и время начала следующего периода
        return all_data

    def get_orderbook(self, market, ticker):
        """Стакан котировок по инструменту

        param str market: Рынок. 'shares' - акции, 'futures' - фьючерсы
        param str ticker: Тикер
        """
        if market == 'shares':  # Если рынок акций
            url = f'{self.api_server}/engines/stock/markets/shares/boards/tqbr/securities/{ticker}/orderbook.json'  # URL запроса
        elif market == 'futures':  # Если рынок фьючерсов
            url = f'{self.api_server}/engines/futures/markets/forts/boards/rfud/securities/{ticker}/orderbook.json'  # URL запроса
        else:  # Если рынок неизвестен
            self.logger.error(f'Неизвестный рынок {market}')
            return None
        response = get(url, headers=self.headers)  # Отправляем запрос, получаем ответ
        return loads(response.content.decode('utf-8'))  # Результат запроса в виде JSON

    def get_trades(self, market, ticker, tradeno=None):
        """Все сделки по инструменту

        param str market: Рынок. 'shares' - акции, 'futures' - фьючерсы
        param str ticker: Тикер
        param int tradeno: Получить сделки, которые идут начиная с указанного номера
        """
        if market == 'shares':  # Если рынок акций
            url = f'{self.api_server}/engines/stock/markets/shares/boards/tqbr/securities/{ticker}/trades.json'  # URL запроса
        elif market == 'futures':  # Если рынок фьючерсов
            url = f'{self.api_server}/engines/futures/markets/forts/boards/rfud/securities/{ticker}/trades.json'  # URL запроса
        else:  # Если рынок неизвестен
            self.logger.error(f'Неизвестный рынок {market}')
            return None
        params = {}  # Параметров нет
        if tradeno is not None:  # Но если указан номер сделки
            params = {
                'tradeno': tradeno  # То будем получать сделки начиная с указанного номера
            }
        response = get(url, params=params, headers=self.headers)  # Отправляем запрос, получаем ответ
        return loads(response.content.decode('utf-8'))  # Результат запроса в виде JSON

    # Super Candles - Акции - https://moexalgo.github.io/docs/api/super-candles-акции
    # Super Candles - Фьючерсы - https://moexalgo.github.io/docs/api/super-candles-фьючерсы
    # Super Candles - Валюта - https://moexalgo.github.io/docs/api/super-candles-валюта

    # https://apim.moex.com/iss/datashop/algopack/eq/tradestats.json - Метрики рассчитанные на основе потока сделок (tradestats) по всем инструментам
    # https://apim.moex.com/iss/datashop/algopack/eq/tradestats/:ticker.json - Метрики рассчитанные на основе потока сделок (tradestats) по инструменту
    # https://apim.moex.com/iss/datashop/algopack/eq/obstats.json - Метрики рассчитанные на основе стакана котировок (obstats) по всем инструментам
    # https://apim.moex.com/iss/datashop/algopack/eq/obstats/:ticker.json - Метрики рассчитанные на основе стакана котировок (obstats) по инструменту
    # https://apim.moex.com/iss/datashop/algopack/eq/orderstats.json - Метрики рассчитанные на основе потока заявок (orderstats) по всем инструментам
    # https://apim.moex.com/iss/datashop/algopack/eq/orderstats/:ticker.json - Метрики рассчитанные на основе потока заявок (orderstats) по инструменту

    # https://apim.moex.com/iss/datashop/algopack/fo/tradestats.json - Метрики рассчитанные на основе потока сделок (tradestats) по всем инструментам
    # https://apim.moex.com/iss/datashop/algopack/fo/tradestats/:ticker.json - Метрики рассчитанные на основе потока сделок (tradestats) по инструменту
    # https://apim.moex.com/iss/datashop/algopack/fo/obstats.json - Метрики рассчитанные на основе стакана котировок (obstats) по всем инструментам
    # https://apim.moex.com/iss/datashop/algopack/fo/obstats/:ticker.json - Метрики рассчитанные на основе стакана котировок (obstats) по инструменту

    # https://apim.moex.com/iss/datashop/algopack/fx/tradestats.json - Метрики рассчитанные на основе потока сделок (tradestats) по всем инструментам
    # https://apim.moex.com/iss/datashop/algopack/fx/tradestats/:ticker.json - Метрики рассчитанные на основе потока сделок (tradestats) по инструменту
    # https://apim.moex.com/iss/datashop/algopack/fx/obstats.json - Метрики рассчитанные на основе стакана котировок (obstats) по всем инструментам
    # https://apim.moex.com/iss/datashop/algopack/fx/obstats/:ticker.json - Метрики рассчитанные на основе стакана котировок (obstats) по инструменту
    # https://apim.moex.com/iss/datashop/algopack/fx/orderstats.json - Метрики рассчитанные на основе потока заявок (orderstats) по всем инструментам
    # https://apim.moex.com/iss/datashop/algopack/fx/orderstats/:ticker.json - Метрики рассчитанные на основе потока заявок (orderstats) по инструменту

    # Futures Open Interest (FUTOI) - https://moexalgo.github.io/docs/api/futures-open-interest-futoi

    # https://apim.moex.com/iss/analyticalproducts/futoi/securities.json - Futures Open Interest (FUTOI) по всем инструментам

    def get_all_futoi(self, date):
        """Futures Open Interest (FUTOI) по всем инструментам

        :param date date: Дата торгов
        """
        url = f'{self.api_server}/analyticalproducts/futoi/securities.json'  # URL запроса
        start = 0  # Начинаем получать данные с первой записи
        all_data = None  # Накопленные данные
        while True:  # Пока не обработаем все инструменты
            params = {
                'date': date,  # Дата торгов
                'start': start   # Номер первой записи с начала интервала
            }
            response = get(url, params=params, headers=self.headers)  # Отправляем запрос, получаем ответ
            content = loads(response.content.decode('utf-8'))  # Результат запроса в виде JSON
            data = content['futoi']['data']  # Пришедшие данные
            if len(data) == 0:  # Если данных нет (достигнут конец выборки)
                break  # то выходим
            if all_data is None:  # Если это первые пришедшие данные
                all_data = content  # то сохраняем их полностью
            else:  # Если данные уже есть
                all_data['futoi']['data'].extend(data)  # то добавляем к уже имеющимся
            start += len(data)  # Номер первой записи перемещаем за последнюю полученную
        return all_data

    def get_futoi(self, ticker, dt_from, dt_till):
        """Futures Open Interest (FUTOI) по инструменту

        :param str ticker: Тикер
        :param date dt_from: Дата и время начала запроса
        :param date dt_till: Дата и время окончания запроса
        """
        url = f'{self.api_server}/analyticalproducts/futoi/securities/{ticker}.json'  # URL запроса
        all_data = None  # Накопленные данные

        days = (dt_till - dt_from).days  # Пагинация по торговым сессиям
        for i in range(0, days + 1, 2):  # В каждом запросе, гарантированно, вмещаются 2 дня
            params = {
                'from': dt_till - timedelta(days=i+1),  # Дата и время начала запроса
                'till': dt_till - timedelta(days=i),  # Дата и время окончания запроса
            }
            response = get(url, params=params, headers=self.headers)  # Отправляем запрос, получаем ответ
            content = loads(response.content.decode('utf-8'))  # Результат запроса в виде JSON
            data = content['futoi']['data']  # Пришедшие данные
            if all_data is None:  # Если это первые пришедшие данные
                all_data = content  # то сохраняем их полностью
            elif len(data) > 0:  # Если данные уже есть и пришли не пустые
                all_data['futoi']['data'].extend(data)  # то добавляем к уже имеющимся

        # TODO Пагинация (параметр start) не работает у Московской Биржи
        # start = 0  # Начинаем получать данные с первой записи интервала
        # while True:  # Пока не обработаем все периоды запроса
        #     params = {
        #         'from': dt_from,  # Дата и время начала запроса
        #         'till': dt_till,  # Дата и время окончания запроса
        #         'start': start   # Номер первой записи с начала интервала
        #     }
        #     response = get(url, params=params, headers=self.headers)  # Отправляем запрос, получаем ответ
        #     content = loads(response.content.decode('utf-8'))  # Результат запроса в виде JSON
        #     data = content['futoi']['data']  # Пришедшие данные
        #     if len(data) == 0:  # Если данных нет (достигнут конец выборки)
        #         break  # то выходим
        #     print(params, data[0], data[-1])
        #     if all_data is None:  # Если это первые пришедшие данные
        #         all_data = content  # то сохраняем их полностью
        #     else:  # Если данные уже есть
        #         all_data['futoi']['data'].extend(data)  # то добавляем к уже имеющимся
        #     start += len(data)  # Номер первой записи перемещаем за последнюю полученную

        return all_data

    # Market Concentration (HI2) - https://moexalgo.github.io/docs/api/market-concentration-hi-2

    # https://apim.moex.com/iss/datashop/algopack/:market/hi2.json - Индекс рыночной концентрации по всем инструментам
    # https://apim.moex.com/iss/datashop/algopack/:market/hi2/:ticker.json - Индекс рыночной концентрации по инструменту

    # Mega Alerts - https://moexalgo.github.io/docs/api/mega-alerts

    # https://apim.moex.com/iss/datashop/algopack/:market/alerts.json - Торговые аномалии по всем инструментам
    # https://apim.moex.com/iss/datashop/algopack/:market/alerts/:ticker.json - Торговые аномалии по инструменту

    # Запросы REST

    def check_result(self, response):
        """Анализ результата запроса

        :param Response response: Результат запроса
        :return: Справочник из JSON, текст, None в случае веб ошибки
        """
        if response is None:  # Если ответ не пришел. Например, при таймауте
            self.logger.error('Ошибка запроса: Таймаут')  # Событие ошибки
            return None  # то возвращаем пустое значение
        content = response.content.decode('utf-8')  # Результат запроса
        if response.status_code != 200:  # Если статус ошибки
            self.logger.error(f'Ошибка запроса: {response.status_code} Запрос: {response.request.path_url} Ответ: {content}')  # Событие ошибки
            return None  # то возвращаем пустое значение
        self.logger.debug(f'Запрос : {response.request.path_url}')
        self.logger.debug(f'Ответ  : {content}')
        return loads(content)  # Декодируем JSON в справочник, возвращаем его. Ошибки также могут приходить в виде JSON

    # Функции конвертации

    @staticmethod
    def dataname_to_moex_market_symbol(dataname) -> tuple[str | None, str]:
        """Код рынка Московской Биржи и тикер из названия тикера

        :param str dataname: Название тикера
        :return: Код рынка Московской Биржи и тикер
        """
        market_map = {'TQBR': 'shares', 'SPBFUT': 'futures', 'CETS': 'fx'}  # TODO Дополнить всеми <режимами торгов>: <рынками>
        symbol_parts = dataname.split('.')  # По разделителю пытаемся разбить тикер на части
        if len(symbol_parts) >= 2:  # Если тикер задан в формате <Код режима торгов>.<Код тикера>
            board = symbol_parts[0]  # Код режима торгов
            symbol = '.'.join(symbol_parts[1:])  # Код тикера
        else:  # Если тикер задан без кода режима торгов
            return None, dataname  # то код рынка неизвестен
        moex_market = market_map.get(board)  # Пытаемся получить код рынка Московской Биржи
        return moex_market, symbol

    @staticmethod
    def timeframe_to_moex_timeframe(tf: str) -> int:
        """Перевод временнОго интервала во временной интервал Московской Биржи

        :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        :return: Временной интервал Московской Биржи
        """
        tf_map = {'M1': 1, 'M10': 10, 'M60': 60, 'D1': 24, 'W1': 7, 'MN1': 31, 'MN3': 4}  # Справочник временнЫх интервалов
        if tf in tf_map:  # Если временной интервал есть в справочнике
            return tf_map[tf]  # то возвращаем временной интервал Финама
        raise NotImplementedError(f'Временной интервал {tf} не поддерживается')  # С остальными временнЫми интервалами не работаем

    @staticmethod
    def moex_timeframe_to_timeframe(moex_tf) -> str:
        """Перевод временнОго интервала Московской Биржи во временной интервал

        :param int moex_tf: Временной интервал Московской Биржи
        :return: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        """
        finam_tf_map = {1: 'M1', 10: 'M10', 60: 'M60', 24: 'D1', 7: 'W1', 31: 'MN1', 4: 'MN3'}  # Справочник временнЫх интервалов Московской Биржи
        if moex_tf in finam_tf_map:  # Если временной интервал Московской Биржи есть в справочнике
            return finam_tf_map[moex_tf]  # то возвращаем временной интервал
        raise NotImplementedError(f'Временной интервал Московской Биржи {moex_tf} не поддерживается')  # С остальными временнЫми интервалами Московской Биржи не работаем

    def get_long_token_from_keyring(self, service: str, username: str) -> str | None:
        """Получение токена из системного хранилища keyring по частям"""
        try:
            index = 0  # Номер части токена
            token_parts = []  # Части токена
            while True:  # Пока есть части токена
                token_part = keyring.get_password(service, f'{username}{index}')  # Получаем часть токена
                if token_part is None:  # Если части токена нет
                    break  # то выходим, дальше не продолжаем
                token_parts.append(token_part)  # Добавляем часть токена
                index += 1  # Переходим к следующей части токена
            if not token_parts:  # Если токен не найден
                self.logger.error(f'Токен не найден в системном хранилище. Вызовите fp_provider = FinamPy("<Токен>")')
                return None
            token = ''.join(token_parts)  # Собираем токен из частей
            self.logger.debug('Токен успешно загружен из системного хранилища')
            return token
        except keyring.errors.KeyringError as e:
            self.logger.fatal(f'Ошибка доступа к системному хранилищу: {e}')
        except Exception as e:
            self.logger.fatal(f'Ошибка при загрузке токена: {e}')

    def set_long_token_to_keyring(self, service: str, username: str, token: str, password_split_size: int = 500) -> None:
        """Установка токена в системное хранилище keyring по частям"""
        try:
            self.clear_long_token_from_keyring(service, username)  # Очищаем предыдущие части токена
            token_parts = [token[i:i + password_split_size] for i in range(0, len(token), password_split_size)]  # Разбиваем токен на части заданного размера
            for index, token_part in enumerate(token_parts):  # Пробегаемся по частям токена
                keyring.set_password(service, f'{username}{index}', token_part)  # Сохраняем часть токена
            self.logger.debug(f'Частей сохраненного токена в хранилище: {len(token_parts)}')
        except keyring.errors.KeyringError as e:
            self.logger.fatal(f'Ошибка сохранения в системное хранилище: {e}')
        except Exception as e:
            self.logger.fatal(f'Ошибка при сохранении токена: {e}')

    def clear_long_token_from_keyring(self, service: str, username: str) -> None:
        """Удаление всех частей токена из системного хранилища keyring"""
        try:
            index = 0  # Номер части токена
            while True:  # Пока есть части токена
                if keyring.get_password(service, f'{username}{index}') is None:  # Если части токена нет
                    break  # то выходим, дальше не продолжаем
                keyring.delete_password(service, f'{username}{index}')  # Удаляем часть токена
                index += 1  # Переходим к следующей части токена
        except keyring.errors.KeyringError as e:
            self.logger.fatal(f'Ошибка доступа к системному хранилищу: {e}')
