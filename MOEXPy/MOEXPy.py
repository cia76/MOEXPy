import json
import logging  # Будем вести лог
from datetime import datetime, timedelta
from threading import Thread
from typing import Literal, Any
from uuid import uuid4  # Уникальный идентификатор подписки
from zoneinfo import ZoneInfo  # ВременнАя зона
from json import loads  # Получаем ответы в формае JSON

import keyring  # Безопасное хранение торгового токена
from requests import get  # Запросы через HTTP API
from websockets import Subprotocol  # Протокол STOMP
from websockets.sync.client import connect  # Подключение к серверу WebSockets в синхронном режиме
from stomp.utils import Frame, convert_frame, parse_frame  # Работа с сервером WebSockets по протоколу STOMP


class MOEXPy:
    """Работа с Algopack API Московской Биржи https://moexalgo.github.io/docs/api из Python"""
    iss_server = 'https://iss.moex.com/iss'  # Справочники МосБиржи (ISS)
    ws_server = 'wss://iss.moex.com/infocx/v3/websocket'  # Информационно-статистический сервер распространения биржевой информации в реальном времени (ISS+) на Московской Бирже
    api_server = 'https://apim.moex.com/iss'  # Алгопак (ISS)
    engine_map = dict(stocks='eq', futures='fo', currency='fx')  # Площадки Алгопака: Акции/фьючерсы/вылюта
    tz_msk = ZoneInfo('Europe/Moscow')  # Московская Биржа работает по московскому времени
    logger = logging.getLogger('MOEXPy')  # Будем вести лог

    def __init__(self, token=None, login=None, passcode=None):
        """Инициализация

        :param str token: Токен (ISS)
        :param str login: Логин (ISS+)
        :param str passcode: Пароль (ISS+)
        """
        if token is None:  # Если торговый токен не указан (запросы ISS)
            self.token = self.get_long_token_from_keyring('MOEXPy', 'token')  # то получаем его из защищенного хранилища по частям
        else:  # Если указан торговый токен
            self.token = token  # Торговый токен
            self.set_long_token_to_keyring('MOEXPy', 'token', self.token)  # Сохраняем его в защищенное хранилище
        self.headers = {'Accept': 'application/json', 'Authorization': f'Bearer {self.token}'}  # Заголовки для запросов
        if login is None:  # Если логин не указан (подписки ISS+)
            self.login = self.get_long_token_from_keyring('MOEXPy', 'login')  # то получаем его из защищенного хранилища по частям
            self.passcode = self.get_long_token_from_keyring('MOEXPy', 'passcode')  # также получаем пароль из защищенного хранилища по частям
        else:  # Если указан логин
            self.login = login  # Логин
            self.set_long_token_to_keyring('MOEXPy', 'login', self.login)  # Сохраняем его в защищенное хранилище
            self.passcode = passcode  # Пароль
            self.set_long_token_to_keyring('MOEXPy', 'passcode', self.passcode)  # Сохраняем его в защищенное хранилище
        self.ws_socket = None  # Подключения к серверу WebSockets пока нет

        # События сервера WebSocket
        self.on_connected = Event()  # Подключение
        self.on_error = Event()  # Ошибка
        self.on_receipt = Event()
        self.on_message = Event()  # Сообщение (данные подписки)
        self.on_reply = Event()
        self.on_closed = Event()  # Отключение

        # Справочники
        dict_data = get(f'{self.iss_server}/index.json').json()  # Получаем и разбираем данные в формате JSON
        engines_columns = dict_data['engines']['columns']  # Торговые площадки - Названия колонок
        engines_data = dict_data['engines']['data']  # Торговые площадки - Данные
        self.engines_dict = {row[engines_columns.index('id')]: {col: row[i] for i, col in enumerate(engines_columns) if col != 'id'} for row in engines_data}  # Справочник по ключу id
        markets_columns = dict_data['markets']['columns']  # Рынки - Названия колонок
        markets_data = dict_data['markets']['data']  # Рынки - Данные
        self.markets_dict = {row[markets_columns.index('id')]: {col: row[i] for i, col in enumerate(markets_columns) if col != 'id'} for row in markets_data}  # Справочник по ключу id
        boards_columns = dict_data['boards']['columns']  # Режимы торгов - Названия колонок
        boards_data = dict_data['boards']['data']  # Режимы торгов - Данные
        self.boards_dict = {row[boards_columns.index('boardid')]: {col: row[i] for i, col in enumerate(boards_columns) if col != 'boardid'} for row in boards_data}  # Справочник по ключу boardid

        self.subscriptions = {}  # Справочник подписок

    # Real-time market data - Акции - https://moexalgo.github.io/docs/api/real-time-market-data-акции
    # Real-time market data - Фьючерсы - https://moexalgo.github.io/docs/api/real-time-market-data-фьючерсы

    def get_all_tickers(self, board):
        """Торговая статистика за сегодня по всем инструментам режима торгов

        param str board: Режим торгов
        """
        market, _, engine = self.get_market_engine(board)  # По режиму торгов получаем рынок и торговую площадку
        if market is None:  # Если рынок не пришел
            return None  # то выходим, дальше не продолжаем
        url = f'{self.iss_server}/engines/{engine}/markets/{market}/boards/{board}/securities.json'  # URL запроса
        start = 0  # Начинаем получать данные с первой записи интервала
        all_data = None  # Накопленные данные
        while True:  # Пока не обработаем все периоды запроса
            params = {
                'start': start   # Номер первой записи с начала интервала
            }
            content = self.check_result(get(url, params=params, headers=self.headers))  # Отправляем запрос, получаем ответ
            if content is None:  # Если ответ не пришел
                return None  # то выходим, дальше не продолжаем
            data = content['securities']['data']  # Пришедшие данные
            if len(data) == 0:  # Если данных нет (достигнут конец выборки)
                break  # то выходим
            if all_data is None:  # Если это первые пришедшие данные
                all_data = content  # то сохраняем их полностью
            else:  # Если данные уже есть
                all_data['securities']['data'].extend(data)  # то добавляем к уже имеющимся
            start += len(data)  # Номер первой записи перемещаем за последнюю полученную
        return all_data

    def get_ticker(self, board, ticker):
        """Торговая статистика за сегодня по инструменту

        param str board: Режим торгов
        param str ticker: Тикер
        """
        market, _, engine = self.get_market_engine(board)  # По режиму торгов получаем рынок и торговую площадку
        if market is None:  # Если рынок не пришел
            return None  # то выходим, дальше не продолжаем
        url = f'{self.iss_server}/engines/{engine}/markets/{market}/boards/{board}/securities/{ticker}.json'  # URL запроса
        return self.check_result(get(url, headers=self.headers))

    def get_candles(self, board, ticker, dt_from, dt_till, interval):
        """Свечи по инструменту

        param str board: Режим торгов
        param str ticker: Тикер
        param datetime dt_from: Дата и время начала запроса
        param datetime dt_till: Дата и время окончания запроса
        param int interval: Временной интервал. 1 - 'M1', 10 - 'M10', 60 - 'M60', 24 - 'D1', 7 - 'W1', 31 - 'MN1', 4 - 'MN3'
        """
        market, _, engine = self.get_market_engine(board)  # По режиму торгов получаем рынок и торговую площадку
        if market is None:  # Если рынок не пришел
            return None  # то выходим, дальше не продолжаем
        url = f'{self.iss_server}/engines/{engine}/markets/{market}/boards/{board}/securities/{ticker}/candles.json'  # URL запроса
        all_data = None  # Накопленные данные
        while dt_from < dt_till:  # Пока не обработаем все периоды запроса
            params = {
                'from': dt_from,  # Дата и время начала запроса
                'till': dt_till,  # Дата и время окончания запроса
                'interval': interval  # Временной интервал
            }
            content = self.check_result(get(url, params=params, headers=self.headers))  # Отправляем запрос, получаем ответ
            if content is None:  # Если ответ не пришел
                return None  # то выходим, дальше не продолжаем
            data = content['candles']['data']  # Пришедшие данные
            if len(data) == 0:  # Если данных нет (достигнут конец выборки)
                break  # то выходим
            if all_data is None:  # Если это первые пришедшие данные
                all_data = content  # то сохраняем их полностью
            else:  # Если данные уже есть
                all_data['candles']['data'].extend(data)  # то добавляем к уже имеющимся
            dt_from = datetime.strptime(data[-1][-2], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=1)  # Дата и время начала следующего периода
        return all_data

    def get_orderbook(self, board, ticker):
        """Стакан котировок по инструменту

        param str board: Режим торгов
        param str ticker: Тикер
        """
        market, _, engine = self.get_market_engine(board)  # По режиму торгов получаем рынок и торговую площадку
        if market is None:  # Если рынок не пришел
            return None  # то выходим, дальше не продолжаем
        url = f'{self.iss_server}/engines/{engine}/markets/{market}/boards/{board}/securities/{ticker}/orderbook.json'  # URL запроса
        return self.check_result(get(url, headers=self.headers))

    def get_trades(self, board, ticker, tradeno=None):
        """Все сделки по инструменту

        param str board: Режим торгов
        param str ticker: Тикер
        param int tradeno: Получить сделки, которые идут начиная с указанного номера
        """
        market, _, engine = self.get_market_engine(board)  # По режиму торгов получаем рынок и торговую площадку
        if market is None:  # Если рынок не пришел
            return None
        url = f'{self.iss_server}/engines/{engine}/markets/{market}/boards/{board}/securities/{ticker}/trades.json'  # URL запроса
        params = {} if tradeno is None else dict(tradeno=tradeno)  # Если указан номер сделки, то будем получать сделки начиная с указанного номера
        return self.check_result(get(url, params=params, headers=self.headers))

    # Super Candles - Акции - https://moexalgo.github.io/docs/api/super-candles-акции
    # Super Candles - Фьючерсы - https://moexalgo.github.io/docs/api/super-candles-фьючерсы
    # Super Candles - Валюта - https://moexalgo.github.io/docs/api/super-candles-валюта

    def get_all_stats(self, stats: Literal['trade', 'ob', 'order'], engine: Literal['stock', 'futures', 'currency'], date, latest=False, limit=1000):
        """Метрики рассчитанные на основе потока сделок/котировок/заявок по всем инструментам

        :param Literal['trade', 'ob', 'order'] stats: Поток сделок/котировок/заявок
        :param Literal['stock', 'futures', 'currency'] engine: Торговая площадка акций/фьючерсов/валют
        :param date date: Дата торгов
        :param bool latest: Последняя пятиминутка
        :param int limit: Кол-во записей (не более 1000)
        """
        url = f'{self.api_server}/datashop/algopack/{self.engine_map[engine]}/{stats}stats.json'  # URL запроса
        params = dict(date=date, latest=latest, limit=limit)
        return self.check_result(get(url, params=params, headers=self.headers))

    def get_stats(self, stats: Literal['trade', 'ob', 'order'], engine: Literal['stock', 'futures', 'currency'], ticker, dt_from, dt_till, latest=False):
        """Метрики рассчитанные на основе потока сделок/котировок/заявок по инструменту

        :param Literal['trade', 'ob', 'order'] stats: Поток сделок/котировок/заявок
        :param Literal['stock', 'futures', 'currency'] engine: Торговая площадка акций/фьючерсов/валют
        :param str ticker: Тикер
        :param date dt_from: Дата и время начала запроса
        :param date dt_till: Дата и время окончания запроса
        :param bool latest: Последняя пятиминутка
        """
        url = f'{self.api_server}/datashop/algopack/{self.engine_map[engine]}/{stats}stats/{ticker}.json'  # URL запроса
        all_data = None  # Накопленные данные
        while dt_from < dt_till:  # Пока не обработаем все периоды запроса
            params = {
                'from': dt_from,  # Дата и время начала запроса
                'till': dt_till,  # Дата и время окончания запроса
                'latest': latest  # Последняя пятиминутка
            }
            content = self.check_result(get(url, params=params, headers=self.headers))  # Отправляем запрос, получаем ответ
            if content is None:  # Если ответ не пришел
                return None  # то выходим, дальше не продолжаем
            data = content['candles']['data']  # Пришедшие данные
            if len(data) == 0:  # Если данных нет (достигнут конец выборки)
                break  # то выходим
            if all_data is None:  # Если это первые пришедшие данные
                all_data = content  # то сохраняем их полностью
            else:  # Если данные уже есть
                all_data['candles']['data'].extend(data)  # то добавляем к уже имеющимся
            dt_from = datetime.strptime(data[-1][-2], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=1)  # Дата и время начала следующего периода
        return all_data

    # Futures Open Interest (FUTOI) - https://moexalgo.github.io/docs/api/futures-open-interest-futoi

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
            content = self.check_result(get(url, params=params, headers=self.headers))
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
            data = [row for row in content['futoi']['data'] if dt_from <= datetime.strptime(f'{row[2]} {row[3]}', '%Y-%m-%d %H:%M:%S') <= dt_till]  # Пришедшие данные с фильтром по дате/времени запроса
            if all_data is None:  # Если это первые пришедшие данные
                content['futoi']['data'] = data
                all_data = content  # то сохраняем их полностью
            elif len(data) > 0:  # Если данные уже есть и пришли не пустые
                all_data['futoi']['data'].extend(data)  # то добавляем к уже имеющимся
        return all_data

    # Market Concentration (HI2) - https://moexalgo.github.io/docs/api/market-concentration-hi-2

    def get_all_hi2(self, engine: Literal['stock', 'futures', 'currency'], date):
        """Индекс рыночной концентрации (Херфиндаля-Хиршмана) по всем инструментам

        :param Literal['stock', 'futures', 'currency'] engine: Торговая площадка акций/фьючерсов/валют
        :param date date: Дата торгов
        """
        url = f'{self.api_server}/datashop/algopack/{self.engine_map[engine]}/hi2.json'  # URL запроса
        params = dict(date=date)
        return self.check_result(get(url, params=params, headers=self.headers))

    def get_hi2(self, engine: Literal['stock', 'futures', 'currency'], ticker, date):
        """Индекс рыночной концентрации (Херфиндаля-Хиршмана) по инструменту

        :param Literal['stock', 'futures', 'currency'] engine: Торговая площадка акций/фьючерсов/валют
        :param str ticker: Тикер
        :param date date: Дата торгов
        """
        url = f'{self.api_server}/datashop/algopack/{self.engine_map[engine]}/hi2/{ticker}.json'  # URL запроса
        params = dict(date=date)
        return self.check_result(get(url, params=params, headers=self.headers))

    # Mega Alerts - https://moexalgo.github.io/docs/api/mega-alerts

    def get_all_alerts(self, engine: Literal['stock', 'futures'], date):
        """Торговые аномалии по всем инструментам

        :param Literal['stock', 'futures'] engine: Торговая площадка акций/фьючерсов
        :param date date: Дата торгов
        """
        url = f'{self.api_server}/datashop/algopack/{self.engine_map[engine]}/alerts.json'  # URL запроса
        params = dict(date=date)
        return self.check_result(get(url, params=params, headers=self.headers))

    def get_alerts(self, engine: Literal['stock', 'futures'], ticker, date):
        """Торговые аномалии по всем инструменту

        :param Literal['stock', 'futures'] engine: Торговая площадка акций/фьючерсов
        :param str ticker: Тикер
        :param date date: Дата торгов
        """
        url = f'{self.api_server}/datashop/algopack/{self.engine_map[engine]}/alerts/{ticker}.json'  # URL запроса
        params = dict(date=date)
        return self.check_result(get(url, params=params, headers=self.headers))

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

    # Запросы WebSocket

    def send_websocket(self, cmd: Literal['CONNECT', 'DISCONNECT', 'SUBSCRIBE', 'UNSUBSCRIBE', 'REQUEST', 'SEND'], params):
        """Отправка запроса через командный WebSocket

        :param Literal['CONNECT', 'DISCONNECT', 'SUBSCRIBE', 'UNSUBSCRIBE', 'REQUEST', 'SEND'] cmd: Клиентские команды
        :param dict params: Параметры запроса в виде словаря
        """
        if self.ws_socket is None:  # Если не было подключения к серверу WebSocket
            self.ws_socket = connect(self.ws_server, subprotocols=[Subprotocol('STOMP')])  # то пробуем к нему подключиться по протоколу STOMP
            connect_request_frame = Frame(cmd='CONNECT', headers=dict(domain='passport', login=self.login, passcode=self.passcode))  # Запрос на авторизацию
            self.ws_socket.send(b''.join(convert_frame(connect_request_frame)))  # Отправляем
            connect_response_frame = parse_frame(self.ws_socket.recv())  # Ожидаем и получаем ответ
            if connect_response_frame.cmd != 'CONNECTED':  # Если не подключились
                self.logger.error(f'Ошибка подключения к WebSocket: {connect_response_frame.cmd}')
                self.ws_socket = None  # Подключения к серверу WebSocket нет
                return  # Выходим, дальше не продолжаем
            else:  # Если подключились
                Thread(target=self.websocket_thread, name='WebSocketThread', daemon=True).start()  # Создаем и запускаем поток управления подписками. Завершится с окончанием основного потока
        if cmd == 'SUBSCRIBE':  # Если подписываемся
            subscription_id = str(uuid4())  # то генерируем уникальный номер подписки
            self.subscriptions[subscription_id] = params  # Заносим в список подписок
            params['id'] = subscription_id  # Также передаем в параметры
        elif cmd == 'UNSUBSCRIBE':
            del self.subscriptions[params['id']]  # Удаляем подписку из списка
        request_frame = Frame(cmd=cmd, headers=params)  # Клиентская команда с параметрами
        self.logger.debug(f'Отправлены данные WebSocket {request_frame.cmd} - {request_frame.body} - {request_frame.headers}')
        self.ws_socket.send(b''.join(convert_frame(request_frame)))  # Отправляем

    # Подписки WebSocket

    def websocket_thread(self):
        """Поток управления подписками"""
        self.logger.debug(f'WebSocket Thread: Запущен')
        while True:
            response_frame = parse_frame(self.ws_socket.recv())  # Получаем ответ или таймаут
            cmd = response_frame.cmd  # Полученная команда
            headers = response_frame.headers  # Заголовки команды
            body = json.loads(response_frame.body.decode('utf8').strip('\0'))  # Расшифровываем пришедшее сообщение
            self.logger.debug(f'Пришли данные WebSocket {cmd} - {headers} - {body}')
            if cmd == 'CONNECTED':  # Подключение
                self.on_connected.trigger(headers, body)
            elif cmd == 'ERROR':  # Ошибка
                self.on_error.trigger(headers, body)
            elif cmd == 'RECEIPT':
                self.on_receipt.trigger(headers, body)
            elif cmd == 'MESSAGE':  # Сообщение (данные подписки)
                subscription_id = headers.get('subscription')  # Пытаемся получить уникальный номер подписки
                if subscription_id is not None:  # Если пришло сообщение по подписке
                    headers.update(self.subscriptions[subscription_id])  # то в заголовок добавляем данные подписки
                self.on_message.trigger(headers, body)
            elif cmd == 'REPLY':
                self.on_reply.trigger(headers, body)
            elif cmd == 'CLOSED':  # Отключение
                self.on_closed.trigger(headers, body)

    # Функции конвертации

    @staticmethod
    def dataname_to_board_symbol(dataname) -> tuple[str | None, str]:
        """Код режима торгов и тикер из названия тикера

        :param str dataname: Название тикера
        :return: Код режима торгов и тикер
        """
        symbol_parts = dataname.split('.')  # По разделителю пытаемся разбить тикер на части
        if len(symbol_parts) >= 2:  # Если тикер задан в формате <Код режима торгов>.<Код тикера>
            board = symbol_parts[0]  # Код режима торгов
            symbol = '.'.join(symbol_parts[1:])  # Код тикера
        else:  # Если тикер задан без кода режима торгов
            return None, dataname  # то код рынка неизвестен
        return board, symbol

    @staticmethod
    def board_symbol_to_dataname(board, symbol) -> str:
        """Название тикера из кода режима торгов и тикера

        :param str board: Код режима торгов
        :param str symbol: Тикер
        :return: Название тикера
        """
        return f'{board}.{symbol}'

    def get_market_engine(self, board: str) -> tuple[str | None, str | None, str | None]:
        """Рынок и торговая площадка из режима торгов

        :param str board: Режим торгов
        :return: Рынок, торговая площадка
        """
        board_row = self.boards_dict.get(board)  # Строка режима торгов
        if board_row is None:  # Если не найдена
            self.logger.error(f'Неизвестный рынок и торговая площадка для режима торгов {board}')
            return None, None, None  # то и рынки с торговыми площадками найти не удастся. Выходим, дальше не продолжаем
        market_row = self.markets_dict.get(board_row['market_id'])  # Строка рынка (должна быть)
        return market_row['market_name'], market_row['marketplace'], market_row['trade_engine_name']  # Например: shares, MXSE, stock

    @staticmethod
    def timeframe_to_moex_timeframe(tf: str) -> int:
        """Перевод временнОго интервала во временной интервал Московской Биржи (REST)

        :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        :return: Временной интервал Московской Биржи
        """
        tf_map = {'M1': 1, 'M10': 10, 'M60': 60, 'D1': 24, 'W1': 7, 'MN1': 31, 'MN3': 4}  # Справочник временнЫх интервалов
        if tf in tf_map:  # Если временной интервал есть в справочнике
            return tf_map[tf]  # то возвращаем временной интервал Финама
        raise NotImplementedError(f'Временной интервал {tf} не поддерживается')  # С остальными временнЫми интервалами не работаем

    @staticmethod
    def timeframe_to_moex_ws_timeframe(tf: str) -> str:
        """Перевод временнОго интервала во временной интервал Московской Биржи (WebSockets)

        :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        :return: Временной интервал Московской Биржи (WebSockets)
        """
        tf_map = {'M1': 'M1', 'M10': 'M10', 'M60': 'H1', 'D1': 'D1', 'W1': 'W1', 'MN1': 'm1', 'MN3': 'Q1'}  # Справочник временнЫх интервалов
        if tf in tf_map:  # Если временной интервал есть в справочнике
            return tf_map[tf]  # то возвращаем временной интервал Финама
        raise NotImplementedError(f'Временной интервал {tf} не поддерживается')  # С остальными временнЫми интервалами не работаем

    @staticmethod
    def moex_timeframe_to_timeframe(moex_tf) -> str:
        """Перевод временнОго интервала Московской Биржи (REST) во временной интервал

        :param int moex_tf: Временной интервал Московской Биржи (REST)
        :return: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        """
        tf_map = {1: 'M1', 10: 'M10', 60: 'M60', 24: 'D1', 7: 'W1', 31: 'MN1', 4: 'MN3'}  # Справочник временнЫх интервалов Московской Биржи
        if moex_tf in tf_map:  # Если временной интервал Московской Биржи есть в справочнике
            return tf_map[moex_tf]  # то возвращаем временной интервал
        raise NotImplementedError(f'Временной интервал Московской Биржи {moex_tf} не поддерживается')  # С остальными временнЫми интервалами Московской Биржи не работаем

    @staticmethod
    def moex_ws_timeframe_to_timeframe(moex_tf) -> str:
        """Перевод временнОго интервала Московской Биржи (WebSockets) во временной интервал

        :param str moex_tf: Временной интервал Московской Биржи (WebSockets)
        :return: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        """
        tf_map = {'M1': 'M1', 'M10': 'M10', 'H1': 'M60', 'D1': 'D1', 'W1': 'W1', 'm1': 'MN1', 'Q1': 'MN3'}  # Справочник временнЫх интервалов Московской Биржи
        if moex_tf in tf_map:  # Если временной интервал Московской Биржи есть в справочнике
            return tf_map[moex_tf]  # то возвращаем временной интервал
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
                self.logger.error(f'Токен не найден в системном хранилище. Вызовите mp_provider = MOEXPy("<Токен>")')
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


class Event:
    """Событие с подпиской / отменой подписки"""
    def __init__(self):
        self._callbacks: set[Any] = set()  # Избегаем дубликатов функций при помощи set

    def subscribe(self, callback) -> None:
        """Подписаться на событие"""
        self._callbacks.add(callback)  # Добавляем функцию в список

    def unsubscribe(self, callback) -> None:
        """Отписаться от события"""
        self._callbacks.discard(callback)  # Удаляем функцию из списка. Если функции нет в списке, то не будет ошибки

    def trigger(self, *args, **kwargs) -> None:
        """Вызвать событие"""
        for callback in list(self._callbacks):  # Пробегаемся по копии списка, чтобы избежать исключения при удалении
            callback(*args, **kwargs)  # Вызываем функцию
