import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время

from MOEXPy.MOEXPy.MOEXPy import MOEXPy  # Работа с Algopack API Московской Биржи


def on_new_bar(headers, body):  # Обработчик события прихода нового бара
    if headers['destination'] != f'{marketplace}.candles':  # Если пришла подписка не на новый бар
        return  # то выходим, дальше не продолжаем
    if headers['selector']['ticker'] != f'{marketplace}.{dataname}':  # Если пришла подписка на другой тикер
        return  # то выходим, дальше не продолжаем
    if headers['selector']['interval'] != tf:  # Если пришла подписка на другой интервал
        return  # то выходим, дальше не продолжаем
    global last_bar, dt_last_bar  # Последний полученный бар и его дата/время
    for row in body['data']:  # Пробегаемся по всем строкам
        row_dict = dict(zip(body['columns'], row))  # Переводим строку бара в словарь
        dt_bar = datetime.fromisoformat(row_dict['FROM'])  # Дата/время полученного бара
        if dt_last_bar is not None and dt_last_bar < dt_bar:  # Если время бара стало больше (предыдущий бар закрыт, новый бар открыт)
            logger.info(f'{dt_last_bar:%d.%m.%Y %H:%M:%S} '
                        f'O:{round(float(last_bar['OPEN'][0]), last_bar['OPEN'][1])} '
                        f'H:{round(float(last_bar['HIGH'][0]), last_bar['HIGH'][1])} '
                        f'L:{round(float(last_bar['LOW'][0]), last_bar['LOW'][1])} '
                        f'C:{round(float(last_bar['CLOSE'][0]), last_bar['CLOSE'][1])} '
                        f'V:{int(float(last_bar['VOLUME']))}')
        last_bar = row_dict  # Запоминаем бар
        dt_last_bar = dt_bar  # Запоминаем дату и время бара


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('MOEXPy.Connect')  # Будем вести лог
    # mp_provider = MOEXPy('<Токен>', '<Логин>', '<Пароль>')  # При первом подключении нужно передать токен
    mp_provider = MOEXPy()  # Подключаемся к Algopack API Московской Биржи

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Connect.log', encoding='utf-8'),
                                  logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(
        tz=mp_provider.tz_msk).timetuple()  # В логе время указываем по МСК
    logging.getLogger('urllib3').setLevel(logging.CRITICAL + 1)  # Не пропускать в лог
    logging.getLogger('websockets').setLevel(logging.CRITICAL + 1)  # события в этих библиотеках

    dataname = 'TQBR.SBER'  # Тикер
    tf = 'M1'  # Временной интервал МосБиржи. M1: 1 минута, M10: 10 минут, H1: 1 час, D1: 1 день, W1: 1 неделя, m1: 1 месяц, m3: 1 квартал, Q1: 1 квартал

    # Проверяем работу запрос/ответ
    logger.info(f'Данные тикера {dataname}')  # МосБиржа не передает время на своих серверах. Поэтому, запросим данные тикера
    board, symbol = mp_provider.dataname_to_board_symbol(dataname)  # Код режима торгов и тикер из названия тикера
    si = mp_provider.get_ticker(board, symbol)  # Получаем информацию о тикере (спецификация и рыночные данные)
    logger.info(si)

    # Проверяем работу подписок
    logger.info(f'Подписка на {tf} бары тикера {dataname}')
    last_bar = None  # Последнего полученного бара пока нет
    dt_last_bar = None  # И даты/времени у него пока нет
    mp_provider.on_message.subscribe(on_new_bar)  # Обработчик события прихода нового бара
    _, marketplace, _ = mp_provider.get_market_engine(board)  # Рынок и торговая площадка
    mp_provider.send_websocket(
        cmd='SUBSCRIBE',  # Подписываемся
        params={
            'destination': f'{marketplace}.candles',  # на бары
            'selector': dict(ticker=f'{marketplace}.{dataname}', interval=tf),
            # тикера по временнОму интервалу МосБиржи
        })

    # Выход
    input('Enter - выход\n')
