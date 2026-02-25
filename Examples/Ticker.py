import logging  # Выводим лог на консоль и в файл
from datetime import date, timedelta, datetime  # Дата и время

from MOEXPy import MOEXPy  # Работа с Algopack API Московской Биржи


def get_future_on_date(base, future_date=date.today()):  # Фьючерсный контракт на дату
    if future_date.day > 15 and future_date.month in (3, 6, 9, 12):  # Если нужно переходить на следующий фьючерс
        future_date += timedelta(days=30)  # то добавляем месяц к дате
    period = 'H' if future_date.month <= 3 else 'M' if future_date.month <= 6 else 'U' if future_date.month <= 9 else 'Z'  # Месяц экспирации: 3-H, 6-M, 9-U, 12-Z
    digit = future_date.year % 10  # Последняя цифра года
    return f'SPBFUT.{base}{period}{digit}'


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('MOEXPy.Ticker')  # Будем вести лог
    # mp_provider = MOEXPy('<Токен>')  # При первом подключении нужно передать токен
    mp_provider = MOEXPy()  # Подключаемся к Algopack API Московской Биржи

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Ticker.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=mp_provider.tz_msk).timetuple()  # В логе время указываем по МСК
    logging.getLogger('urllib3').setLevel(logging.CRITICAL + 1)  # Пропускаем события запросов

    datanames = ('TQBR.SBER', 'TQBR.HYDR', get_future_on_date("Si"), get_future_on_date("RI"), 'SPBFUT.CNYRUBF', 'SPBFUT.IMOEXF')  # Кортеж тикеров

    for dataname in datanames:  # Пробегаемся по всем тикерам
        moex_market, symbol = mp_provider.dataname_to_moex_market_symbol(dataname)  # Код рынка Московской Биржи и тикер из названия тикера
        si = mp_provider.get_ticker(moex_market, symbol)  # Получаем информацию о тикере (спецификация и рыночные данные)
        col_securities = {col: idx for idx, col in enumerate(si['securities']['columns'])}  # Колонки спецификации тикера с их порядковыми номерами
        data_securities = si['securities']['data'][0]  # Спецификация тикера
        col_marketdata = {col: idx for idx, col in enumerate(si['marketdata']['columns'])}  # Колонки рыночных данных тикера с их порядковыми номерами
        data_marketdata = si['marketdata']['data'][0]  # Рыночные данные тикера
        logger.info(f'Информация о тикере {data_securities[col_securities["BOARDID"]]}.{data_securities[col_securities["SECID"]]} ({data_securities[col_securities["SHORTNAME"]]}, {moex_market})')
        logger.debug(si)
        logger.info(f'- Лот: {data_securities[col_securities["LOTSIZE"]] if moex_market == 'shares' else data_securities[col_securities["LOTVOLUME"]]}')
        logger.info(f'- Шаг цены: {data_securities[col_securities["MINSTEP"]]}')
        logger.info(f'- Кол-во десятичных знаков: {data_securities[col_securities["DECIMALS"]]}')
        logger.info(f'- Последняя цена сделки: {data_marketdata[col_marketdata["LAST"]]}')
