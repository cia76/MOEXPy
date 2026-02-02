import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время

from MOEXPy import MOEXPy  # Работа с Algopack API Московской Биржи


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('MOEXPy.Bars')  # Будем вести лог
    mp_provider = MOEXPy()  # Подключаемся к Algopack API Московской Биржи

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Bars.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=mp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    dataname = 'TQBR.SBER'  # Тикер
    tf = 'D1'  # Временной интервал

    moex_market, symbol = mp_provider.dataname_to_moex_market_symbol(dataname)  # Код рынка Московской Биржи и тикер из названия тикера
    moex_tf = mp_provider.timeframe_to_moex_timeframe(tf)  # Временной интервал Московской Биржи
    bars = mp_provider.get_candles(moex_market, symbol, datetime(1990, 1, 1), datetime.now(), moex_tf)  # Получаем всю историю тикера
    col_bars = {col: idx for idx, col in enumerate(bars['candles']['columns'])}  # Колонки истории тикера с их порядковыми номерами
    data_bars = bars['candles']['data']  # Данные истории тикера
    if len(data_bars) == 0:  # Если бары не получены
        logger.info('Бары не получены')
    else:  # Бары получены
        logger.info(f'Получено бар  : {len(data_bars)}')
        for bar in data_bars:  # Пробегаемся по всем барам
            logger.info(f'{bar[col_bars["begin"]]} O:{bar[col_bars["open"]]} H:{bar[col_bars["high"]]} L:{bar[col_bars["low"]]} C:{bar[col_bars["close"]]} V:{int(bar[col_bars["volume"]])}')
