import logging  # Выводим лог на консоль и в файл
from datetime import datetime, timedelta  # Дата и время

from MOEXPy.MOEXPy.MOEXPy import MOEXPy  # Работа с Algopack API Московской Биржи


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('MOEXPy.Futoi')  # Будем вести лог
    mp_provider = MOEXPy()  # Подключаемся к Algopack API Московской Биржи

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Bars.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=mp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    ticker = 'IMOEXF'  # Тикер

    date_end_msk = datetime.now(mp_provider.tz_msk).replace(tzinfo=None).date()  # Дата окончания запроса
    # date_end_msk = date_end_msk - timedelta(days=15)  # Без платной подписки получаем данные с задержкой 14 дней
    date_begin_msk = date_end_msk - timedelta(days=3)  # Дата начала запроса

    futoi = mp_provider.get_futoi(ticker, date_begin_msk, date_end_msk)  # Получаем всю историю тикера
    col_futoi = {col: idx for idx, col in enumerate(futoi['futoi']['columns'])}  # Колонки истории тикера с их порядковыми номерами
    data_futoi = futoi['futoi']['data']  # Данные истории тикера
    if len(data_futoi) == 0:  # Если бары не получены
        logger.info('FUTOI не получен')
    else:  # Бары получены
        logger.info(f'Получено значений FUTOI : {len(data_futoi)}')
        for bar in data_futoi:  # Пробегаемся по всем барам
            logger.info(bar)
