import asyncio
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import websockets
from sqlalchemy import (create_engine,
                        Column,
                        Integer,
                        String,
                        Numeric,
                        DateTime)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from source.func import check_eth_price, print_suc_del

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

BASE_DIR = Path(__file__).resolve().parent.parent
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

price_history = []

Base = declarative_base()

# TODO раздробить класс processor:
#   на управление БД,
#   работа с данными,
#   работа логики.



class FuturesTrade(Base):
    """
    Модель для таблицы futures_trades в базе данных.
    Содержит информацию о сделках по фьючерсам, такую как символ,
    цена и временная метка.
    """
    __tablename__ = os.getenv('TABLE_NAME')

    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    price = Column(Numeric)
    timestamp = Column(DateTime)


class FuturesProcessor:
    """
    Класс для обработки данных о торговле фьючерсами.
    Подключается к Binance WebSocket, обрабатывает сделки и
    сохраняет их в базу данных.
    Также вызывает функцию для проверки изменения цены ETH.
    """

    def __init__(self, symbol):
        """
        Инициализация объекта FuturesProcessor.

        Parameters:
            symbol (str): Символ для отслеживания торгов.
        """
        self.symbol = symbol
        self.engine = create_engine(os.getenv('ENGINE'))

        self.session = self.create_session(self.engine)
        self.create_table(self.symbol)

    def create_table(self, table_name):
        """
        Создает таблицу в базе данных, если она не существует.

        Parameters:
            table_name (str): Имя таблицы.
        """
        if not self.engine.dialect.has_table(self.engine.connect(),
                                             table_name):
            Base.metadata.create_all(self.engine)

    @staticmethod
    def create_session(engine):
        """
        Создает сеанс для взаимодействия с базой данных.

        Parameters:
            engine: Объект мотора SQLAlchemy.

        Returns:
            sqlalchemy.orm.Session: Объект сеанса.
        """
        Session = sessionmaker(bind=engine)
        return Session()

    async def handle_trade(self, data):
        """
        Обрабатывает данные о сделке, сохраняет их в базу данных
        и вызывает функцию для проверки изменения цены ETH.

        Parameters:
            data (str): JSON-строка данных о сделке.
        """
        try:
            if data:
                data = json.loads(data)
                if self.symbol == "ethusdt":
                    print(
                        f"Торговая пара ({self.symbol}): "
                        f"{data['s']}, Цена: {data['p']} {data['s']}")
                trade_symbol = data['s']

                trade_price = float(data['p'])

                await check_eth_price(trade_price)

                session = self.create_session(self.engine)

                trade_entry = FuturesTrade(
                    symbol=trade_symbol,
                    price=trade_price,
                    timestamp=datetime.now()
                )
                session.add(trade_entry)
                session.commit()
                session.close()

                await asyncio.sleep(1)
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            await asyncio.sleep(10)

    async def read_data_to_dataframe(self):
        """
        Читает данные из базы данных и возвращает их в виде DataFrame.

        Returns:
            pd.DataFrame: DataFrame с данными о сделках.
        """
        trades = self.session.query(FuturesTrade).filter_by(
            symbol=self.symbol.upper()).all()
        data = {'ID': [], 'Symbol': [], 'Price': [], 'Timestamp': []}

        for trade in trades:
            data['ID'].append(trade.id)
            data['Symbol'].append(trade.symbol)
            data['Price'].append(trade.price)
            data['Timestamp'].append(trade.timestamp)

        df = pd.DataFrame(data)
        return df

    async def delete_old_trades(self):
        """
        Удаляет записи из базы данных, старше 60 минут.
        """
        sixty_minutes_ago = datetime.now() - timedelta(minutes=60)

        try:
            # Выбираем записи, старше 60 минут

            old_trades = self.session.query(FuturesTrade).filter(
                FuturesTrade.timestamp < sixty_minutes_ago
            ).all()
            if old_trades:
                await print_suc_del()
                # Удаляем найденные записи
                for trade in old_trades:
                    self.session.delete(trade)

                # Коммитим изменения в базе данных
                self.session.commit()
            else:
                return
        except Exception as e:
            print(f"Произошла ошибка при удалении старых записей: {e}")
        finally:
            # Закрываем сессию
            self.session.close()

    async def run(self):
        """
        Запускает подключение к WebSocket и цикл обработки сделок.
        """
        async with websockets.connect(
                f"wss://stream.binance.com:9443/ws/{self.symbol}@trade",
                ping_timeout=20,
                # Таймаут ожидания ответа на пинг (в секундах)
                close_timeout=None,
                # Таймаут на закрытие соединения (в секундах),
                # None - неограниченный
                max_queue=2 ** 5,  # Максимальный размер очереди сообщений
        ) as ws:
            while True:
                try:
                    response = await ws.recv()

                    handle_trade_task = asyncio.create_task(
                        self.handle_trade(response))
                    delete_old_trades_task = asyncio.create_task(
                        self.delete_old_trades())
                    await asyncio.gather(handle_trade_task,
                                         delete_old_trades_task)

                    # await self.read_data_to_dataframe()
                except:
                    if not KeyboardInterrupt:
                        continue
