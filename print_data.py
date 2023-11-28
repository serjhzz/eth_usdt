from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from source.classes import FuturesTrade


def create_postgresql_connection(database_url):
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    return engine, Session()


# Функция для считывания и вывода данных из таблицы
def read_and_print_data(session):
    trades = session.query(FuturesTrade).all()
    for trade in trades:
        print(
            f"ID: {trade.id}, "
            f"Symbol: {trade.symbol}, "
            f"Price: {trade.price}, "
            f"Timestamp: {trade.timestamp}")


# Функция для закрытия подключения к базе данных
def close_database_connection(engine):
    if engine:
        engine.dispose()
        print("Connection closed.")


database_url = (
    'postgresql://postgres:12345@localhost:5432/postgres')
engine, session = create_postgresql_connection(database_url)

if session:
    read_and_print_data(session)

close_database_connection(engine)
