import os
import sys

import pandas as pd
import pytest

from source.func import (find_regression_coefficient,
                         adjust_ethusdt_price,
                         ethusdt_regression)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# TODO добавить тесты классов


@pytest.mark.asyncio
async def test_find_regression_coefficient():
    df = pd.DataFrame({
        'Price_btc': [1, 2, 3, 4, 5],
        'Price_eth': [2, 4, 6, 8, 10]
    })
    result = await find_regression_coefficient(df, 'Price_btc', 'Price_eth')
    assert result == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_adjust_ethusdt_price():
    df = pd.DataFrame({
        'Price_btc': [1, 2, 3, 4, 5],
        'Price_eth': [2, 4, 6, 8, 10]
    })
    result = await adjust_ethusdt_price(df)
    assert result.tolist() == pytest.approx([0.0, 0.0, 0.0, 0.0, 0.0])


@pytest.mark.asyncio
async def test_ethusdt_regression():
    """Тест для функции ethusdt_regression"""

    eth_df = pd.DataFrame({
        'Timestamp': pd.to_datetime(['2022-01-01', '2022-01-02']),
        'Price_eth': [100, 110]
    })

    btc_df = pd.DataFrame({
        'Timestamp': pd.to_datetime(['2022-01-01', '2022-01-02']),
        'Price_btc': [50, 55]
    })

    await ethusdt_regression(eth_df, btc_df)
