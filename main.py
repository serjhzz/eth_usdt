import asyncio

from source.classes import FuturesProcessor
from source.func import ethusdt_regression


async def main():

    eth_processor = FuturesProcessor('ethusdt')
    btc_processor = FuturesProcessor('btcusdt')

    eth_df = await eth_processor.read_data_to_dataframe()
    btc_df = await btc_processor.read_data_to_dataframe()

    eth_task = asyncio.create_task(eth_processor.run())
    btc_task = asyncio.create_task(btc_processor.run())
    regression_task = asyncio.create_task(ethusdt_regression(eth_df, btc_df))

    await asyncio.gather(eth_task, btc_task, regression_task)


if __name__ == '__main__':

    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)

    event_loop.run_until_complete(main())

