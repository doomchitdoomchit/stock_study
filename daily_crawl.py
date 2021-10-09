from crawl_stock_data import daily_crawling as dc
import pandas as pd
import _pickle
import gzip
import os
from datetime import datetime

if __name__ == "__main__":
    base_path = ''
    if dc.check_today_open():
        kospi_ohlcv, kosdaq_ohlcv = dc.daily_ohlcv()
        dc.add_ohlcv(base_path, kospi_ohlcv, kosdaq_ohlcv)
        ohlcv_file = pd.concat([kospi_ohlcv, kosdaq_ohlcv], axis=0, ignore_index=True)
        with gzip.open(os.path.abspath(os.path.join(base_path, '..', 'recent_ohlcv',
                                                    f'{datetime.now().strftime("%Y%m%d")}.pkl.gz')), 'wb') as f:
            _pickle.dump(ohlcv_file, f)

        trade_foreign, trade_person, trade_agency = dc.daily_trading()
        dc.add_trading(base_path, trade_foreign, trade_person, trade_agency)
        trade_file = pd.concat([trade_foreign, trade_person, trade_agency], axis=0, ignore_index=True)
        with gzip.open(os.path.abspath(os.path.join(base_path, '..', 'recent_trade',
                                                    f'{datetime.now().strftime("%Y%m%d")}.pkl.gz')), 'wb') as f:
            _pickle.dump(trade_file, f)