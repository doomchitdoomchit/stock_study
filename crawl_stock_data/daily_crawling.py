from datetime import datetime
from pykrx import stock
import pandas as pd
import os
import _pickle
import gzip
from time import sleep
import numpy as np


def daily_ohlcv() -> (pd.DataFrame, pd.DataFrame):
    """
    오늘 주식내용 크롤링
    :return:
    """
    today_date = datetime.now().strftime('%Y%m%d')
    kospi_daily = stock.get_market_ohlcv_by_ticker(today_date, market="KOSPI")
    kosdaq_daily = stock.get_market_ohlcv_by_ticker(today_date, market="KOSDAQ")
    return kospi_daily, kosdaq_daily


def check_today_open() -> bool:
    """
    오늘 장이 열리는지 확인
    :return: bool
    """
    import exchange_calendars as excal
    XKRX = excal.get_calendar("XKRX")
    return XKRX.is_session(datetime.now().strftime('%Y-%m-%d'))


def add_ohlcv(base_path: str, kospi_daily: pd.DataFrame, kosdaq_daily: pd.DataFrame) -> None:
    """
    데이터 추가
    :param base_path: 기본 파일 위치
    :param kospi_daily: 코스피 데일리
    :param kosdaq_daily: 코스닥 데일리 데이터
    :return: 없음. 그냥 추가
    """
    for _daily in [kospi_daily, kosdaq_daily]:
        for _ in _daily.reset_index().values:
            ticker = _[0]
            _val = _[1:]
            file_path = os.path.join(base_path, f'{ticker}/ohlcv.pkl.gz')
            if os.path.exists(file_path):
                with gzip.open(file_path, 'rb') as f:
                    temp_data = _pickle.load(f)
                temp_data.loc[pd.DatetimeIndex([datetime.now().date()])[0]] = list(_val[:-2]) + [_val[-1]]
                temp_data = temp_data.astype({
                                        "시가": np.int32, "고가": np.int32, "저가": np.int32, "종가": np.int32,
                                        "거래량": np.int32, "등락률": np.float32})
                with gzip.open(file_path, 'wb') as f:
                    _pickle.dump(temp_data, f)
            else:
                os.makedirs(os.path.join(base_path, f'{ticker}'))
                temp_data = stock.get_market_ohlcv_by_date(fromdate="20170101",
                                                           todate=datetime.now().strftime('%Y%m%d'),
                                                           ticker=ticker,
                                                           adjusted=False)
                with gzip.open(file_path, ' wb') as f:
                    _pickle.dump(temp_data.drop('거래대금', axis=1, inplace=True), f)
                    sleep(2)


def daily_trading() -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    일일 거래금과 거래량 받기
    :return: 외국인, 개인, 기관
    """
    today_date = datetime.now().strftime('%Y%m%d')
    trading_foreigner = stock.get_market_net_purchases_of_equities_by_ticker(today_date, today_date, 'ALL', '외국인')
    trading_foreigner.drop(['종목명', '순매수거래량', '순매수거래대금'], axis=1, inplace=True)
    trading_personal = stock.get_market_net_purchases_of_equities_by_ticker(today_date, today_date, 'ALL', '개인')
    trading_personal.drop(['종목명', '순매수거래량', '순매수거래대금'], axis=1, inplace=True)
    trading_agencysum = stock.get_market_net_purchases_of_equities_by_ticker(today_date, today_date, 'ALL', '기관합계')
    trading_agencysum.drop(['종목명', '순매수거래량', '순매수거래대금'], axis=1, inplace=True)
    return trading_foreigner, trading_personal, trading_agencysum


def init_trading_value(ticker: str) -> pd.DataFrame:
    """
    거래대금 구하기
    :param ticker: 티커 6자리넘버
    :return: DF
    """
    today_date = datetime.now().strftime('%Y%m%d')
    agency_list = ['금융투자', '보험', '투신', '사모', '은행', '기타금융', '연기금']
    init_sell = stock.get_market_trading_value_by_date("20170101", today_date, ticker, on='매도', detail=True)
    sleep(1)
    init_buy = stock.get_market_trading_value_by_date("20170101", today_date, ticker, on='매수', detail=True)
    sleep(1)
    init_sell['기관합계'] = init_sell[agency_list].sum(axis=1)
    init_buy['기관합계'] = init_buy[agency_list].sum(axis=1)
    init_sell.drop(agency_list+['기타법인', '기타외국인', '전체'], axis=1, inplace=True)
    init_buy.drop(agency_list+['기타법인', '기타외국인', '전체'], axis=1, inplace=True)
    init_sell.columns = [_col + '_매도금' for _col in init_sell.columns]
    init_buy.columns = [_col + '_매수금' for _col in init_buy.columns]
    init_data = pd.concat([init_sell, init_buy], axis=1)
    return init_data


def init_trading_volume(ticker: str) -> pd.DataFrame:
    """
    거래량 구하기
    :param ticker: 티커 6자리
    :return: DF
    """
    today_date = datetime.now().strftime('%Y%m%d')
    agency_list = ['금융투자', '보험', '투신', '사모', '은행', '기타금융', '연기금']
    init_sell = stock.get_market_trading_volume_by_date("20170101", today_date, ticker, on='매도', detail=True)
    sleep(1)
    init_buy = stock.get_market_trading_volume_by_date("20170101", today_date, ticker, on='매수', detail=True)
    sleep(1)
    init_sell['기관합계'] = init_sell[agency_list].sum(axis=1)
    init_buy['기관합계'] = init_buy[agency_list].sum(axis=1)
    init_sell.drop(agency_list+['기타법인', '기타외국인', '전체'], axis=1, inplace=True)
    init_buy.drop(agency_list+['기타법인', '기타외국인', '전체'], axis=1, inplace=True)
    init_sell.columns = [_col + '_매도량' for _col in init_sell.columns]
    init_buy.columns = [_col + '_매수량' for _col in init_buy.columns]
    init_data = pd.concat([init_sell, init_buy], axis=1)
    return init_data


def add_trading(base_path: str,
                trading_foreigner: pd.DataFrame,
                trading_personal: pd.DataFrame,
                trading_agencysum: pd.DataFrame) -> None:
    merge_index = set(trading_foreigner.index) | set(trading_personal.index) | set(trading_agencysum.index)
    for ticker in merge_index:
        file_path = os.path.join(base_path, f'{ticker}')
        chk_list = []
        if os.path.isdir(file_path):
            if os.path.exists(os.path.join(file_path, 'trading.pkl.gz')):
                for _trading in [trading_foreigner, trading_personal, trading_agencysum]:
                    try:
                        chk_list.extend(_trading.loc[ticker].values)
                    except KeyError:
                        chk_list.extend([0, 0, 0, 0])
                chk_df = pd.DataFrame([chk_list], index=pd.DatetimeIndex([datetime.now().date()])[0],
                                      columns=['외국인_매도량', '외국인_매수량', '외국인_매도금', '외국인_매수금',
                                               '개인_매도량', '개인_매수량', '개인_매도금', '개인_매수금',
                                               '기관합계_매도량', '기관합계_매수량', '기관합계_매도금', '기관합계_매수금'])
                with gzip.open(os.path.join(file_path, 'trading.pkl.gz'), 'rb') as f:
                    temp_data = _pickle.load(f)
                temp_data = pd.concat([temp_data, chk_df[temp_data.columns]], axis=0)
                with gzip.open(os.path.join(file_path, 'trading.pkl.gz'), 'wb') as f:
                    _pickle.dump(temp_data, f)
            else:
                init_value = init_trading_value(ticker)
                init_volume = init_trading_volume(ticker)
                init_file = pd.concat([init_value, init_volume], axis=1)
                with gzip.open(os.path.join(file_path, 'trading.pkl.gz'), 'wb') as f:
                    _pickle.dump(init_file, f)
        else:
            continue


# if __name__ == "__main__":
