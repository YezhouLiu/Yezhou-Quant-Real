from data_download.input.corporate_actions_extractor import extract_corporate_actions
from data_download.input.fundamentals_downloader import download_fundamentals
from data_download.input.price_downloader import download_prices
from data_download.update.update_tradable_universe import update_tradable_universe


def daily_update():
    # 最近股价下载
    download_prices()
    
    # 公司行为抽取
    extract_corporate_actions()
    
    # 每日更新可交易标的
    update_tradable_universe()
    
    