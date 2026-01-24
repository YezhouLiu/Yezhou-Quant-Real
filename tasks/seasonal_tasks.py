from data_download.input.fundamentals_downloader import download_fundamentals


def seasonal_update():
    # 季度更新公司基本面数据
    download_fundamentals(tradable_only=True, sleep_seconds=0.3,)