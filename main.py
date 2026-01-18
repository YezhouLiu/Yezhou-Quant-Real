from data_download.input.corporate_actions_extractor import extract_corporate_actions
from data_download.input.price_downloader import download_prices

if __name__ == "__main__":
    download_prices()
    extract_corporate_actions()