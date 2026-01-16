import sys
from pathlib import Path
import os

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
os.chdir(project_root)

import yfinance as yf
from time import sleep
from database.utils.db_utils import get_db_connection
from utils.logger import get_logger

log = get_logger("enrich_yfinance")


def enrich_sector_industry(force: bool = False):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        if force:
            cursor.execute("SELECT ticker FROM instruments")
        else:
            cursor.execute("SELECT ticker FROM instruments WHERE sector IS NULL OR industry IS NULL")
        
        tickers = [row[0] for row in cursor.fetchall()]
        
        if not tickers:
            log.info("✅ 所有记录已有 sector/industry")
            return
        
        log.info(f"📊 找到 {len(tickers)} 个需要补全的记录")
        
        success, failed = 0, 0
        for i, ticker in enumerate(tickers, 1):
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                
                sector = info.get('sector')
                industry = info.get('industry')
                
                if sector or industry:
                    cursor.execute("""
                        UPDATE instruments
                        SET sector = %s, industry = %s, updated_at = now()
                        WHERE ticker = %s
                    """, (sector, industry, ticker))
                    success += 1
                else:
                    failed += 1
                
                # 每50条输出一次进度
                if i % 50 == 0 or i == len(tickers):
                    log.info(f"[{i}/{len(tickers)}] 成功: {success}, 失败: {failed}")
                
                # 每100条commit一次
                if i % 100 == 0:
                    conn.commit()
                
                sleep(0.1)
                
            except Exception as e:
                failed += 1
                if failed % 50 == 0:
                    log.warning(f"失败数量: {failed}")
        
        conn.commit()
        log.info(f"✅ 完成: 成功 {success}, 失败 {failed}")


if __name__ == "__main__":
    import sys
    force = '--force' in sys.argv
    enrich_sector_industry(force=force)
