from database.utils.db_utils import get_db_connection
from utils.logger import get_logger

log = get_logger("database")


def drop_all_tables():
    """删除所有数据库表"""
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询所有用户表
        cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename;
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
            print("✓ 数据库中没有表")
            return
        
        print(f"找到 {len(tables)} 个表:")
        for table in tables:
            print(f"  - {table}")
        print()
        
        # 删除所有表
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            log.info(f"[✔] 已删除表 {table}")
        
        conn.commit()
        print(f"\n✅ 已删除 {len(tables)} 个表")
        
    except Exception as e:
        if conn:
            conn.rollback()
        log.error(f"[✖] 删除表失败: {e}")
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    print("\n" + "="*70)
    print("⚠️  警告：即将删除所有数据库表")
    print("="*70 + "\n")
    
    confirm = input("确认删除所有表？(输入 YES 继续): ")
    
    if confirm == "YES":
        drop_all_tables()
        print("\n" + "="*70)
        print("✅ 数据库表已全部删除")
        print("="*70 + "\n")
    else:
        print("\n❌ 操作已取消\n")
