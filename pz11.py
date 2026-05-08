import time
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine('mysql+pymysql://root:root@127.0.0.1:3308/index_test_pz11')

df = pd.read_csv(r'C:\Users\Владелец\Downloads\100000 Sales Records.csv')
df.columns = [c.strip().replace(' ', '_').lower() for c in df.columns]
df['order_date'] = pd.to_datetime(df['order_date'])
df['ship_date'] = pd.to_datetime(df['ship_date'])

with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS sales;"))
    conn.execute(text("""
        CREATE TABLE sales (
            id INT AUTO_INCREMENT PRIMARY KEY,
            region VARCHAR(50), country VARCHAR(50), item_type VARCHAR(30),
            sales_channel VARCHAR(10), order_priority VARCHAR(10),
            order_date DATE, order_id INT, ship_date DATE,
            units_sold INT, unit_price DOUBLE, unit_cost DOUBLE,
            total_revenue DOUBLE, total_cost DOUBLE, total_profit DOUBLE
        );
    """))
    conn.commit()

df.to_sql('sales', engine, if_exists='append', index=False, chunksize=5000)

queries = {
    "1. GROUP BY order_priority": "SELECT order_priority, COUNT(*) FROM sales GROUP BY order_priority;",
    "2. WHERE region + country": "SELECT COUNT(*) FROM sales WHERE region = 'Asia' AND country = 'India';",
    "3. GROUP BY item_type": "SELECT item_type, AVG(total_revenue) FROM sales GROUP BY item_type;",
    "4. ORDER BY total_profit": "SELECT * FROM sales ORDER BY total_profit DESC LIMIT 10;",
    "5. WHERE order_date": "SELECT COUNT(*) FROM sales WHERE order_date BETWEEN '2015-01-01' AND '2016-01-01';",
    "6. GROUP BY year": "SELECT YEAR(order_date), SUM(total_revenue) FROM sales GROUP BY YEAR(order_date);",
    "7. WHERE channel + item": "SELECT COUNT(*) FROM sales WHERE sales_channel = 'Online' AND item_type = 'Cosmetics';",
    "8. GROUP BY country": "SELECT country, SUM(units_sold) FROM sales GROUP BY country ORDER BY SUM(units_sold) DESC;",
    "9. WHERE profit + priority": "SELECT * FROM sales WHERE total_profit > 5000 AND order_priority = 'H';",
    "10. GROUP BY region": "SELECT region, AVG(unit_price) FROM sales GROUP BY region;"
}

def run_tests(label):
    print(f"\n{label}")
    results = []
    with engine.connect() as conn:
        for name, q in queries.items():
            t = time.perf_counter()
            conn.execute(text(q))
            elapsed = round(time.perf_counter() - t, 4)
            results.append(elapsed)
            print(f"{name}: {elapsed} сек")
    return results

with engine.connect() as conn:
    for idx in ['idx_region', 'idx_country', 'idx_item_type', 'idx_order_date',
                'idx_sales_channel', 'idx_order_priority', 'idx_channel_item']:
        try: conn.execute(text(f"DROP INDEX {idx} ON sales;"))
        except: pass
    conn.commit()

t1 = run_tests("БЕЗ ИНДЕКСОВ")

with engine.connect() as conn:
    conn.execute(text("CREATE INDEX idx_region ON sales(region);"))
    conn.execute(text("CREATE INDEX idx_country ON sales(country);"))
    conn.execute(text("CREATE INDEX idx_item_type ON sales(item_type);"))
    conn.execute(text("CREATE INDEX idx_order_date ON sales(order_date);"))
    conn.execute(text("CREATE INDEX idx_sales_channel ON sales(sales_channel);"))
    conn.execute(text("CREATE INDEX idx_order_priority ON sales(order_priority);"))
    conn.execute(text("CREATE INDEX idx_channel_item ON sales(sales_channel, item_type);"))
    conn.commit()

t2 = run_tests("С ИНДЕКСАМИ")

print(f"\n{'|Запрос|':<20} {'|Без индексов|':<15} {'|С индексами|':<20} {'|Прирост|':<15}")
for i, (name, a, b) in enumerate(zip(queries.keys(), t1, t2)):
    speed = round(a/b, 2) if b else 0
    print(f"{name:<25} {a:<15} {b:<15} {speed}x")