# I dont know why but the DuckDB CLI doesnt work but it works with Python
import duckdb

conn = duckdb.connect(database=r'C:\Users\lenovo\Desktop\Self Learning\DATA-ENGINEERING-LAB-PROJECT\Lab2\lab2_google_play\data\db\playstore.duckdb', read_only=False)

print(conn.execute("SELECT * FROM test_connection").fetchall())
conn.close()
