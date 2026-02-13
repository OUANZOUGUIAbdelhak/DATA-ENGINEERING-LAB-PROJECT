# I dont know why but the DuckDB CLI doesnt work but it works with Python
import duckdb

conn = duckdb.connect(database=r'C:\Users\lenovo\Desktop\Self Learning\DATA-ENGINEERING-LAB-PROJECT\Lab2\lab2_google_play\data\db\playstore.duckdb', read_only=False)

print("Checking tables")
print(conn.execute("SHOW TABLES").fetchall())

print("Checking stg_playstore_apps")
print(conn.execute("SELECT * FROM stg_playstore_apps LIMIT 2").fetchall())

print("Checking stg_playstore_reviews")
print(conn.execute("SELECT * FROM stg_playstore_reviews LIMIT 2").fetchall())

conn.close()
