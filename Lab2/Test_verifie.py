import duckdb

con = duckdb.connect(r'c:\Users\lenovo\Desktop\Self Learning\DATA-ENGINEERING-LAB-PROJECT\Lab2\lab2_google_play\data\db\playstore.duckdb')

tables = con.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()

print("--- Snapshot records for notebooklm ---")
res = con.execute("SELECT app_id, app_name, genre_name, dbt_valid_from, dbt_valid_to FROM snapshots.apps_snapshot WHERE app_id = 'com.google.android.apps.labs.language.tailwind' ORDER BY dbt_valid_from").fetchall()
for r in res:
    print(r)

print("--- dim_apps_scd records for notebooklm ---")
res2 = con.execute("SELECT app_id, app_name, is_current FROM main.dim_apps_scd WHERE app_id = 'com.google.android.apps.labs.language.tailwind' ORDER BY dbt_valid_from").fetchall()
for r in res2:
    print(r)

con.close()
