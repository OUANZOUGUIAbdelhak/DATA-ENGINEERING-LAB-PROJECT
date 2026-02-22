import json

file_path = r"c:\Users\lenovo\Desktop\Self Learning\DATA-ENGINEERING-LAB-PROJECT\Lab2\lab2_google_play\data\raw\apps_metadata.json"

with open(file_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Mutate the first app's genre to test SCD tracking
modified_app = None
for app in data:
    if "genre" in app:
        old_genre = app["genre"]
        app["genre"] = old_genre + " Modified"
        modified_app = app
        break

if modified_app:
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print(f"Modified app {modified_app['title']} genre to {modified_app['genre']}")
else:
    print("Could not find an app to modify")
