import pymongo
import certifi

URI = "mongodb+srv://poorvi1si23ad037_db_user:CvgWGwtnby2Uvtxj@cluster0.xvwmptt.mongodb.net/?appName=Cluster0"

try:
    client = pymongo.MongoClient(URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=15000)
    dbs = client.list_database_names()
    print("Connected successfully!")
    print("Databases:", dbs)
    client.close()
except Exception as e:
    print("Connection failed:", e)
