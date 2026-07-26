import pymongo
import certifi

URI = "mongodb+srv://poorvi1si23ad037_db_user:CvgWGwtnby2Uvtxj@cluster0.xvwmptt.mongodb.net/?appName=Cluster0"
client = pymongo.MongoClient(URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=15000)

# Drop entire database to completely free all storage
client.drop_database("urban_env_db")
print("Dropped entire urban_env_db database")
print("All storage freed.")
print("Collections now:", client["urban_env_db"].list_collection_names())
client.close()
