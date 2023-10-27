import pymongo
import mysql.connector
import schedule
import time

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["odeme_veritabani"]
mongo_collection = mongo_db["odemeler"]

mysql_connection = mysql.connector.connect(
    host="localhost",
    user="kullanici_adi",
    password="parola",
    database="mysql_veritabani"
)
mysql_cursor = mysql_connection.cursor()

def process_payments():
    payments = mongo_collection.find()
    
    for payment in payments:
        mysql_cursor.execute("INSERT INTO odemeler (musteri_id, miktar) VALUES (%s, %s)", (payment["musteri_id"], payment["miktar"]))
        mysql_connection.commit()
        
        print(f"Fatura oluşturuldu: Müşteri ID - {payment['musteri_id']}, Miktar - {payment['miktar']}")

schedule.every().day.at("12:00").do(process_payments)

while True:
    schedule.run_pending()
    time.sleep(1)
