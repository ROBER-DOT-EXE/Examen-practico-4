import pymongo
import json
import requests
import time as time_sleep

DB_NAME = "cosmosdb-examen-practico-4"
COLLECTION_NAME = 'base-de-datos-cosmos-examen-practico-4'
CONNECTION = ''

API_KEY = ''

def get_weather_data(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    return response.json()

def insert_sample_document(client, data):
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    document_id = collection.insert_one(data).inserted_id
    print("Documento insertado con id {}".format(document_id))
    return document_id

while True:
    city = "Madrid"
    weather_data = get_weather_data(city)
    
    if weather_data.get('cod') != 200:
        print(f"Error obteniendo datos del clima: {weather_data.get('message')}")
        continue

    data_ready = {
        'city': city,
        'Time': weather_data['dt'],
        'Temperature': weather_data['main']['temp'],
        'Weather': weather_data['weather'][0]['description'],
        'Other_data': {
            'humidity': weather_data['main']['humidity'],
            'pressure': weather_data['main']['pressure'],
            'wind_speed': weather_data['wind']['speed']
        }
    }

    insert_sample_document(client=pymongo.MongoClient(CONNECTION), data=data_ready)
    time_sleep.sleep(1)