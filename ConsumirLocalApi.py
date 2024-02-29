import requests

# URL de la API local
url_api = 'http://127.0.0.1:5000/api/datos'

# Realizar una solicitud GET a la API
response = requests.get(url_api)

# Verificar si la solicitud fue exitosa (código de estado 200)
if response.status_code == 200:
    # Obtener los datos en formato JSON
    datos_api = response.json()
    
    # Imprimir los datos obtenidos
    for dato in datos_api:
        print(dato)
else:
    # Mostrar un mensaje si la solicitud no fue exitosa
    print(f"Error en la solicitud. Código de estado: {response.status_code}")
