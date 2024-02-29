from flask import Flask, jsonify

app = Flask(__name__)

# Datos de ejemplo
datos = [
    {
        "id": "A002",
        "name": "James",
        "math": 89,
        "physics": 76,
        "chemistry": 51
    },
    {
        "id": "A003",
        "name": "Jenny",
        "math": 79,
        "physics": 90,
        "chemistry": 78
    },
    {
        "id": "A001",
        "name": "Tom",
        "math": 60,
        "physics": 66,
        "chemistry": 61
    }
]


## endpoint http://127.0.0.1:5000/api/datos
@app.route('/api/datos', methods=['GET'])
def obtener_datos():
    return jsonify(datos)

if __name__ == '__main__':
    app.run(debug=True)
