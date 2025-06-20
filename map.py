from flask import Flask, jsonify, request
import requests
import os

app = Flask(__name__)

# Azure Maps configuration
AZURE_MAPS_KEY = os.environ.get('AZURE_MAPS_KEY')
AZURE_MAPS_BASE_URL = "https://atlas.microsoft.com"

@app.route('/api/geocode', methods=['GET'])
def geocode_address():
    """Custom endpoint to geocode an address"""
    address = request.args.get('address')
    
    if not address:
        return jsonify({'error': 'Address parameter required'}), 400
    
    # Call Azure Maps Search API
    url = f"{AZURE_MAPS_BASE_URL}/search/address/json"
    params = {
        'api-version': '1.0',
        'subscription-key': AZURE_MAPS_KEY,
        'query': address
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        # Process and return custom formatted response
        if data['results']:
            result = data['results'][0]
            return jsonify({
                'latitude': result['position']['lat'],
                'longitude': result['position']['lon'],
                'formatted_address': result['address']['freeformAddress']
            })
    
    return jsonify({'error': 'Geocoding failed'}), 500

@app.route('/api/route', methods=['POST'])
def calculate_route():
    """Custom endpoint to calculate route between points"""
    data = request.json
    start = data.get('start')  # [lon, lat]
    end = data.get('end')      # [lon, lat]
    
    # Call Azure Maps Route API
    url = f"{AZURE_MAPS_BASE_URL}/route/directions/json"
    params = {
        'api-version': '1.0',
        'subscription-key': AZURE_MAPS_KEY,
        'query': f"{start[1]},{start[0]}:{end[1]},{end[0]}"
    }
    
    response = requests.get(url, params=params)
    # Process and return custom response...

if __name__ == '__main__':
    app.run(debug=True)
