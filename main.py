import requests

# Example: Get Dodgers roster
url = "https://statsapi.mlb.com/api/v1/teams/119/roster?season=2024"
response = requests.get(url)
data = response.json()