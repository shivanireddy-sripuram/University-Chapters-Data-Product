import requests


API_URL = (
    "https://services2.arcgis.com/5I7u4SJE1vUr79JC/"
    "arcgis/rest/services/UniversityChapters_Public/"
    "FeatureServer/0/query"
)

PARAMS = {
    "where": "State IN ('CA','OR','WA')",
    "outFields": "*",
    "returnGeometry": "true",
    "f": "json",
}


def fetch_university_chapters():
    """Fetch university chapter data from the ArcGIS FeatureServer."""

    response = requests.get(
        API_URL,
        params=PARAMS,
        timeout=30,
    )

    response.raise_for_status()

    payload = response.json()

    return payload


if __name__ == "__main__":
    data = fetch_university_chapters()

    features = data.get("features", [])

    print(f"Successfully retrieved {len(features)} university chapters.")