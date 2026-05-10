import unittest

from fastapi.testclient import TestClient

from backend.app.main import app


class DesoGeojsonEndpointTest(unittest.TestCase):
    def test_serves_stockholm_deso_geojson(self):
        client = TestClient(app)

        response = client.get("/api/deso_geojson")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"].split(";")[0], "application/geo+json")
        payload = response.json()
        self.assertEqual(payload["type"], "FeatureCollection")
        self.assertGreater(len(payload["features"]), 1000)
        self.assertIn("desokod", payload["features"][0]["properties"])


if __name__ == "__main__":
    unittest.main()
