import time

import requests
from loguru import logger

SERVER_HOST = "web"
SERVER_PORT = 5000
URL = "http://" + SERVER_HOST
if SERVER_PORT != 80:
    URL += ":{}".format(SERVER_PORT)
IMAGES_ENDPOINT = URL + "/api/v1.0/images"


def test_post_image():
    input_data = {"image_url": "https://jrnlst.ru/sites/default/files/covers/cover_6.jpg"}
    response = requests.post(IMAGES_ENDPOINT, json=input_data)
    logger.info(response.json())
    assert response.status_code == 200
    assert "image_id" in response.json()


def test_get_image():
    time.sleep(30)  # Waiting for 30 seconds to process image
    response = requests.get(IMAGES_ENDPOINT)
    image_ids = response.json()["image_ids"]
    response = requests.get(f"{IMAGES_ENDPOINT}/{image_ids[0]}")
    logger.info(response.json())
    assert response.status_code == 200
    assert "description" in response.json()
    assert isinstance(response.json()["description"], str)


def test_get_image_error():
    response = requests.get(f"{IMAGES_ENDPOINT}")
    image_ids = response.json()["image_ids"]
    not_existing_image_id = max(image_ids) + 1
    response = requests.get(f"{IMAGES_ENDPOINT}/{not_existing_image_id}")
    assert response.status_code == 404
