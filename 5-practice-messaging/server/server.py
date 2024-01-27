from flask import Flask, request, abort

from config import IMAGES_ENDPOINT, DATA_DIR

import pika
import json

# from loguru import logger
import os


def create_app() -> Flask:
    """
    Create flask application
    """
    app = Flask(__name__)
    connection = pika.BlockingConnection(pika.ConnectionParameters("RabbitMQ"))
    channel = connection.channel()
    channel.queue_declare(queue="main")

    create_app.images_cnt = 0

    @app.route(IMAGES_ENDPOINT, methods=["POST"])
    def add_image():
        create_app.images_cnt += 1
        image_url = request.json["image_url"]
        msg_body = str(create_app.images_cnt) + "#" + image_url
        channel.basic_publish(exchange="", routing_key="main", body=msg_body)
        return json.dumps({"image_id": create_app.images_cnt})

    @app.route(IMAGES_ENDPOINT, methods=["GET"])
    def get_image_ids():
        files_in_data = os.listdir(DATA_DIR)
        image_ids = []
        for file in files_in_data:
            file_id = file.split(".")[0]
            image_ids.append(int(file_id))
        return json.dumps({"image_ids": image_ids})

    @app.route(f"{IMAGES_ENDPOINT}/<string:image_id>", methods=["GET"])
    def get_processing_result(image_id):
        files_in_data = os.listdir(DATA_DIR)
        for file in files_in_data:
            file_id = file.split(".")[0]
            if file_id == image_id:
                fin = open(DATA_DIR + "/" + file, "r")
                description = fin.readline()
                return json.dumps({"description": description})
        abort(404)

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
