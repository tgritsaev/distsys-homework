from caption import get_image_caption

# from server import processed_ids

import pika

# from loguru import logger

import os

DATA_DIR = "/data"


def main():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
            channel = connection.channel()

            channel.queue_declare(queue="main")

            def callback(ch, method, properties, body):
                str_body = body.decode()
                id, url = str_body.split("#")
                image_caption = get_image_caption(url)

                fout = open(DATA_DIR + f"/{id}.txt", "w+")
                fout.write(image_caption)
                fout.close()
                files_in_data = os.listdir(DATA_DIR)
                channel.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue="main", on_message_callback=callback)

            channel.start_consuming()
        except:
            continue


if __name__ == "__main__":
    main()
