import logging
import threading
import time

from kafka import KafkaProducer


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092')

        while not self.stop_event.is_set():

            message = b'{"botid":"daa7d2b3-0155-4bbe-5c50-4e7bdd4f9b03", "sessionid":"f38676b0-ba94-11e5-8a59-51a87ea384d5", "usertype":"\xe5\x9d\x90\xe5\xb8\xad","msgcontent":"\xe5\x9d\x90\xe5\xb8\xad\xe7\x9a\x84\xe5\x8a\x9f\xe8\x83\xbd\xef\xbc\x9a\xe6\x8a\xa5\xe8\xa1\xa8\xe3\x80\x81\xe5\xbd\x95\xe9\x9f\xb3\xe3\x80\x81\xe6\x95\xb0\xe6\x8d\xae\xe8\xb5\x84\xe6\x96\x99\xe3\x80\x81\xe5\xb7\xa5\xe4\xbd\x9c\xe8\xae\xa1\xe5\x88\x92\xe7\xad\x89\xef\xbc\x9b\xe7\xae\xa1\xe7\x90\x86\xe6\x9d\x83\xe9\x99\x90\xe6\xaf\x94\xe8\xbe\x83\xe5\xa4\x9a","msgtype":"\xe6\x96\x87\xe6\x9c\xac","msgtime":"2018-02-24 17:56:40"}'
            producer.send('test', message)
            time.sleep(1)

        producer.close()


def main():
    tasks = [
        Producer(),
        Producer(),
        Producer(),
    ]

    for t in tasks:
        t.start()

    time.sleep(50)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
