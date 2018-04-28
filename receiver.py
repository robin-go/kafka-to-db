import json
import logging
import multiprocessing

from kafka import KafkaConsumer
from sqlalchemy.orm import sessionmaker

from models import Record, engine

DBSession = sessionmaker(bind=engine)
session = DBSession()


class Consumer(multiprocessing.Process):

    def run(self):
        consumer = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            group_id='test')
        consumer.subscribe(['test'])

        max_num = 20
        chatlog_list = []
        for message in consumer:
            try:
                content = json.loads(message.value.decode())
                chatlog = Record(
                    botid=content.get('botid'),
                    sessionid=content.get('sessionid'),
                    usertype=content.get('usertype'),
                    msgcontent=content.get('msgcontent'),
                    msgtype=content.get('msgtype'),
                    msgtime=content.get('msgtime')
                )
                chatlog_list.append(chatlog)
                if len(chatlog_list) >= max_num:
                    session.add_all(chatlog_list)
                    try:
                        session.commit()
                        chatlog_list = []
                    except Exception:
                        logging.exception('数据有误，无法提交')
                        chatlog_list = []
            except Exception:
                logging.exception('程序异常')


def main():
    tasks = [
        Consumer(),
        Consumer(),
        Consumer()
    ]

    for t in tasks:
        t.start()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    logging.basicConfig(
        filename='receiver.log',
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
