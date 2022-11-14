import random
import uuid
import _thread
import time
import asyncio
import websockets
import json
import logging

from websockets.exceptions import *

from sample_diffs import SampleInput

logger = logging.getLogger('websockets')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


def get_full_sample(problem_predix):
    with open('sample.txt', 'r') as f:
        content = f.read()
    return dict(fullText=content, resource="{}.problem".format(problem_predix), serviceType='update')


class ConnectionHandler:

    def __init__(self, ws_connection, conn_id, fulltext_prob=0.15):
        self.conn = ws_connection
        self.conn_id = conn_id

        self.problem_prefix = uuid.uuid1().hex[:8]
        self.state_id = "-80000000"
        self.fulltext_prob = fulltext_prob
        self.log = dict()

        self.cnt = 0
        self.message_cnt = 0

    def get_connection(self):
        return self.conn

    def get_next_message(self, is_first_message=False):
        new_id = uuid.uuid1().hex
        if random.random() < self.fulltext_prob or is_first_message:
            self.message_cnt = 0
            return dict(id=new_id, request=get_full_sample(self.problem_prefix))
        inp = SampleInput.INPUT_ARR[self.message_cnt]
        self.message_cnt = 0 if self.message_cnt + 1 >= len(SampleInput.INPUT_ARR) else self.message_cnt + 1
        to_send = dict(**inp, resource=f'{self.problem_prefix}.problem')
        if inp['serviceType'] == 'occurrences':
            to_send['expectedStateId'] = self.state_id
        else:
            to_send['requiredStateId'] = self.state_id

        return dict(id=new_id, request=to_send)

    async def send_message(self):
        next_message = self.get_next_message(False if self.cnt != 0 else True)

        self.log[next_message['id']] = dict(cnt=self.cnt)
        self.cnt += 1
        try:
            await self.conn.send(json.dumps(next_message))
            print('Send')
            print(next_message)
        except ConnectionClosedOK as e:
            print('Connection closed ok {}'.format(e.reason))
            self.log[next_message['id']]['state'] = 'Rejected'
        except ConnectionClosedError as e:
            print('Connection closed error {}'.format(e.reason))
            self.log[next_message['id']]['state'] = 'ClientErr'
        finally:
            self.log[next_message['id']]['sent_at'] = time.time()

    async def consumer_handler(self):
        async for message in self.conn:
            self.process_message(message)

    def process_message(self, message):
        arrived_at = time.time()
        try:
            obj = json.loads(message)
            print(obj)
            if 'id' in obj and obj['id'] in self.log:
                self.state_id = obj['response']['stateId'] if 'response' in obj and 'stateId' in obj[
                    'response'] else self.state_id
                if self.cnt != self.log[obj['id']]['cnt'] + 1:
                    self.log[obj['id']]['state'] = 'late'
                else:
                    self.log[obj['id']]['state'] = 'OK'
                self.log[obj['id']]['response_at'] = arrived_at
                self.log[obj['id']]['resp_cnt'] = self.log[obj['id']]['resp_cnt'] + 1 \
                    if 'resp_cnt' in self.log[obj['id']] else 1
                self.log[obj['id']]['resp_size'] = self.log[obj['id']]['resp_size'] + len(message) \
                    if 'resp_size' in self.log[obj['id']] else len(message)
        except Exception as e:
            pass


class StressTester:

    async def init_connections(self):
        for i in range(0, self.conn_count):
            conn = await websockets.connect(self.url,
                                            subprotocols=['tools.refinery.language.web.xtext.v1'])
            conn_handler = ConnectionHandler(conn, i)
            self.connections.append(conn_handler)
            # Start async process to receive all messages of this connection.
            asyncio.create_task(conn_handler.consumer_handler())

    def __init__(self, ws_url, num_of_connections, full_text_prob=0.1, max_diff_words=10):
        self.url = ws_url
        self.conn_count = num_of_connections
        self.connections = []

    def send_message_everywhere(self):
        for c in self.connections:
            asyncio.create_task(c.send_message())


WS_URL = 'wss://xtext.test.refinery.services/xtext-service'


load_profile = [dict(time_wait=1, itherations=1),dict(time_wait=0.5, itherations=0)]


async def start_testing():
    t = StressTester(WS_URL, [], 1)
    await t.init_connections()

    for l in load_profile:
        for i in range(0, l['itherations']):
            t.send_message_everywhere()
            await asyncio.sleep(l['time_wait'])

    await asyncio.sleep(5)
    for c in t.connections:
        await c.get_connection().close()
        with open(f'stats/{c.conn_id}.csv','w+') as f:
            f.write('\n'.join([';'.join(
                      [
                      str(c.log[key]['cnt']),
                      str(c.log[key]['sent_at']),
                      str((c.log[key]['response_at'] if 'response_at' in c.log[key] else c.log[key]['sent_at'])-c.log[key]['sent_at']),
                      str(c.log[key]['state']),
                      str(c.log[key]['resp_cnt']),
                      str(c.log[key]['resp_size'])
                      ])
                      for key in c.log]))


asyncio.run(start_testing())
