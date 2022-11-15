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

# logger = logging.getLogger('websockets')
# logger.setLevel(logging.DEBUG)
# logger.addHandler(logging.StreamHandler())

from opentelemetry import metrics
from opentelemetry.exporter.prometheus_remote_write import (
    PrometheusRemoteWriteMetricsExporter,
)
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

exporter = PrometheusRemoteWriteMetricsExporter(
    endpoint="http://localhost:8080/workspaces/ws-d59302a8-3858-4815-83e3-4e6519150ea4/api/v1/remote_write",
    headers={
        "host": "aps-workspaces.us-east-1.amazonaws.com"
    }
)
reader = PeriodicExportingMetricReader(exporter, 1000)
provider = MeterProvider(metric_readers=[reader])
metrics.set_meter_provider(provider)
meter = metrics.get_meter(__name__)


OK_Message_Num = 1
Late_Message_NUM = 0

def get_late_raito(observer):
    global Late_Message_NUM
    global OK_Message_Num
    raito = Late_Message_NUM / OK_Message_Num
    Late_Message_NUM = 0
    OK_Message_Num = 0
    yield Observation(raito, {})

msg_sent_counter = meter.create_counter(
    name="messages sent",
    description="number of messages",
    unit="1",
)

msg_received_counter = meter.create_counter(
    name="ws messages received",
    description="number of messages",
    unit="1",
)

ws_conn_counter = meter.create_up_down_counter(
    name="ws connections open",
    description="number of messages",
    unit="1",
)

ws_errors = meter.create_counter(
    name="ws errors",
    description="Errors.",
    unit="1"
)

late_raito = meter.create_observable_gauge(
    callbacks=[get_late_raito],
    name="late message raito",
    description="",
    unit="1"
)

resp_time_hg = meter.create_histogram(
    name="Response time",
    description="Response time of messages",
    unit="1"
)

resp_size_hg = meter.create_histogram(
    name="Response size",
    description="Response size of messages",
    unit="1"
)


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
        self.is_closed = False
        self.close_reason = 'active'

        self.cnt = 0
        self.message_cnt = 0

    def get_connection(self):
        return self.conn

    async def close_connection(self):
        await self.conn.close()
        self.is_closed = True

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
        if self.is_closed:
            return
        next_message = self.get_next_message(False if self.cnt != 0 else True)

        self.log[next_message['id']] = dict(cnt=self.cnt)
        self.cnt += 1
        try:
            await self.conn.send(json.dumps(next_message))
            msg_sent_counter.add(1)
        except ConnectionClosedOK as e:
            print('Connection closed ok {}'.format(e.reason))
            self.close_reason = 'server_closed'
            self.log[next_message['id']]['state'] = 'server_closed'
            self.is_closed = True
            ws_errors.add(1)
        except ConnectionClosedError as e:
            print('Connection closed error {}'.format(e.reason))
            self.is_closed = True
            self.close_reason = 'client_closed'
            self.log[next_message['id']]['state'] = 'client_closed'
        finally:
            self.log[next_message['id']]['sent_at'] = time.time()

    async def consumer_handler(self):
        async for message in self.conn:
            self.process_message(message)

    def process_message(self, message):
        arrived_at = time.time()
        try:
            obj = json.loads(message)
            msg_received_counter.add(1)
            if 'id' in obj and obj['id'] in self.log:
                self.state_id = obj['response']['stateId'] if 'response' in obj and 'stateId' in obj[
                    'response'] else self.state_id
                if self.cnt != self.log[obj['id']]['cnt'] + 1:
                    global Late_Message_NUM
                    Late_Message_NUM += 1
                    self.log[obj['id']]['state'] = 'late'
                else:
                    global OK_Message_Num
                    OK_Message_Num += 1
                    self.log[obj['id']]['state'] = 'OK'
                self.log[obj['id']]['response_at'] = arrived_at
                resp_time_hg.record(arrived_at - self.log[obj['id']]['sent_at'])
                self.log[obj['id']]['resp_cnt'] = self.log[obj['id']]['resp_cnt'] + 1 \
                    if 'resp_cnt' in self.log[obj['id']] else 1
                self.log[obj['id']]['resp_size'] = self.log[obj['id']]['resp_size'] + len(message) \
                    if 'resp_size' in self.log[obj['id']] else len(message)
                resp_size_hg.record(self.log[obj['id']]['resp_size'])
        except Exception as e:
            pass


class StressTester:

    async def init_connections(self, conn_num):
        for i in range(0, conn_num):
            conn = await websockets.connect(self.url,
                                            subprotocols=['tools.refinery.language.web.xtext.v1'])
            ind = len(self.connections) + 1
            conn_handler = ConnectionHandler(conn, ind)
            self.connections.append(conn_handler)
            # Start async process to receive all messages of this connection.
            asyncio.create_task(conn_handler.consumer_handler())

    async def update_connection_num(self, conn_num):
        if conn_num < self.conn_count:
            to_close = self.conn_count - conn_num
            for i in range(0, to_close):
                await self.connections[i].close_connection()
        if self.conn_count < conn_num:
            await self.init_connections(conn_num - self.conn_count)
        self.conn_count = conn_num
        ws_conn_counter.add(conn_num)

    def __init__(self, ws_url, num_of_connections):
        self.url = ws_url
        self.conn_count = num_of_connections
        self.connections = []

    def send_message_everywhere(self):
        for c in self.connections:
            asyncio.create_task(c.send_message())


WS_URL = 'wss://xtext.test.refinery.services/xtext-service'

load_profile = [dict(time_wait=1, itherations=1, conns=1), dict(time_wait=0.5, itherations=1, conns=2)]


def get_stats(handlers):
    open_num = 0
    server_closed = 0
    client_closed = 0
    for h in handlers:
        if not h.is_closed:
            open_num += 1
        elif h.close_reason == 'server_closed':
            server_closed += 1
        elif h.close_reason == 'client_closed':
            client_closed += 1
    print(f'active: {open_num}, client_closed: {client_closed}, server_closed: {server_closed}')


async def start_testing():
    t = StressTester(WS_URL, 0)
    for l in load_profile:
        print('Running load profile: time_between = {}  itherations = {} conns =  {}'.format(l['time_wait'],
                                                                                             l['itherations'],
                                                                                             l['conns']))
        await t.update_connection_num(l['conns'])
        for i in range(0, l['itherations']):
            t.send_message_everywhere()
            await asyncio.sleep(l['time_wait'])
            if i % 50 == 0:
                get_stats(t.connections)
    print('DONE')
    get_stats(t.connections)
    for c in t.connections:
        await c.get_connection().close()
        # with open(f'stats/{c.conn_id}.csv','w+') as f:
        #    f.write('\n'.join([';'.join(
        #              [
        #              str(c.log[key]['cnt']),
        #              str(c.log[key]['sent_at']),
        #              str((c.log[key]['response_at'] if 'response_at' in c.log[key] else c.log[key]['sent_at'])-c.log[key]['sent_at']),
        #              str(c.log[key]['state']),
        #              str(c.log[key]['resp_cnt']),
        #              str(c.log[key]['resp_size'])
        #              ])
        #              for key in c.log]))


asyncio.run(start_testing())
