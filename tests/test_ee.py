import time
import unittest
import logging

import requests

import handlers.ee.python.http.server

DWT = "data was transferred"
DIT = "data is being transferred"


class TestEE(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        self.port = 50100
        self.base_url = f'http://localhost:{self.port}'
        logging.basicConfig(
            level='DEBUG',
            format="%(asctime)s %(levelname)-s PID=%(process)d %(module)s:%(lineno)d \
    func=%(funcName)s - %(message)s")

    def test_start_ee(self):
        result = requests.post('http://localhost:50000/api/ee/process/python').json()
        print(result)
        target = 'ok'
        self.assertEqual(result['status'], target)

    def test_ee_http_server_batch(self):
        # server = handlers.ee.python.http.server.Server(self.port)
        # server.run()
        result = requests.post(f'{self.base_url}/batch', data=DIT).text
        result = requests.post(f'{self.base_url}/batch', data=DIT).text
        target = 'OK'
        self.assertEqual(result, target)

    def test_ee_http_server_calc(self):
        # server = handlers.ee.python.http.server.Server(self.port)
        # server.run()
        result = requests.post(f'{self.base_url}/calc', data=DWT).text
        target = 'OK'
        self.assertEqual(result, target)
