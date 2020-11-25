import json
import multiprocessing

from bottle import Bottle, request


class Server:

    status = {
        'ok': {'status': 'ok'},
        'running': {'status': 'running'},
        'finished': {'status': 'finished'},
        'failed': {'status': 'failed'},
    }

    def __init__(self, port, address='0.0.0.0'):
        self.port = port
        self.address = address
        self.batches = []
        self._schema = None
        self.bottle = Bottle()
        self.init_routes()
        self.queue = multiprocessing.Queue()

    def init_routes(self):
        self.bottle.route(path='/schema', method='POST', callback=self.url_schema)
        self.bottle.route(path='/batch', method='POST', callback=self.url_batch)
        self.bottle.route(path='/calc', method='POST', callback=self.url_calc)
        self.bottle.route(path='/status', method='POST', callback=self.url_status)

    def url_schema(self):
        print('schema')
        schema = request.body.read()
        print(schema)
        self._schema = schema
        return self.status['ok']

    def url_batch(self):
        print('batch')
        batch = request.body.read()
        print(batch)
        batch = json.loads(batch)
        print(batch)
        self.batches += batch

        return self.status['ok']

    def url_calc(self):
        print('calc')
        print(self._schema)
        print(self.batches)
        cp = multiprocessing.Process(target=self.external_process, args=(self.queue, self.batches, self._schema))
        cp.start()
        return self.status['ok']

    def url_status(self):
        print('status')

    def run(self):
        self.bottle.run(host=self.address, port=self.port)

    @staticmethod
    def external_process(queue, batches, schema):
        calculator = Calculator.
