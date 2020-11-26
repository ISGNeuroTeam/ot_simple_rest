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

    def __init__(self, port, script, address='0.0.0.0'):
        self.script = script
        self.port = port
        self.address = address
        self.batches = []
        self._schema = None
        self.plugin = None
        self.bottle = Bottle()
        self.init_routes()
        self.init_plugin()
        self.queue = multiprocessing.Queue()

    def init_routes(self):
        self.bottle.route(path='/schema', method='POST', callback=self.url_schema)
        self.bottle.route(path='/batch', method='POST', callback=self.url_batch)
        self.bottle.route(path='/calc', method='GET', callback=self.url_calc)
        self.bottle.route(path='/status', method='GET', callback=self.url_status)
        self.bottle.route(path='/result', method='GET', callback=self.url_result)

    def init_plugin(self):
        self.plugin = self.script.Worker()

    def url_schema(self):
        schema = request.body.read()
        self._schema = schema
        return self.status['ok']

    def url_batch(self):
        batch = request.body.read()
        batch = json.loads(batch)
        self.batches += batch

        return self.status['ok']

    def url_calc(self):
        cp = multiprocessing.Process(target=self.external_process,
                                     args=(self.queue, self.batches, self._schema, self.plugin))
        cp.start()
        return self.status['ok']

    def url_status(self):
        if self.queue.empty():
            return self.status['running']
        else:
            return self.status['finished']

    def url_result(self):
        schema, dataset = self.queue.get()
        response = {'schema': schema, 'dataset': dataset}
        return response

    def run(self):
        self.bottle.run(host=self.address, port=self.port)

    @staticmethod
    def external_process(queue, batches, schema, plugin):
        plugin.load_data(schema, batches)
        new_schema, result = plugin.calc()

        queue.put((new_schema, result))
