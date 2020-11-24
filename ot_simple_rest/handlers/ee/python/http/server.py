from bottle import Bottle, request


class Server:

    def __init__(self, port, ip='0.0.0.0'):
        self.port = port
        self.address = ip
        self.batches = []
        self.bottle = Bottle()
        self.init_routes()

    def init_routes(self):
        self.bottle.route(path='/batch', method='POST', callback=self.batch)
        self.bottle.route(path='/calc', method='POST', callback=self.calc)

    def batch(self, *args):
        print('batch')
        batch = request.body.read()
        self.batches.append(batch)
        return 'OK'

    def calc(self):
        print('calc')
        print(self.batches)
        return 'OK'

    def run(self):
        self.bottle.run(host='localhost', port=self.port)
