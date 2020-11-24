from abc import ABC

import tornado.web


class Status:
    DWT = "data was transferred"
    DIT = "data is being transferred"
    RDT = "run data transfer"


class MainHandler(tornado.web.RequestHandler, ABC):

    def initialize(self, batches):
        self.batches = batches

    def post(self):
        batch = self.request.body.decode('utf-8')
        print(f'Batch: {batch}')
        if batch == Status.DWT:
            print(f'Status: {Status.DWT}')
            print(f'{self.batches[:10]}...{self.batches[-10:]}')
        elif batch == Status.RDT:
            print(f'Status: {Status.RDT}')
        else:
            print(f'Status: {Status.DIT}')
            self.batches += batch
        self.write("OK")

