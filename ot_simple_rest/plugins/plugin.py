from abc import ABC


class Plugin(ABC):

    def __init__(self):
        self.schema = None
        self.dataset = None

    def load_data(self, schema, dataset):
        self.schema = schema
        self.dataset = dataset

    def calc(self):
        pass