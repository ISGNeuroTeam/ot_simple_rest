class Worker:

    def __init__(self):
        self.schema = None
        self.dataset = None

    def load_data(self, schema, dataset):
        self.schema = schema
        self.dataset = dataset

    def calc(self):
        return "`y` INT, `z` INT", [[1, 2], [3, 4]]
