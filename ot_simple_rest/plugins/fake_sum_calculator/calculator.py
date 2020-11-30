from plugin import Plugin


class Worker(Plugin):

    def calc(self):
        dataset = self.dataset
        schema = self.schema
        return "`y` INT, `z` INT", [[1, 2], [3, 4]]
