from handlers.ee.python.plugins.plugin import Plugin


class Worker(Plugin):

    def calc(self):

        return "`y` INT, `z` INT", [[1, 2], [3, 4]]
