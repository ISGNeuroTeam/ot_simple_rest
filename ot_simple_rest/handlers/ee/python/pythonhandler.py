import logging
import uuid

import multiprocessing
from abc import ABC

import tornado.web

from handlers.ee.python.http.server import Server


__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2020, ISG Neuro"
__credits__ = []
__license__ = ""
__version__ = "0.1.0"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@isgneuro.com"
__status__ = "Develop"


class PythonHandler(tornado.web.RequestHandler, ABC):

    def initialize(self, static_conf):
        """
        Gets config and init logger.

        :param static_conf: todo

        :return:
        """
        self.handler_id = str(uuid.uuid4())
        self.static_conf = static_conf
        self.logger = logging.getLogger('osr_hid')

    def write_error(self, status_code: int, **kwargs) -> None:
        """Override to implement custom error pages.

        ``write_error`` may call `write`, `render`, `set_header`, etc
        to produce output as usual.

        If this error was caused by an uncaught exception (including
        HTTPError), an ``exc_info`` triple will be available as
        ``kwargs["exc_info"]``.  Note that this exception may not be
        the "current" exception for purposes of methods like
        ``sys.exc_info()`` or ``traceback.format_exc``.
        """
        if "exc_info" in kwargs:
            error = str(kwargs["exc_info"][1])
            error_msg = {"status": "rest_error", "server_error": self._reason, "status_code": status_code,
                         "error": error}
            self.logger.debug(f'Error_msg: {error_msg}', extra={'hid': self.handler_id})
            self.finish(error_msg)

    async def post(self):
        """
        It writes response to remote side.

        :return:
        """

        def eep_target(port):
            server = Server(port)
            server.run()

        self.logger.info("Request: %s" % self.request.body)

        print('Starting ee process')
        eep = multiprocessing.Process(target=eep_target, args=(50100,))
        eep.start()

        print('Started ee process')
        response = {'status': 'ok'}
        self.write(response)

