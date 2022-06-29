import json
import os
import logging

import tornado.web
import tornado.httputil

from handlers.eva.base import BaseHandler
from tools.svg_manager import SVGManager

class STATUS:
    OK = 3
    UNKNOWN_FAIL = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6


class SvgLoadHandler(BaseHandler):

    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.file_conf = kwargs['file_upload_conf']
        self.static_conf = kwargs['static_conf']
        self.logger = logging.getLogger('osr')
        svg_path = self.file_conf.get('svg_path', os.path.join(self.static_conf['static_path'], 'svg'))
        self.svg_manager = SVGManager(svg_path)

    async def post(self):
        try:
            body = self.request.body
            args, files = {}, {}
            tornado.httputil.parse_body_arguments(self.request.headers['Content-Type'], body, args, files)
            _file = files['file'][0]
            named_as = self.svg_manager.write(_file['filename'], _file['body'])
            self.write(json.dumps({'status': 'ok', 'filename': named_as, 'status_code': STATUS.OK}))
        except Exception as e:
            self.logger.error(f'Error while writing file: {e}')
            self.write(json.dumps({'status': 'failed', 'error': f'{e}', 'status_code': STATUS.UNKNOWN_FAIL}, default=str))

    async def delete(self):
        try:
            filename = self.get_argument('filename')
            deleted = self.svg_manager.delete(filename)
            if deleted:
                self.write(json.dumps({'status': 'ok', 'status_code': STATUS.OK}))
            else:
                self.write(json.dumps({'status': 'failed', 'error': 'file not found', 'status_code': STATUS.NOT_FOUND}))
        except Exception as e:
            self.logger.error(f'Error while deleting file: {e}')
            self.write(json.dumps({'status': 'failed', 'error': f'{e}', 'status_code': STATUS.UNKNOWN_FAIL}, default=str))
