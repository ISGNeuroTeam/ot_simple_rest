import os
import io
import jwt
import uuid
import json
import tempfile
import tarfile
from datetime import datetime
import logging

import tornado.web
import tornado.httputil

from handlers.eva.base import BaseHandler

__author__ = "Fedor Metelkin"
__copyright__ = "Copyright 2020, ISG Neuro"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Andrey Starchenkov"
__email__ = "fmetelkin@isgneuro.com"
__status__ = "Develop"


class PaperHandler(BaseHandler):
    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.static_conf = kwargs['static_conf']
        self.logger = logging.getLogger('osr')

    async def prepare(self):
        client_token = self.get_cookie('eva_token')
        if client_token:
            self.token = client_token
            try:
                token_data = self.decode_token(client_token)
                user_id = token_data['user_id']
                self.permissions = self.db.get_permissions_data(user_id=user_id,
                                                                names_only=True)
            except (jwt.ExpiredSignatureError, jwt.DecodeError):
                pass
            else:
                self.current_user = user_id

        if not self.current_user:
            raise tornado.web.HTTPError(401, "unauthorized")

    async def post(self):
        body = self.request.body
        args = {}
        files = {}
        tornado.httputil.parse_body_arguments(self.request.headers['Content-Type'], body, args, files)
        reports_path = self.static_conf['static_path'] + 'reports'
        _file = files['file'][0]

        saving_full_path = os.path.join(reports_path, _file['filename'])
        if not os.path.exists(saving_full_path):
            with open(saving_full_path, 'wb') as f:
                f.write(_file['body'])
        self.write({'status': 'ok'})



