import json
import sys
import logging
import jwt

import tornado.web
import tornado.httputil

from handlers.eva.base import BaseHandler
from tools.file_manager import FileManager
from utils.primitives import RestUser

class FileLoadHandler(BaseHandler):
    async def prepare(self):
        client_token = self.get_cookie('eva_token')
        if client_token:
            self.token = client_token
            try:
                token_data = self.decode_token(client_token)
                user_id = token_data['user_id']
                user_name = token_data['username']
                self.request.user = RestUser(name=user_name, _id=user_id)
                self.permissions = self.db.get_permissions_data(user_id=user_id, names_only=True)
            except (jwt.ExpiredSignatureError, jwt.DecodeError):
                pass
            else:
                self.current_user = user_id

        if not self.current_user:
            raise tornado.web.HTTPError(401, "unauthorized")

    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.file_conf = kwargs['file_upload_conf']
        self.static_conf = kwargs['static_conf']
        self.logger = logging.getLogger('osr')

        self.file_path = self.file_conf.get('path', self.static_conf['static_path'])
        
        self.file_manager = FileManager(self.file_path)

    async def post(self):
        try:
            body = self.request.body
            args, files = {}, {}
            tornado.httputil.parse_body_arguments(self.request.headers['Content-Type'], body, args, files)
            _file = files['file'][0]
            named_as = self.file_manager.write(_file['filename'], _file['body'])
            self.logger.debug(f"Successfully written file to filesystem with name: '{named_as}'")
            response = {'status': 'ok', 'filename': named_as, 'notifications': [{'code': 3}]}
            self.write(json.dumps(response))
        except KeyError as e:
            self.logger.error("File must be placed in body field called 'file'!")
            response = {'status': 'failed', 'error': "File must be placed in body field called 'file'!", 'notifications': [{'code': 4}]}
            self.write(json.dumps(response))
        except Exception as e:
            self.logger.error(f'Error while writing file: {e}')
            response = {'status': 'failed', 'error': f'{e}', 'notifications': [{'code': 4}]}
            self.write(json.dumps(response))

    async def delete(self):
        try:
            filename = self.get_argument('filename')
            deleted = self.file_manager.delete(filename)
            if deleted:
                self.write(json.dumps({'status': 'ok'}))
            else:
                self.write(json.dumps({'status': 'failed', 'error': 'file not found'}))
        except Exception as e:
            self.logger.error(f'Error while deleting file: {e}')
            self.write(json.dumps({'status': 'failed', 'error': f'{e}'}, default=str))
