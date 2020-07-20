import os
import io
import jwt
import uuid
import json
import openpyxl
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


class PaperLoadHandler(BaseHandler):
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
        self.write({'status': 'success'})

class PapersHandler(BaseHandler):
    def initialize(self, **kwargs):
        super().initialize(kwargs['db_conn_pool'])
        self.static_conf = kwargs['static_conf']
        self.logger = logging.getLogger('osr')

    async def get(self):
      reports_path = self.static_conf['static_path'] + 'reports'
      files = os.listdir(reports_path)
      self.write({'files':files,'status': 'success'})


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
        tornado.httputil.parse_body_arguments(self.request.headers['Content-Type'], body, args, {})
        file_name = args['file'][0].decode('utf-8')
        reports_path = self.static_conf['static_path'] + 'reports'
        full_path =  os.path.join(reports_path, file_name)
        data = args['data'][0].decode('utf-8')
        data = json.loads(data)
        format_file = file_name.split('.')[1]
        file_res = ''
        if format_file == 'xlsx':
          file_res = self.work_xlsx(full_path,data)

        # print(file_res)


        # wb = openpyxl.load_workbook(full_path)
        # sheet = wb.active
        # print(sheet)

        # with open(full_path, 'r') as f:
            # f.read()
            # print(f.read())
        # for row in f:
        #   print(row)

        # for key in data.keys(): 
        #   print(key)
        # print(f)

        
       
        self.write({'file':file_res,'status': 'success'})

    def work_xlsx(self,path,data):

      wb = openpyxl.load_workbook(path)
      sheet = wb.active
      sheet_name = wb.get_sheet_names()[0]

      # new_wb = openpyxl.Workbook()
      # new_sheet = new_wb.active
      # new_sheet.title = sheet_name
      new_sheet = {}

      
  
      for rownum in range(sheet.max_row):
        for columnnum in range(sheet.max_column):

          cell = sheet.cell(rownum + 1, columnnum + 1).value
          for key in data.keys():
            if cell == '$'+key+'$':
              cell = data[key]
          try:
              new_sheet[str(rownum + 1)]
          except:
              new_sheet[str(rownum + 1)] = {}
          # if not new_sheet[str(rownum + 1)]:
          new_sheet[str(rownum + 1)][str(columnnum + 1)] = cell

      return json.dumps(new_sheet)
      # for rownum in range(new_sheet.max_row):
      #   for columnnum in range(new_sheet.max_column):

      #     cell = new_sheet.cell(rownum + 1, columnnum + 1).value
      #     print(cell)


      # wb = xlrd.open_workbook(path)
      # sheet = wb.sheet_by_index(0)
      # sheet_name = wb.sheet_names()[0]

      # new_wb = xlwt.Workbook()
      # new_sheet = new_wb.add_sheet(sheet_name)

      
      # for rownum in range(sheet.nrows):
      #   row = sheet.row_values(rownum) 

      #   for index, item in enumerate(row):
      #     new_sheet.write(rownum+1, index+1, 'hello kitty')

     
      
      # print(new_wb)
      



