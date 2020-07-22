import os
import io
import jwt
import uuid
import json
import docx
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


class PaperLoadHandler(BaseHandler):   # метод отвечающяя за загрузку файл в папку
    def initialize(self, **kwargs):  # инициализируем переменные которые придут при вызове этого метода
        super().initialize(kwargs['db_conn_pool'])
        self.static_conf = kwargs['static_conf']
        self.logger = logging.getLogger('osr')

    async def prepare(self): #  метод без которого не будет работать класс, в нем мы проверяем куку и разрешение на дальнейшую работу
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

    async def post(self): # метод который положит файл в папку
        body = self.request.body # получаем данные с фронта
        args = {}
        files = {}
        tornado.httputil.parse_body_arguments(self.request.headers['Content-Type'], body, args, files) # парсим и получаем данные в нужном нам виде
        reports_path = self.static_conf['static_path'] + 'reports' # путь куда будем сохранять файл
        _file = files['file'][0] # тут из всех переданных данных забираем собственно файл

        saving_full_path = os.path.join(reports_path, _file['filename']) # тут к пути еще добовляем имя файла
        # if not os.path.exists(saving_full_path): # тут не до конца понимаю как работает но в общем проверят есть ли уже такой файл и если нет
        with open(saving_full_path, 'wb') as f: # то открывает его (и создает видимо)
            f.write(_file['body'])  # и записывает в него данные с фронта
        self.write({'status': 'success'}) # передаем успешное выполнение запроса

class PapersHandler(BaseHandler):  # метод возвращающий все файлы в папке на фронт
    def initialize(self, **kwargs):   # инициализируем переменные которые придут при вызове этого метода
        super().initialize(kwargs['db_conn_pool'])
        self.static_conf = kwargs['static_conf']
        self.logger = logging.getLogger('osr')

    async def get(self): # метод который вернет все файлы в папке
      reports_path = self.static_conf['static_path'] + 'reports'  # путь до нужной папки
      files = []  # переменная в которой будет храниться список файлов
      for file in os.listdir(reports_path): # забираем список всех файлов и каталогов из нужной папки
        if os.path.isfile(os.path.join(reports_path, file)): # проверяем если это файл, а не каталог
          files.append(file) # то заносим этот файл в подготовленный массив
      # listOfFiles = [f for f in os.listdir(reports_path) if os.path.isfile(f)]
      # print(files)
      
      self.write({'files':files,'status': 'success'}) # возвращаем список файлов и сообщение что успешно все прошло 


class PaperHandler(BaseHandler): # метод который изменит файл с фротна и вернет ссылку на новый
    def initialize(self, **kwargs): # инициализируем переменные которые придут при вызове этого метода
        super().initialize(kwargs['db_conn_pool'])
        self.static_conf = kwargs['static_conf']
        self.logger = logging.getLogger('osr')
        self.static_dir_name = 'storage'

    async def prepare(self):   # инициализируем переменные которые придут при вызове этого метода
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
        

    async def post(self): # метод который изменит файл на основе данных с фронта и отдаст ссылку на новый
        body = self.request.body # получаем данные с фронта
        args = {}
        tornado.httputil.parse_body_arguments(self.request.headers['Content-Type'], body, args, {}) # парсим и получаем данные в нужном нам виде
        file_name = args['file'][0].decode('utf-8')  # получаем имя нужного файла после раскадировки
        reports_path = self.static_conf['static_path'] + 'reports' # путь до папки откуда брать файлы
        full_path =  os.path.join(reports_path, file_name)  # полный путь уже с именем нужного файла
        data = args['data'][0].decode('utf-8')  # декадируем данные пришедшие с фронта 
        data = json.loads(data) # и превращаем json в словарь
        about_file = file_name.split('.') # получаем массив в котором первое значение имя файла, а второе его разрешение
        file_res = ''  # результат в виде ссылки на измененный файл
        if about_file[1] == 'xlsx': # если файл с разрешением xlsx 
          file_res = self.work_xlsx(full_path,data,about_file[0]) # то вызываем метод который обработает xlsx
        else:
          file_res = self.work_docx(full_path,data,about_file[0]) # то вызываем метод который обработает docx
        

            
        self.write({'file':file_res,'status': 'success'}) # вернем ссылку на новый обработанный файл и сообщение что все успешно прошло

    def work_docx(self,path,data,name_file): # метод для работы с xlsx файлами
      result = ''
      files= []
      reports_path = self.static_conf['static_path'] + 'reports/changed'  # задаем правлиьный путь для измененных файлов

      for i, part_data in enumerate(data):

        doc = docx.Document(path)

        for paragraph in doc.paragraphs:  
          for key in part_data.keys(): # пробегаемся по словарю данных с фронта
            if  paragraph.text.find('$'+key+'$') != -1: # а затем проверяем есть ли в этой ячейке ключ словаря
              paragraph.text = paragraph.text.replace('$'+key+'$', part_data[key])

        
        filename = name_file+'-changed-'+str(i)+'.docx'
        files.append(filename) # уже полный путь с названием файла
        doc.save(os.path.join(reports_path, filename))  # сохраняем измененный файл в папку
      

      if len(files) > 1: #  если у нас несколько файлов

       
        with tempfile.TemporaryDirectory() as directory: # создаем временную папку

          archive_path = os.path.join(directory, name_file+'_changed_archive.tar')  # задаем путь до архива во временной папке
          archive = tarfile.open(archive_path, mode='x:gz')  # открываем архив
  
          for name in files:  # пробегаемся по всем файлам
            os.rename(os.path.join(reports_path, name), os.path.join(directory, name)) # перемещаем созданные файлы во временную папку

          archive.add(directory, name_file+'-changed') # добовляем их в архив

          archive.close() # закрываем архив
          os.rename(os.path.join(directory, name_file+'_changed_archive.tar'), os.path.join(reports_path, name_file+'_changed_archive.tar')) # переносим архив в папку с изменнными файлами


        result = 'reports/changed/'+name_file+'_changed_archive.tar' # задаем путь до архива

      else:  # если файл только один
        result = 'reports/changed/'+name_file+'-changed'+'.docx' # просто указываем путь до архива

      return result # возвращаем ссылку на измененный файл 

    def work_xlsx(self,path,data,name_file): # метод для работы с xlsx файлами

      files = []
      reports_path = self.static_conf['static_path'] + 'reports/changed'  # задаем правлиьный путь для измененных файлов
      result = ''

      for i, part_data in enumerate(data):

        wb = openpyxl.load_workbook(path) # открываем файл
        sheet = wb.active  # выбираем активный лист
        sheet_name = wb.get_sheet_names()[0] # здесь запоминаме имя этого активного листа
  
        for rownum in range(sheet.max_row): # пробегаемся по всем строкам 
          for columnnum in range(sheet.max_column): #  и в каждой строке по всем столбцам
            cell = sheet.cell(rownum + 1, columnnum + 1).value #  запоминаем занчение в текущей ячейки
            for key in part_data.keys(): # пробегаемся по словарю данных с фронта
              if cell is not None and type(cell) is str: # првоеряем не пустая ли ячейка и что ячейка строка 
                if  cell.find('$'+key+'$') != -1: # а затем проверяем есть ли в этой ячейке ключ словаря
                  cell = cell.replace('$'+key+'$', part_data[key])  # то заменяем значение ячейке на значение из данных
                  sheet.cell(rownum + 1, columnnum + 1).value = cell # Записываем измененую ячейку в файл
              
        filename = name_file+'-changed-'+str(i)+'.xlsx'
        files.append(filename) # уже полный путь с названием файла
        wb.save(os.path.join(reports_path, filename)) # сохраняем измененный файл в папку

      if len(files) > 1: #  если у нас несколько файлов

       
        with tempfile.TemporaryDirectory() as directory: # создаем временную папку

          archive_path = os.path.join(directory, name_file+'_changed_archive.tar')  # задаем путь до архива во временной папке
          archive = tarfile.open(archive_path, mode='x:gz')  # открываем архив
  
          for name in files:  # пробегаемся по всем файлам
            os.rename(os.path.join(reports_path, name), os.path.join(directory, name)) # перемещаем созданные файлы во временную папку

          archive.add(directory, name_file+'-changed') # добовляем их в архив

          archive.close() # закрываем архив
          os.rename(os.path.join(directory, name_file+'_changed_archive.tar'), os.path.join(reports_path, name_file+'_changed_archive.tar')) # переносим архив в папку с изменнными файлами


        result = 'reports/changed/'+name_file+'_changed_archive.tar' # задаем путь до архива

      else:  # если файл только один
        result = 'reports/changed/'+name_file+'-changed'+'.xlsx' # просто указываем путь до архива

  

      return result # возвращаем ссылку на измененный файл 


      



