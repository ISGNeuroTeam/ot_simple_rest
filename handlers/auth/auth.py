from datetime import datetime, timedelta
import logging

import bcrypt
import jwt

import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.locks
import tornado.web
import tornado.util

from handlers.auth.db_connector import PostgresConnector

SECRET_KEY = '8b62abb2-bbf6-4e0e-a7c1-2e4734bebbd9'

logger = logging.getLogger('osr')


class NoResultError(Exception):
    pass


class BaseHandler(tornado.web.RequestHandler):
    def initialize(self, db_conn_pool):
        """
        Gets config and init logger.

        :param db_conn_pool: Postgres DB connection pool object.
        :return:
        """
        self.db = PostgresConnector(db_conn_pool)
        self.permissions = list()

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Credentials', True)
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def options(self, *args, **kwargs):
        self.set_status(204)
        self.finish()

    def decode_token(self, token):
        return jwt.decode(token, SECRET_KEY, algorithms='HS256')

    def generate_token(self, payload):
        return jwt.encode(payload, SECRET_KEY, algorithm='HS256')

    async def prepare(self):
        client_token = self.get_cookie('eva_token')
        if client_token:
            try:
                token_data = self.decode_token(client_token)
                user_id = token_data['user_id']
                user_roles = self.db.get_user_roles(user_id)
                self.permissions = self.db.get_user_permissions(user_roles)
            except (jwt.ExpiredSignatureError, jwt.DecodeError):
                pass
            else:
                self.current_user = user_id
        if not self.current_user:
            raise tornado.web.HTTPError(401, "unauthorized")


class AuthCreateHandler(BaseHandler):
    async def post(self):
        if self.db.check_user_exists(self.get_argument("username")):
            raise tornado.web.HTTPError(400, f"author with name '{self.get_argument('username')}' already exists")
        hashed_password = await tornado.ioloop.IOLoop.current().run_in_executor(
            None,
            bcrypt.hashpw,
            tornado.escape.utf8(self.get_argument("password")),
            bcrypt.gensalt(),
        )
        self.db.add_user(
            roles=self.get_arguments('roles'),
            groups=self.get_arguments('groups'),
            username=self.get_argument('username'),
            password=tornado.escape.to_unicode(hashed_password)
        )
        self.write({'status': 'success'})


class AuthLoginHandler(BaseHandler):
    async def post(self):
        user = self.db.check_user_exists(self.get_argument("username"))
        if not user:
            raise tornado.web.HTTPError(400, "incorrect login")

        password_equal = await tornado.ioloop.IOLoop.current().run_in_executor(
            None,
            bcrypt.checkpw,
            tornado.escape.utf8(self.get_argument("password")),
            tornado.escape.utf8(user.password),
        )
        if not password_equal:
            raise tornado.web.HTTPError(400, "incorrect password")

        user_tokens = self.db.get_user_tokens(user.id)
        client_token = self.get_cookie('eva_token')

        if not client_token:
            payload = {'user_id': user.id, 'username': user.username,
                       'exp': int((datetime.now() + timedelta(hours=12)).timestamp())}
            token = self.generate_token(payload)
            expired_date = datetime.now() + timedelta(hours=12)
            self.db.add_session(
                token=token.decode('utf-8'),
                user_id=user.id,
                expired_date=expired_date
            )

            self.current_user = user.id
            self.set_cookie('eva_token', token, expires=expired_date)
            self.write({'status': 'success'})

        elif client_token not in user_tokens:
            raise tornado.web.HTTPError(401, "unauthorized")
        else:
            self.write({'status': 'success'})


class RolesHandler(BaseHandler):
    async def get(self):
        if 'list_roles' in self.permissions or 'admin_all' in self.permissions:
            target_user_id = self.get_argument('id', None)
            if target_user_id:
                roles = self.db.get_roles_data(user_id=target_user_id)
            else:
                roles = self.db.get_roles_data()
        else:
            roles = self.db.get_roles_data(user_id=self.current_user)
        self.write({'roles': roles})

    async def post(self):
        if 'create_roles' in self.permissions or 'admin_all' in self.permissions:
            role_id = self.db.add_role(role_name=self.get_argument('role_name'),
                                       users=self.get_arguments('users'),
                                       permissions=self.get_arguments('permissions'))
        else:
            raise tornado.web.HTTPError(403, "has no permission for create roles")
        self.write({'id': role_id})


class RoleHandler(BaseHandler):
    async def get(self):
        role_id = self.get_argument('id', None)
        if not role_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        if 'read_roles' in self.permissions or 'admin_all' in self.permissions:
            role_data = self.db.get_role_data(role_id)
        else:
            raise tornado.web.HTTPError(403, "has no permission for read roles")
        self.write({'role_data': role_data})

    async def put(self):
        role_id = self.get_argument('id', None)
        if not role_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        if 'manage_roles' in self.permissions or 'admin_all' in self.permissions:
            self.db.update_role(role_id=role_id,
                                users=self.get_arguments('users'),
                                permissions=self.get_arguments('permissions'))
        else:
            raise tornado.web.HTTPError(403, "has no permission for manage roles")
        self.write({'id': role_id})

    async def delete(self):
        role_id = self.get_argument('id', None)
        if not role_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        if 'delete_roles' in self.permissions or 'admin_all' in self.permissions:
            role_id = self.db.delete_role(role_id)
        else:
            raise tornado.web.HTTPError(403, "has no permission for delete roles")
        self.write({'id': role_id})


class UsersHandler(BaseHandler):
    async def get(self):
        logger.info(f'user_id: {self.current_user}')
        if 'list_users' in self.permissions or 'admin_all' in self.permissions:
            users = self.db.get_users_data()
        else:
            users = self.db.get_users_data(user_id=self.current_user)
        self.write({'users': users})

    async def post(self):
        if 'create_users' in self.permissions or 'admin_all' in self.permissions:
            hashed_password = await tornado.ioloop.IOLoop.current().run_in_executor(
                None,
                bcrypt.hashpw,
                tornado.escape.utf8(self.get_argument("password")),
                bcrypt.gensalt(),
            )
            role_id = self.db.add_user(username=self.get_argument('username'),
                                       password=tornado.escape.to_unicode(hashed_password),
                                       roles=self.get_arguments('roles'),
                                       groups=self.get_arguments('groups'))
        else:
            raise tornado.web.HTTPError(403, "has no permission for create roles")
        self.write({'id': role_id})


class UserHandler(BaseHandler):
    async def get(self):
        target_user_id = self.get_argument('id', None)
        if not target_user_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        if 'read_users' in self.permissions or 'admin_all' in self.permissions:
            user_data = self.db.get_users_data(target_user_id)
        else:
            raise tornado.web.HTTPError(403, "has no permission for read users")
        self.write({'user_data': user_data[0]})

    async def put(self):
        target_user_id = self.get_argument('id', None)
        if not target_user_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        if 'manage_roles' in self.permissions or 'admin_all' in self.permissions:
            user_data = self.db.get_users_data(target_user_id)[0]
            old_password = user_data.pop("password")
            old_username = user_data.pop("username")

            new_password = self.get_argument("password", None)
            if new_password:
                password_equal = await tornado.ioloop.IOLoop.current().run_in_executor(
                    None,
                    bcrypt.checkpw,
                    tornado.escape.utf8(new_password),
                    tornado.escape.utf8(old_password),
                )
                new_hashed_password = await tornado.ioloop.IOLoop.current().run_in_executor(
                    None,
                    bcrypt.hashpw,
                    tornado.escape.utf8(new_password),
                    bcrypt.gensalt(),
                )
                new_password = tornado.escape.to_unicode(new_hashed_password) if not password_equal else None

            new_username = self.get_argument('username', None)
            new_username = self.get_argument('username') if new_username != old_username else None
            user_id = self.db.update_user(user_id=target_user_id,
                                          password=new_password,
                                          username=new_username,
                                          roles=self.get_arguments('roles'),
                                          groups=self.get_arguments('groups'))
        else:
            raise tornado.web.HTTPError(403, "has no permission for manage roles")
        self.write({'id': user_id})

    async def delete(self):
        target_user_id = self.get_argument('id', None)
        if not target_user_id:
            raise tornado.web.HTTPError(400, "param 'id' is needed")

        if 'delete_users' in self.permissions or 'admin_all' in self.permissions:
            user_id = self.db.delete_user(target_user_id)
        else:
            raise tornado.web.HTTPError(403, "has no permission for delete roles")
        self.write({'id': user_id})


class GroupsHandler(BaseHandler):
    async def get(self):
        if 'list_groups' in self.permissions or 'admin_all' in self.permissions:
            target_user_id = self.get_argument('id', None)
            if target_user_id:
                groups = self.db.get_groups_data(user_id=target_user_id)
            else:
                groups = self.db.get_groups_data()
        else:
            groups = self.db.get_groups_data(user_id=self.current_user)
        self.write({'groups': groups})

    async def post(self):
        if 'create_groups' in self.permissions or 'admin_all' in self.permissions:
            group_id = self.db.add_group(group_name=self.get_argument('group_name'),
                                         color=self.get_argument('color'),
                                         users=self.get_arguments('users'),
                                         indexes=self.get_arguments('indexes'))
        else:
            raise tornado.web.HTTPError(403, "has no permission for create groups")
        self.write({'id': group_id})


class PermissionsHandler(BaseHandler):
    async def get(self):
        if 'list_permissions' in self.permissions or 'admin_all' in self.permissions:
            permissions = self.db.get_permissions_list()
        else:
            user_roles = self.db.get_user_roles(self.current_user)
            permissions = self.db.get_user_permissions(user_roles)
        self.write({'user_id': self.current_user, 'permissions': permissions})


class GroupHandler(BaseHandler):
    pass
