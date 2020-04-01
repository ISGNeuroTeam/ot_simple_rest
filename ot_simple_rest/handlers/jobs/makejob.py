import logging
import re

import tornado.web
import jwt

from handlers.eva.base import BaseHandler

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = ["Anton Khromov"]
__license__ = ""
__version__ = "0.11.2"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class MakeJob(BaseHandler):
    """
    This handler is the beginning of a long way of each SPL/OTL search query in OT.Simple Platform.
    The algorithm of search query's becoming to Dispatcher's Job is next:

    1. Remove OT.Simple Splunk app service data from SPL query.
    2. Get Role Model information about query and user.
    3. Get service OTL form of query from original SPL.
    4. Check for Role Model Access to requested indexes.
    5. Make searches queue based on subsearches of main query.
    6. Check if the same (original_spl, tws, twf) query Job is already calculated and has ready cache.
    7. Check if the same query Job is already be running.
    8. Register new Job in Dispatcher DB.
    """

    def initialize(self, **kwargs):
        """
        Gets config and init logger.

        :param manager: Jobs manager object
        :return:
        """
        super().initialize(kwargs['db_conn_pool'])
        self.user_conf = kwargs['user_conf']
        self.check_index_access = False if self.user_conf['check_index_access'] == 'False' else True
        self.jobs_manager = kwargs['manager']
        self.logger = logging.getLogger('osr')

    def prepare(self):
        client_token = self.get_cookie('eva_token')
        if client_token:
            self.token = client_token
            try:
                token_data = self.decode_token(client_token)
                user_id = token_data['user_id']
            except (jwt.ExpiredSignatureError, jwt.DecodeError):
                pass
            else:
                self.current_user = user_id

        if not self.current_user:
            raise tornado.web.HTTPError(401, "unauthorized")

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
            self.logger.debug(f'Error_msg: {error_msg}')
            self.finish(error_msg)

    def get_original_spl(self):
        request = self.request.arguments
        original_spl = request["original_spl"][0].decode()
        original_spl = re.sub(r"\|\s*ot\s[^|]*\|", "", original_spl)
        original_spl = re.sub(r"\|\s*simple[^\"]*", "", original_spl)
        original_spl = original_spl.replace("oteval", "eval")
        original_spl = original_spl.strip()
        return original_spl

    def user_has_right(self, indexes):
        """
        It checks Role Model if user has access to requested indexes.

        :param indexes: Requested indexes parsed from SPL query.
        :type indexes: List.
        :return: Boolean access flag and resolved indexes.
        """
        if not indexes:
            return True, indexes

        accessed_indexes = []
        user_indexes = self.db.get_indexes_data(user_id=self.current_user,
                                                names_only=True)
        access_flag = False
        if user_indexes:
            if '*' in user_indexes:
                access_flag = True
            else:
                for index in indexes:
                    index = index.replace('"', '').replace('\\', '')
                    for _index in user_indexes:
                        indexes_from_rm = re.findall(index.replace("*", ".*"), _index)
                        self.logger.debug("Indexes from rm: %s. Left index: %s. Right index: %s." % (
                            indexes_from_rm, index, _index
                        ))
                        for ifrm in indexes_from_rm:
                            accessed_indexes.append(ifrm)
            if accessed_indexes:
                access_flag = True
        self.logger.debug(f'User has a right: {access_flag}')

        return access_flag, accessed_indexes

    async def post(self):
        """
        It writes response to remote side.

        :return:
        """
        original_spl = self.get_original_spl()
        indexes = re.findall(r"index=(\S+)", original_spl)
        access_flag, indexes = self.user_has_right(indexes)
        if not access_flag:
            return self.write({"status": "fail", "error": "User has no access to index"})

        self.logger.debug(f'User has access. Indexes: {indexes}.')
        response = await self.jobs_manager.make_job(request=self.request,
                                                    indexes=indexes)
        self.logger.debug(f'MakeJob RESPONSE: {response}')
        self.write(response)

