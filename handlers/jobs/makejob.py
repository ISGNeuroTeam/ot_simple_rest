import re
import logging

import tornado.web
from tornado.ioloop import IOLoop

from parsers.spl_resolver.Resolver import Resolver
from utils import backlasher
from handlers.jobs.db_connector import PostgresConnector

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = ["Anton Khromov"]
__license__ = ""
__version__ = "0.9.2"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


class MakeJob(tornado.web.RequestHandler):
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

    logger = logging.getLogger('osr')

    def initialize(self, db_conf, resolver_conf):
        """
        Gets config and init logger.
        :param resolver_conf: Resolver config.
        :type resolver_conf: Dictionary.
        :param db_conf: DB config.
        :type db_conf: Dictionary.
        :return:
        """

        self.db = PostgresConnector(db_conf)
        self.resolver_conf = resolver_conf

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
            self.logger.debug('Error_msg: %s' % error_msg)
            self.finish(error_msg)

    async def post(self):
        """
        It writes response to remote side.
        :return:
        """

        future = IOLoop.current().run_in_executor(None, self.make_job)
        await future

    @staticmethod
    def validate():
        # TODO
        return True

    def check_cache(self, cache_ttl, original_spl, tws, twf, field_extraction, preview):
        """
        It checks if the same query Job is already finished and it's cache is ready to be downloaded. This way it will
        return it's id for OT.Simple Splunk app JobLoader to download it's cache.

        :param original_spl: Original SPL query.
        :type original_spl: String.
        :param cache_ttl: Time To Life of cache.
        :param tws: Time Window Start.
        :type tws: Integer.
        :param twf: Time Window Finish.
        :type twf: Integer.
        :param field_extraction: Field Extraction mode.
        :type field_extraction: Boolean.
        :param preview: Preview mode.
        :type preview: Boolean.
        :return: Job cache's id and date of creating.
        """
        cache_id = creating_date = None
        self.logger.debug('cache_ttl: %s' % cache_ttl)
        if cache_ttl:
            cache_id, creating_date = self.db.check_cache(original_spl=original_spl, tws=tws, twf=twf,
                                                          field_extraction=field_extraction, preview=preview)

        self.logger.debug('cache_id: %s, creating_date: %s' % (cache_id, creating_date))
        return cache_id, creating_date

    def check_running(self, original_spl, tws, twf, field_extraction, preview):
        """
        It checks if the same query Job is already running. This way it will return id of running job and will not
        register a new one.

        :param original_spl: Original SPL query.
        :type original_spl: String.
        :param tws: Time Window Start.
        :type tws: Integer.
        :param twf: Time Window Finish.
        :type twf: Integer.
        :param field_extraction: Field Extraction mode.
        :type field_extraction: Boolean.
        :param preview: Preview mode.
        :type preview: Boolean.
        :return: Job's id and date of creating.
        """
        job_id, creating_date = self.db.check_running(original_spl=original_spl, tws=tws, twf=twf,
                                                      field_extraction=field_extraction, preview=preview)

        self.logger.debug('job_id: %s, creating_date: %s' % (job_id, creating_date))
        return job_id, creating_date

    def user_has_right(self, username, indexes):
        """
        It checks Role Model if user has access to requested indexes.

        :param username: User from query meta.
        :type username: String.
        :param indexes: Requested indexes parsed from SPL query.
        :type indexes: List.
        :param cur: Cursor to Postgres DB.
        :return: Boolean access flag and resolved indexes.
        """
        accessed_indexes = []
        if indexes:
            user_indexes = self.db.check_user_role(username)
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
            self.logger.debug('User has a right: %s' % access_flag)
        else:
            access_flag = True
        return access_flag, accessed_indexes

    def make_job(self):
        """
        It checks for the same query Jobs and returns id for loading results to OT.Simple Splunk app.

        :return:
        """
        request = self.request.body_arguments
        self.logger.debug('Request: %s' % request)

        # Step 1. Remove OT.Simple Splunk app service data from SPL query.
        original_spl = request['original_spl'][0].decode()
        self.logger.debug("Original spl: %s" % original_spl)
        original_spl = re.sub(r"\|\s*ot\s[^|]*\|", "", original_spl)
        original_spl = re.sub(r"\|\s*simple", "", original_spl)
        original_spl = original_spl.replace("oteval", "eval")
        original_spl = original_spl.strip()
        self.logger.debug('Fixed original_spl: %s' % original_spl)

        # Step 2. Get Role Model information about query and user.
        username = request['username'][0].decode()
        indexes = re.findall(r"index=(\S+)", original_spl)

        # Get search time window.
        tws = int(float(request['tws'][0]))
        twf = int(float(request['twf'][0]))

        # Get cache lifetime.
        cache_ttl = int(request['cache_ttl'][0])

        # Get field extraction mode.
        field_extraction = request['field_extraction'][0]
        field_extraction = True if field_extraction == b'True' else False

        # Get preview mode.
        preview = request['preview'][0]
        preview = True if preview == b'True' else False

        # Update time window to discrete value.
        tws, twf = backlasher.discretize(tws, twf, cache_ttl if cache_ttl else 0)
        self.logger.debug("Discrete time window: [%s,%s]." % (tws, twf))

        sid = request['sid'][0].decode()

        # Step 4. Check for Role Model Access to requested indexes.
        access_flag, indexes = self.user_has_right(username, indexes)
        if access_flag:
            self.logger.debug("User has access. Indexes: %s." % indexes)
            resolver = Resolver(indexes, tws, twf, self.db, sid, self.request.remote_ip,
                                self.resolver_conf.get('no_subsearch_commands'))
            resolved_spl = resolver.resolve(original_spl)
            self.logger.debug("Resolved_spl: %s" % resolved_spl)

            # Step 5. Make searches queue based on subsearches of main query.
            searches = []
            for search in resolved_spl['subsearches'].values():
                if ('otrest' or 'otloadjob') in search[0]:
                    continue
                searches.append(search)

            # Append main search query to the end.
            searches.append(resolved_spl['search'])
            self.logger.debug("Searches: %s" % searches)
            response = {"status": "fail", "error": "No any searches were resolved"}
            for search in searches:

                # Step 6. Check if the same query Job is already calculated and has ready cache.
                cache_id, creating_date = self.check_cache(cache_ttl, search[0], tws, twf, field_extraction, preview)

                if cache_id is None:
                    self.logger.debug('No cache')

                    # Check for validation.
                    if self.validate():

                        # Step 7. Check if the same query Job is already be running.
                        job_id, creating_date = self.check_running(search[0], tws, twf, field_extraction, preview)
                        self.logger.debug('Running job_id: %s, creating_date: %s' % (job_id, creating_date))
                        if job_id is None:

                            # Form the list of subsearches for each search.
                            subsearches = []
                            if 'subsearch=' in search[1]:
                                _subsearches = re.findall(r'subsearch=([\w\d]+)', search[1])
                                for each in _subsearches:
                                    subsearches.append(resolved_spl['subsearches'][each][0])

                            # Step 8. Register new Job in Dispatcher DB.
                            self.logger.debug('Search: %s. Subsearches: %s.' % (search[1], subsearches))
                            job_id, creating_date = self.db.add_job(search=search, subsearches=subsearches,
                                                                    tws=tws, twf=twf, cache_ttl=cache_ttl,
                                                                    username=username,
                                                                    field_extraction=field_extraction,
                                                                    preview=preview)

                            # Add SID to DB if search is not subsearch.
                            if search == searches[-1]:
                                self.db.add_sid(sid=sid, remote_ip=self.request.remote_ip,
                                                original_spl=original_spl)

                        # Return id of new Job.
                        response = {"_time": creating_date, "status": "success", "job_id": job_id}

                    else:
                        # Return validation error.
                        response = {"status": "fail", "error": "Validation failed"}

                else:
                    # Return id of the same already calculated Job with ready cache. Ot.Simple Splunk app JobLoader will
                    # request it to download.
                    response = {"_time": creating_date, "status": "success", "job_id": cache_id}

        else:
            # Return Role Model Access error.
            self.logger.debug("User has no access.")
            response = {"status": "fail", "error": "User has no access to index"}

        self.logger.debug('Response: %s' % response)
        self.write(response)
