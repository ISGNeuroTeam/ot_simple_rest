#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ot_simple_rest.py

import logging


import tornado.ioloop
import tornado.web

from handlers.loadjob import LoadJob
from handlers.makejob import MakeJob
from handlers.makerolemodel import MakeRoleModel


def set_logger(loglevel, logfile, logger_name):

    levels = {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG
        }

    logging.basicConfig(
        filename=logfile,
        level=levels[loglevel],
        format="%(asctime)s %(levelname)-s PID=%(process)d %(module)s:%(lineno)d \
func=%(funcName)s - %(message)s")

    logger = logging.getLogger(logger_name)
    return logger


def main():

    logger = set_logger('INFO', './otsimplerest.log', 'osr')

    db_conf = {
        "host": "c3000-blade2.corp.ot.ru",
        "database": "SuperVisor",
        "user": "postgres",
        # "async": True
    }

    ignite_conf = {
        "nodes": [
            # ("172.25.12.70", 10800)
            ("172.25.12.31", 10800),
            ("172.25.12.32", 10800),
            ("172.25.12.33", 10800)
        ]
    }

    logger.info('DB configuration: %s' % db_conf)
    logger.info('Ignite configuration: %s' % ignite_conf)

    application = tornado.web.Application([
        (r'/makejob', MakeJob, {"db_conf": db_conf}),
        (r'/loadjob', LoadJob, {"db_conf": db_conf, "ignite_conf": ignite_conf}),
        (r'/makerolemodel', MakeRoleModel, {"db_conf": db_conf})
    ])

    logger.info('Starting server')

    application.listen(50000)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()
