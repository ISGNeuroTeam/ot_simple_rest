#!/usr/bin/env python
# -*- coding: utf-8 -*-
# resolver.py
import re
import json
import logging
import regex as pcre
from hashlib import sha256
from base64 import standard_b64encode as b64encode
from parsers.otl_resolver.macros import Macros
from parsers.otl_to_sparksql.otl_parser import OTLtoSQL


__author__ = ["Andrey Starchenkov", "Anton Khromov"]
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = ["Sergei Ermilov", "Anastasiya Safonova"]
__license__ = ""
__version__ = "0.4.0"
__maintainer__ = "Andrey Starchenkov"
__email__ = "akhromov@ot.ru"
__status__ = "Production"

pcre.DEFAULT_VERSION = pcre.VERSION1


class Resolver:
    """
    Gets service OTL string from original one. Transforms next commands:

    1. search -> | read "{__fts_json__}"
    2. | otrest endpoint=/any/path/to/api/ -> | otrest subsearch=subsearch_id
    3. any_command [subsearch] -> any_command subsearch=subsearch_id

    This is needed for calculation part of Dispatcher.
    """

    logger = logging.getLogger('osr')

    # Patterns for transformation.
    # quoted_hide_pattern - the following pattern checks json-like string type,
    # extended with ability to pass both `'` and `"` as quotes for the string.
    # Left side of the pattern saves the quote, middle part checks escaping and finishes with copying the quote
    # WARNING: REGEX engine that fully supports PCRE standard is REQUIRED to make this possible
    quoted_hide_pattern = pcre.compile(r'(?<!\\)(?<q>[\'"])((?:(?!(?<!\\)\g<q>).)*)(?<!\\)\g<q>')
    quoted_return_pattern = r'_quoted_text_(\w+)'
    no_subsearch_return_pattern = r'_hidden_text_(\w+)'
    subsearch_pattern = r'.+\[(.+?)\]'
    read_pattern_middle = r'\[\s*search ([^|\]]+)'
    read_pattern_start = r'^ *search ([^|]+)'
    otstats_pattern_start = r'\|?\s*otstats ([^|]+)'
    otstats_pattern_middle = r'\[\s*\|\s*otstats ([^|\]]+)'
    otrest_pattern = r'otrest[^|]+url\s*?=\s*?([^\|\] ]+)'
    filter_pattern = r'\|\s*search ([^\|]+)'
    otinputlookup_where_pattern = r'otinputlookup([^\|$]+)where\s+([^\|$]+)'
    otfrom_pattern = r'otfrom datamodel:?\s*([^\|$]+)'
    otloadjob_id_pattern = r'otloadjob\s+(\d+\.\d+)'
    otloadjob_otl_pattern = r'otloadjob\s+otl=\"(.+?[^\\])\"(\s+?___token___=\"(.+?[^\\])\")?(\s+?___tail___=\"(.+?[^\\])\")?'
    scala_inline_pattern = r'scala\s+<#(.*?)#>'
    spark_inline_pattern = r'spark\s+<#(.*?)#>'

    def __init__(self, indexes, tws, twf, db=None, sid=None, src_ip=None, no_subsearch_commands=None, macros_dir=None):
        """
        Init with default available indexes, time window and cursor to DB for DataModels.

        :param indexes: list of default available indexes.
        :param tws: Time Window Start.
        :param twf: Time Window Finish.
        :param db: DB connector.
        """
        self.indexes = indexes
        self.tws = tws
        self.twf = twf
        self.db = db
        self.sid = sid
        self.src_ip = src_ip
        self.no_subsearch_commands = no_subsearch_commands

        self.subsearches = {}
        self.hidden_rex = {}
        self.hidden_quoted_text = {}
        self.hidden_no_subsearches = {}
        self.macros_dir = macros_dir

    def create_subsearch(self, match_object):
        """
        Finds subsearches and transforms original OTL with subsearch id.
        any_command [subsearch] -> any_command subsearch=subsearch_id

        :param match_object: Re match object with original OTL.
        :return: String with replaces of subsearches.
        """
        subsearch_query = match_object.group(1)

        subsearch_query_service = re.sub(self.read_pattern_middle, self.create_read_graph, subsearch_query)
        subsearch_query_service = re.sub(self.read_pattern_start, self.create_read_graph, subsearch_query_service)

        subsearch_query_service = re.sub(self.otstats_pattern_middle, self.create_otstats_graph,
                                         subsearch_query_service)
        subsearch_query_service = re.sub(self.otstats_pattern_start, self.create_otstats_graph, subsearch_query_service)

        subsearch_query_service = re.sub(self.filter_pattern, self.create_filter_graph, subsearch_query_service)

        _subsearch_query = re.sub(self.quoted_return_pattern, self.return_quoted, subsearch_query)
        _subsearch_query_service = re.sub(self.quoted_return_pattern, self.return_quoted, subsearch_query_service)

        subsearch_sha256 = sha256(_subsearch_query.strip().encode('utf-8')).hexdigest()

        self.subsearches[f'subsearch_{subsearch_sha256}'] = (_subsearch_query, _subsearch_query_service)
        return match_object.group(0).replace(f'[{subsearch_query}]', f'subsearch=subsearch_{subsearch_sha256}')

    def create_otrest(self, match_object):
        """
        Finds "| otrest endpoint=/any/path/to/api/" command and transforms it to service form.
        | otrest endpoint=/any/path/to/api/-> | otrest subsearch=subsearch_id

        :param match_object: Re match object with original OTL.
        :return: String with replaces of subsearches.
        """
        otrest_sha256 = sha256(match_object.group(0).strip().encode('utf-8')).hexdigest()
        otrest_service = f'| otrest subsearch=subsearch_{otrest_sha256}'
        self.subsearches[f'subsearch_{otrest_sha256}'] = ('| {}'.format(match_object.group(0)), otrest_service)
        return otrest_service

    @staticmethod
    def hide_subsearch_before_read(query):

        subsearch = re.findall(r' subsearch=subsearch_\w+', query)
        if subsearch:
            subsearch = subsearch[0]
        else:
            subsearch = ''

        query = query.replace(subsearch, "")
        return query, subsearch

    def create_read_graph(self, match_object):
        """
        Finds "search __fts_query__" and transforms it to service form.
        search -> | read "{__fts_json__}"

        :param match_object: Re match object with original OTL.
        :return: String with replaces of FTS part.
        """
        query = match_object.group(1)

        query, subsearch = self.hide_subsearch_before_read(query)
        self.logger.debug(f"Whole Query: {match_object.group(0)}. Query: {query}. Indexes: {self.indexes}.")
        graph = OTLtoSQL.parse_read(query, av_indexes=self.indexes, tws=self.tws, twf=self.twf)
        return f'| read {json.dumps(graph)}{subsearch}'

    def create_otstats_graph(self, match_object):
        """
        Finds "otstats __fts_query__" and transforms it to service form.
        search -> | otstats "{__fts_json__}"

        :param match_object: Re match object with original OTL.
        :return: String with replaces of FTS part.
        """
        query = match_object.group(1)

        query, subsearch = self.hide_subsearch_before_read(query)
        self.logger.debug(f"Whole Query: {match_object.group(0)}. Query: {query}. Indexes: {self.indexes}.")
        graph = OTLtoSQL.parse_read(query, av_indexes=self.indexes, tws=self.tws, twf=self.twf)
        return f'| otstats {json.dumps(graph)}{subsearch}'

    @staticmethod
    def create_filter_graph(match_object):
        """
        Finds "| search __filter_query__" and transforms it to service form.
        | search -> | filter "{__filter_json__}"

        :param match_object: Re match object with original OTL.
        :return: String with replaces of filter part.

        """
        query = match_object.group(1)
        graph = OTLtoSQL.parse_filter(query)
        return f'| filter {json.dumps(graph)}'

    @staticmethod
    def create_inputlookup_filter(match_object):
        """
        Finds "| search __filter_query__" and transforms it to service form.
        | search -> | filter "{__filter_json__}"

        :param match_object: Re match object with original OTL.
        :return: String with replaces of filter part.
        """
        query = match_object.group(2)
        graph = OTLtoSQL.parse_filter(query)
        return f'otinputlookup{match_object.group(1)}where {json.dumps(graph)}'

    def create_datamodels(self, match_object):
        """
        Transforms "| otfrom datamodel __NAME__" to "| search (index=__INDEX__ source=__SOURCE__)" or something like.

        :param match_object: Re match object with original OTL.
        :return: String with replaces of datamodel part.
        """
        datamodel_name = match_object.group(1)
        fetch = self.db.get_datamodel(datamodel_name)
        if fetch:
            query = fetch[0]
        else:
            raise Exception('Can\'t find DATAMODEL. Update DATAMODELS DB or fix the name.')
        return query[1:] if query[0] == '|' else query

    def create_otloadjob_id(self, match_object):
        """
        Transforms "| otloadjob __SID__" to "| otloadjob subsearch="subsearch___sha256__" ".

        :param match_object: Re match object with original OTL.
        :return: String with replaces of datamodel part.
        """
        sid = match_object.group(1)
        fetch = self.db.get_otl(sid, self.src_ip)
        if fetch:
            otl = fetch[0]
            otloadjob_sha256 = sha256(otl.strip().encode('utf-8')).hexdigest()
            otloadjob_service = f'| otloadjob subsearch=subsearch_{otloadjob_sha256}'
            self.subsearches[f'subsearch_{otloadjob_sha256}'] = (otl, otloadjob_service)
            return otloadjob_service
        else:
            raise Exception('Job sid is not found.')

    def create_otloadjob_otl(self, match_object):
        """
        Transforms '| otloadjob otl="__OTL__" ___token___="__TOKEN__" ___tail___="__OTL__"' to '| otloadjob subsearch="subsearch___sha256__"'.

        :param match_object: Re match object with original OTL.
        :return: String with replaces of datamodel part.
        """
        otl = match_object.group(1)
        token = match_object.group(3)
        tail = match_object.group(5)
        self.logger.debug(f'OTL: {otl}.')
        self.logger.debug(f'Token: {token}.')
        self.logger.debug(f'Tail: {tail}.')

        otl = otl.replace('\\"', '"')
        if token is None:
            token = ''
        else:
            token = token.replace('\\"', '"')
        if tail is None:
            tail = ''
        else:
            tail = tail.replace('\\"', '"')

        self.logger.debug(f'Unescaped OTL: {otl}.')
        self.logger.debug(f'Unescaped Token: {token}.')
        self.logger.debug(f'Unescaped Tail: {tail}.')

        otl = otl + token + tail
        otl = otl.strip()
        self.logger.debug(f'Concatenated OTL for subsearch: {otl}.')

        otloadjob_sha256 = sha256(otl.strip().encode('utf-8')).hexdigest()
        otloadjob_service = f'otloadjob subsearch=subsearch_{otloadjob_sha256}'
        _otloadjob_service = self.resolve(otl)
        self.subsearches[f'subsearch_{otloadjob_sha256}'] = (otl, _otloadjob_service['search'][1])
        return otloadjob_service

    def hide_quoted(self, match_object):

        quoted_text = match_object.group(2)
        quoted_text_sha256 = sha256(quoted_text.encode('utf-8')).hexdigest()
        self.hidden_quoted_text[quoted_text_sha256] = quoted_text

        return match_object.group(0).replace(quoted_text, f'_quoted_text_{quoted_text_sha256}')

    def return_quoted(self, match_object):
        quoted_text_sha256 = match_object.group(1)
        return match_object.group(0).replace(
            f'_quoted_text_{quoted_text_sha256}',
            self.hidden_quoted_text[quoted_text_sha256]
        )

    def _hide_no_subsearch_command(self, match_object):
        hidden_text = match_object.group(1)
        hidden_text_sha256 = sha256(hidden_text.encode('utf-8')).hexdigest()
        self.hidden_no_subsearches[hidden_text_sha256] = hidden_text

        return match_object.group(0).replace(hidden_text, f'_hidden_text_{hidden_text_sha256}')

    def _return_no_subsearch_command(self, match_object):
        hidden_text_sha256 = match_object.group(1)
        return match_object.group(0).replace(
            f'_hidden_text_{hidden_text_sha256}',
            self.hidden_no_subsearches[hidden_text_sha256]
        )

    def hide_no_subsearch_commands(self, otl):
        if self.no_subsearch_commands is not None:
            commands = self.no_subsearch_commands.split(',')
            raw_str = r'\|\s+{command}[^\[]+(\[.+\])'
            patterns = [re.compile(raw_str.format(command=command)) for command in commands]
            self.logger.debug(f'Patterns: {patterns}.')
            for pattern in patterns:
                otl = pattern.sub(self._hide_no_subsearch_command, otl)
        return otl

    def transform_macros(self, match_object):
        macros_name = match_object.group('macros_name')
        macros_body = match_object.group('macros_body')
        macros = Macros(macros_name, macros_body, self.macros_dir)
        otl = macros.otl.replace('\n', ' ')
        return otl

    def return_no_subsearch_commands(self, otl):
        otl = re.sub(self.no_subsearch_return_pattern, self._return_no_subsearch_command, otl)
        otl = re.sub(self.quoted_return_pattern, self.return_quoted, otl)
        return otl

    def scala_inline_transformer(self, match_object):
        return f"scala \"{b64encode(match_object.group(1).encode('UTF-8')).decode()}\""

    def spark_inline_transformer(self, match_object):
        return f"spark \"{b64encode(match_object.group(1).encode('UTF-8')).decode()}\""

    def resolve(self, otl):
        """
        Finds and replaces service patterns of original OTL.

        :param otl: original OTL.
        :return: dict with search query params.

        >>> Resolver(['testlookup.csv'], 0, 0).resolve("| inputlookup testlookup.csv | search ORG=4 | eval a=2")
        {'search': ('| inputlookup testlookup.csv | search ORG=4 | eval a=2', '| inputlookup testlookup.csv | filter {"query": "ORG=\\\\"4\\\\"", "fields": ["ORG"]}| eval a=2'), 'subsearches': {}}
        >>> Resolver(['testlookup.csv'], 0, 0).resolve("| inputlookup testlookup.csv | search ABC=4 | eval a=2")
        {'search': ('| inputlookup testlookup.csv | search ABC=4 | eval a=2', '| inputlookup testlookup.csv | filter {"query": "ABC=\\\\"4\\\\"", "fields": ["ABC"]}| eval a=2'), 'subsearches': {}}
        """
        _otl = re.sub(Macros.macros_pattern, self.transform_macros, otl)

        _otl = re.sub(self.otloadjob_otl_pattern, self.create_otloadjob_otl, _otl)
        _otl = pcre.sub(self.quoted_hide_pattern, self.hide_quoted, _otl)
        _otl = self.hide_no_subsearch_commands(_otl)
        _otl = (_otl, 1)
        while _otl[1]:
            _otl = re.subn(self.subsearch_pattern, self.create_subsearch, _otl[0])
        self.logger.debug(f"Transformed after 'create_subsearch': {_otl}")

        _otl = re.sub(self.quoted_return_pattern, self.return_quoted, _otl[0])
        self.logger.debug(f"Transformed after 'return_quoted': {_otl}")
        _otl = self.return_no_subsearch_commands(_otl)
        self.logger.debug(f"Transformed after func 'return_no_subsearch_commands': {_otl}")

        _otl = re.sub(self.otfrom_pattern, self.create_datamodels, _otl)
        self.logger.debug(f"Transformed after 'create_datamodels': {_otl}")

        _otl = re.sub(self.read_pattern_middle, self.create_read_graph, _otl, flags=re.I)
        _otl = re.sub(self.read_pattern_start, self.create_read_graph, _otl, flags=re.I)
        self.logger.debug(f"Transformed after 'create_read_graph': {_otl}")
        _otl = re.sub(self.otstats_pattern_middle, self.create_otstats_graph, _otl, flags=re.I)
        _otl = re.sub(self.otstats_pattern_start, self.create_otstats_graph, _otl, flags=re.I)
        self.logger.debug(f"Transformed after 'create_otstats_graph': {_otl}")

        _otl = re.sub(self.otrest_pattern, self.create_otrest, _otl)
        self.logger.debug(f"Transformed after 'create_otrest': {_otl}")
        _otl = re.sub(self.filter_pattern, self.create_filter_graph, _otl, flags=re.I)
        self.logger.debug(f"Transformed after 'create_filter_graph': {_otl}")
        _otl = re.sub(self.otinputlookup_where_pattern, self.create_inputlookup_filter, _otl)
        self.logger.debug(f"Transformed after 'create_inputlookup_filter': {_otl}")
        _otl = re.sub(self.otloadjob_id_pattern, self.create_otloadjob_id, _otl)
        self.logger.debug(f"Transformed after 'create_otloadjob_id': {_otl}")
        _otl = re.sub(self.scala_inline_pattern, self.scala_inline_transformer, _otl, flags=re.S)
        self.logger.debug(f"Transformed after 'scala_inline_transformer': {_otl}")
        _otl = re.sub(self.spark_inline_pattern, self.spark_inline_transformer, _otl, flags=re.S)
        self.logger.debug(f"Transformed after 'spark_inline_transformer': {_otl}")
        return {'search': (otl, _otl), 'subsearches': self.subsearches}
