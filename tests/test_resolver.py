import unittest

import logging
import parsers.otl_resolver.Resolver as Resolver
from utils.primitives import EverythingEqual


class TestResolver(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        self.resolver = Resolver.Resolver([EverythingEqual(), 'main', 'main1', 'main2'], 0, 0, no_subsearch_commands='foreach,appendpipe', macros_dir='./tests/macros/')
        logging.basicConfig(
            level='DEBUG',
            format="%(asctime)s %(levelname)-s PID=%(process)d %(module)s:%(lineno)d \
    func=%(funcName)s - %(message)s")

    def test_read_some_empty(self):
        otl = """search index=main2 SUCCESS host="h1 bla" OR host="" OR host=h3"""
        target = {'search': ('search index=main2 SUCCESS host="h1 bla" OR host="" OR host=h3',
                             '| read {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1 bla\\" OR host=\\"\\" OR host=\\"h3\\"", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_some_or(self):
        otl = """search index=main2 SUCCESS host="h1" OR host="h2" OR host=h3"""
        target = {'search': ('search index=main2 SUCCESS host="h1" OR host="h2" OR host=h3',
                             '| read {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1\\" OR host=\\"h2\\" OR host=\\"h3\\"", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_many_or(self):
        otl = """search index=main2 SUCCESS host="h1" OR host="h2" OR host=h3 OR host=h4 OR host=h5 OR host=h6 OR host=h7 OR host=h8 OR host=h9 OR host=h10"""
        target = {'search': (
            'search index=main2 SUCCESS host="h1" OR host="h2" OR host=h3 OR host=h4 OR host=h5 OR host=h6 OR host=h7 OR host=h8 OR host=h9 OR host=h10',
            '| read {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1\\" OR host=\\"h2\\" OR host=\\"h3\\" OR host=\\"h4\\" OR host=\\"h5\\" OR host=\\"h6\\" OR host=\\"h7\\" OR host=\\"h8\\" OR host=\\"h9\\" OR host=\\"h10\\"", "tws": 0, "twf": 0}}'),
            'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_rex(self):
        otl = """search index=main2 SUCCESS | rex field=host "^(?<host>[^\.\:]+).*\:[0-9]" | stats count by host"""
        target = {'search': (
            'search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host',
            '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host'),
            'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearch_general(self):
        otl = """search index=main1 FAIL | join host [search index=main2 SUCCESS | stats count by host]"""
        target = {'search': ('search index=main1 FAIL | join host [search index=main2 SUCCESS | stats count by host]',
                             '| read {"main1": {"query": "(_raw like \'%FAIL%\')", "tws": 0, "twf": 0}}| join host subsearch=subsearch_1ae3636e1888e78d8be6893c1e7d569b9289d145bdc8e5d33cdc50aa5bf097e7'),
                  'subsearches': {'subsearch_1ae3636e1888e78d8be6893c1e7d569b9289d145bdc8e5d33cdc50aa5bf097e7': (
                      'search index=main2 SUCCESS | stats count by host',
                      '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| stats count by host')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearch_with_rex(self):
        otl = """search index=main FAIL | join host [search index=main2 SUCCESS | rex field=host "^(?<host>[^\.\:]+).*\:[0-9]" | stats count by host]"""
        target = {'search': (
            'search index=main FAIL | join host [search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host]',
            '| read {"main": {"query": "(_raw like \'%FAIL%\')", "tws": 0, "twf": 0}}| join host subsearch=subsearch_44519941c88171a3bdcf5ffe6ae363eadd3e2969d0209252490e76933ed2af26'),
            'subsearches': {'subsearch_44519941c88171a3bdcf5ffe6ae363eadd3e2969d0209252490e76933ed2af26': (
                'search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host',
                '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_susbearch_with_return(self):
        otl = """search index=main [search index=main2 | return random_Field]"""
        target = {'search': ('search index=main [search index=main2 | return random_Field]',
                             '| read {"main": {"query": "", "tws": 0, "twf": 0}} subsearch=subsearch_f185051077b589c430cff82cd0156cc3da0e2399b8189a21fe2bd626eeb0467a'),
                  'subsearches': {'subsearch_f185051077b589c430cff82cd0156cc3da0e2399b8189a21fe2bd626eeb0467a': (
                      'search index=main2 | return random_Field',
                      '| read {"main2": {"query": "", "tws": 0, "twf": 0}}| return random_Field')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearch_with_otoutputlookup(self):
        otl = """search index=main | otoutputlookup testoutputlookup.csv"""
        target = {'search': ('search index=main | otoutputlookup testoutputlookup.csv',
                             '| read {"main": {"query": "", "tws": 0, "twf": 0}}| otoutputlookup testoutputlookup.csv'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearch_with_append(self):
        otl = """search index=main | table _time, serialField, random_Field, WordField, junkField| append [search index=main junkField="word"]"""
        target = {'search': (
            'search index=main | table _time, serialField, random_Field, WordField, junkField| append [search index=main junkField="word"]',
            '| read {"main": {"query": "", "tws": 0, "twf": 0}}| table _time, serialField, random_Field, WordField, junkField| append subsearch=subsearch_33bbbb794d2b8f066c0479361a5caeb24e3553ebdbccfe2aaf278c158b7fdabb'),
            'subsearches': {'subsearch_33bbbb794d2b8f066c0479361a5caeb24e3553ebdbccfe2aaf278c158b7fdabb': (
                'search index=main junkField="word"',
                '| read {"main": {"query": "junkField=\\"word\\"", "tws": 0, "twf": 0}}')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_several_indexes(self):
        otl = """search index=main1 OR index=main2 SUCCESS"""
        target = {'search': ('search index=main1 OR index=main2 SUCCESS',
                             '| read {"main1": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_read_indexes_with_wildcards(self):
        otl = """search index=main* SUCCESS"""
        target = {'search': ('search index=main* SUCCESS',
                             '| read {"main": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main1": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_read_asterisk_instead_of_indexes(self):
        otl = """search index=* SUCCESS"""
        target = {'search': ('search index=* SUCCESS',
                             '| read {"main": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main1": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_filter_some(self):
        otl = """search index=main2 SUCCESS | search host="h1 bla" OR host="" OR host=h3"""
        target = {'search': ('search index=main2 SUCCESS | search host="h1 bla" OR host="" OR host=h3',
                             '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| filter {"query": "host=\\"h1 bla\\" OR host=\\"\\" OR host=\\"h3\\"", "fields": ["host"]}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_filter_fts(self):
        otl = """search index=main2 SUCCESS | search raw_search """
        target = {'search': ('search index=main2 SUCCESS | search raw_search ',
                             '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| filter {"query": "(_raw like \'%raw_search%\')", "fields": []}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_filter_fts_escaped(self):
        otl = """search index=main2 SUCCESS | search "raw search" """
        target = {'search': ('search index=main2 SUCCESS | search "raw search" ',
                             '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| filter {"query": "(_raw rlike \'raw search\')", "fields": []}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_some_and(self):
        otl = """search index=main2 SUCCESS, FAIL field1=h3, field2="h4", field3="zxc, 123" """
        target = {'search': ('search index=main2 SUCCESS, FAIL field1=h3, field2="h4", field3="zxc, 123" ',
                             '| read {"main2": {"query": "(_raw like \'%SUCCESS%\') AND (_raw like \'%FAIL%\') AND field1=\\"h3\\" AND field2=\\"h4\\" AND field3=\\"zxc, 123\\"", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_several_indexes_with_quotes(self):
        otl = """search index="main1" OR index=main2 SUCCESS"""
        target = {'search': ('search index="main1" OR index=main2 SUCCESS',
                             '| read {"main1": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_otloadjob_with_token(self):
        otl = """| ot ttl=60 | otloadjob otl="| ot ttl=60 | search index=\\"pprbepk_business\\" "  ___token___="host=\\"marica20:1111\\" OR host=\\"marica21:1111\\" OR host=\\"marica22:1111\\" OR host=\\"marica23:1111\\" OR host=\\"marica24:1111\\" OR host=\\"marica25:1111\\" OR host=\\"marica26:1111\\" OR host=\\"marica27:1111\\" OR host=\\"marica28:1111\\" OR host=\\"marica29:1111\\" OR host=\\"marica30:1111\\" OR host=\\"marica31:1111\\" OR host=\\"marica32:1111\\" OR host=\\"marica33:1111\\" OR host=\\"marica34:1111\\" OR host=\\"marica35:1111\\" OR host=\\"marica36:1111\\" OR host=\\"marica37:1111\\"" ___tail___=" | fields - _raw | fields _time, host, ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value | sort 0 host, _time | streamstats first(ucp.Ucp*.success.sampleCount) as ucp.Ucp*.success.sampleCount_prev, first(ucp.Ucp*.errors.sampleCount) as ucp.Ucp*.errors.sampleCount_prev2 window=2 by host| rename ucp.Ucp*.latency.sampleCount as ucp.Ucp*.latency.value_prev3 | addtotals delta_ucp.Ucp*.success.sampleCount | fields - ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value, delta_ucp.Ucp*.success.sampleCount, *_prev, *_prev2 | timechart span=15s sum(Total) as \\"1Количество\\", sum(delta_*.errors.sampleCount) as \\"К2оличество *\\", max(tot_*.latency.value) as \\"max_*\\" | simple" | timechart span=15s sum("Кол3ичество") as "Коли4чество" | simple"""
        target = {'search': (
            '| ot ttl=60 | otloadjob otl="| ot ttl=60 | search index=\\"pprbepk_business\\" "  ___token___="host=\\"marica20:1111\\" OR host=\\"marica21:1111\\" OR host=\\"marica22:1111\\" OR host=\\"marica23:1111\\" OR host=\\"marica24:1111\\" OR host=\\"marica25:1111\\" OR host=\\"marica26:1111\\" OR host=\\"marica27:1111\\" OR host=\\"marica28:1111\\" OR host=\\"marica29:1111\\" OR host=\\"marica30:1111\\" OR host=\\"marica31:1111\\" OR host=\\"marica32:1111\\" OR host=\\"marica33:1111\\" OR host=\\"marica34:1111\\" OR host=\\"marica35:1111\\" OR host=\\"marica36:1111\\" OR host=\\"marica37:1111\\"" ___tail___=" | fields - _raw | fields _time, host, ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value | sort 0 host, _time | streamstats first(ucp.Ucp*.success.sampleCount) as ucp.Ucp*.success.sampleCount_prev, first(ucp.Ucp*.errors.sampleCount) as ucp.Ucp*.errors.sampleCount_prev2 window=2 by host| rename ucp.Ucp*.latency.sampleCount as ucp.Ucp*.latency.value_prev3 | addtotals delta_ucp.Ucp*.success.sampleCount | fields - ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value, delta_ucp.Ucp*.success.sampleCount, *_prev, *_prev2 | timechart span=15s sum(Total) as \\"1Количество\\", sum(delta_*.errors.sampleCount) as \\"К2оличество *\\", max(tot_*.latency.value) as \\"max_*\\" | simple" | timechart span=15s sum("Кол3ичество") as "Коли4чество" | simple',
            '| ot ttl=60 | otloadjob subsearch=subsearch_eeb5ad178a8aae0535fcd70645428e615f9643ba6f4bd55b7521f84758765d51 | timechart span=15s sum("Кол3ичество") as "Коли4чество" | simple'),
            'subsearches': {'subsearch_eeb5ad178a8aae0535fcd70645428e615f9643ba6f4bd55b7521f84758765d51': (
                '| ot ttl=60 | search index="pprbepk_business" host="marica20:1111" OR host="marica21:1111" OR host="marica22:1111" OR host="marica23:1111" OR host="marica24:1111" OR host="marica25:1111" OR host="marica26:1111" OR host="marica27:1111" OR host="marica28:1111" OR host="marica29:1111" OR host="marica30:1111" OR host="marica31:1111" OR host="marica32:1111" OR host="marica33:1111" OR host="marica34:1111" OR host="marica35:1111" OR host="marica36:1111" OR host="marica37:1111" | fields - _raw | fields _time, host, ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value | sort 0 host, _time | streamstats first(ucp.Ucp*.success.sampleCount) as ucp.Ucp*.success.sampleCount_prev, first(ucp.Ucp*.errors.sampleCount) as ucp.Ucp*.errors.sampleCount_prev2 window=2 by host| rename ucp.Ucp*.latency.sampleCount as ucp.Ucp*.latency.value_prev3 | addtotals delta_ucp.Ucp*.success.sampleCount | fields - ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value, delta_ucp.Ucp*.success.sampleCount, *_prev, *_prev2 | timechart span=15s sum(Total) as "1Количество", sum(delta_*.errors.sampleCount) as "К2оличество *", max(tot_*.latency.value) as "max_*" | simple',
                '| ot ttl=60 | filter {"query": "host=\\"marica20:1111\\" OR host=\\"marica21:1111\\" OR host=\\"marica22:1111\\" OR host=\\"marica23:1111\\" OR host=\\"marica24:1111\\" OR host=\\"marica25:1111\\" OR host=\\"marica26:1111\\" OR host=\\"marica27:1111\\" OR host=\\"marica28:1111\\" OR host=\\"marica29:1111\\" OR host=\\"marica30:1111\\" OR host=\\"marica31:1111\\" OR host=\\"marica32:1111\\" OR host=\\"marica33:1111\\" OR host=\\"marica34:1111\\" OR host=\\"marica35:1111\\" OR host=\\"marica36:1111\\" OR host=\\"marica37:1111\\"", "fields": ["host"]}| fields - _raw | fields _time, host, ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value | sort 0 host, _time | streamstats first(ucp.Ucp*.success.sampleCount) as ucp.Ucp*.success.sampleCount_prev, first(ucp.Ucp*.errors.sampleCount) as ucp.Ucp*.errors.sampleCount_prev2 window=2 by host| rename ucp.Ucp*.latency.sampleCount as ucp.Ucp*.latency.value_prev3 | addtotals delta_ucp.Ucp*.success.sampleCount | fields - ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value, delta_ucp.Ucp*.success.sampleCount, *_prev, *_prev2 | timechart span=15s sum(Total) as "1Количество", sum(delta_*.errors.sampleCount) as "К2оличество *", max(tot_*.latency.value) as "max_*" | simple')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_otloadjob(self):
        otl = """| ot ttl=60 | otloadjob otl="| ot ttl=60 | search index=alerts* sourcetype!=alert_metadata | fields - _raw| dedup full_id| search alert=\\"pprb_*\\" status!=\\"*resolved\\" status!=\\"suppressed\\" app=\\"*\\" urgency=\\"*\\" summary=\\"*kb.main*\\"| stats count(alert) by alert | simple" | where like(alert,"pprb_appcore_cdm%")| stats count | simple"""
        target = {'search': (
            '| ot ttl=60 | otloadjob otl="| ot ttl=60 | search index=alerts* sourcetype!=alert_metadata | fields - _raw| dedup full_id| search alert=\\"pprb_*\\" status!=\\"*resolved\\" status!=\\"suppressed\\" app=\\"*\\" urgency=\\"*\\" summary=\\"*kb.main*\\"| stats count(alert) by alert | simple" | where like(alert,"pprb_appcore_cdm%")| stats count | simple',
            '| ot ttl=60 | otloadjob subsearch=subsearch_ee49b9572c63942b175269e757b88e44bc94d8980181058fbbe6a1482a1f6742 | where like(alert,"pprb_appcore_cdm%")| stats count | simple'),
            'subsearches': {'subsearch_ee49b9572c63942b175269e757b88e44bc94d8980181058fbbe6a1482a1f6742': (
                '| ot ttl=60 | search index=alerts* sourcetype!=alert_metadata | fields - _raw| dedup full_id| search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*" summary="*kb.main*"| stats count(alert) by alert | simple',
                '| ot ttl=60 | filter {"query": "!(sourcetype=\\"alert_metadata\\")", "fields": ["sourcetype"]}| fields - _raw| dedup full_id| filter {"query": "(alert rlike \'pprb_.*\') AND !(status rlike \'.*resolved\') AND !(status=\\"suppressed\\") AND (app rlike \'.*\') AND (urgency rlike \'.*\') AND (summary rlike \'.*kb.main.*\')", "fields": ["alert", "status", "app", "urgency", "summary"]}| stats count(alert) by alert | simple')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_foreach(self):
        otl = """|makeresults 1| eval total=0 | eval test1=1 | eval test2=2 | eval test3=3 | foreach test* [eval total=total + <<FIELD>>]"""
        target = {'search': (
            '|makeresults 1| eval total=0 | eval test1=1 | eval test2=2 | eval test3=3 | foreach test* [eval total=total + <<FIELD>>]',
            '|makeresults 1| eval total=0 | eval test1=1 | eval test2=2 | eval test3=3 | foreach test* [eval total=total + <<FIELD>>]'),
            'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_filter_with_percent_string(self):
        otl = """search index=pprb_stuff sourcetype=oracle_db source=pprb_oracle_state_infra_p2p_* | search METRIC_NAME="Host CPU Utilization (%)"| eval VALUE=round(VALUE,2)| stats last(VALUE) as "Host CPU Utilization (%)" by host"""
        target = {'search': (
            'search index=pprb_stuff sourcetype=oracle_db source=pprb_oracle_state_infra_p2p_* | search METRIC_NAME="Host CPU Utilization (%)"| eval VALUE=round(VALUE,2)| stats last(VALUE) as "Host CPU Utilization (%)" by host',
            '| read {"pprb_stuff": {"query": "sourcetype=\\"oracle_db\\" AND (source rlike \'pprb_oracle_state_infra_p2p_.*\')", "tws": 0, "twf": 0}}| filter {"query": "METRIC_NAME=\\"Host CPU Utilization (%)\\"", "fields": ["METRIC_NAME"]}| eval VALUE=round(VALUE,2)| stats last(VALUE) as "Host CPU Utilization (%)" by host'),
            'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_filter_with_wildcards(self):
        otl = """search index=main | search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*" summary="*kb.main*" """
        target = {'search': (
            'search index=main | search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*" summary="*kb.main*" ',
            '| read {"main": {"query": "", "tws": 0, "twf": 0}}| filter {"query": "(alert rlike \'pprb_.*\') AND !(status rlike \'.*resolved\') AND !(status=\\"suppressed\\") AND (app rlike \'.*\') AND (urgency rlike \'.*\') AND (summary rlike \'.*kb.main.*\')", "fields": ["alert", "status", "app", "urgency", "summary"]}'),
            'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_read_with_not_equal(self):
        otl = """search index=main sourcetype!=alert_metadata"""
        target = {'search': ('search index=main sourcetype!=alert_metadata',
                             '| read {"main": {"query": "!(sourcetype=\\"alert_metadata\\")", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_filter_escaped_quotes(self):
        otl = """search index=main* sourcetype!=alert_metadata| fields - _raw| dedup full_id | search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*"| stats count(alert) by alert |  where like(alert,"pprb_appcore_namedcounter%")| stats count"""
        target = {'search': (
            'search index=main* sourcetype!=alert_metadata| fields - _raw| dedup full_id | search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*"| stats count(alert) by alert |  where like(alert,"pprb_appcore_namedcounter%")| stats count',
            '| read {"main": {"query": "!(sourcetype=\\"alert_metadata\\")", "tws": 0, "twf": 0}, "main1": {"query": "!(sourcetype=\\"alert_metadata\\")", "tws": 0, "twf": 0}, "main2": {"query": "!(sourcetype=\\"alert_metadata\\")", "tws": 0, "twf": 0}}| fields - _raw| dedup full_id | filter {"query": "(alert rlike \'pprb_.*\') AND !(status rlike \'.*resolved\') AND !(status=\\"suppressed\\") AND (app rlike \'.*\') AND (urgency rlike \'.*\')", "fields": ["alert", "status", "app", "urgency"]}| stats count(alert) by alert |  where like(alert,"pprb_appcore_namedcounter%")| stats count'),
            'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_filter_with_SEARCH_NOT(self):
        otl = """index=main | SEARCH NOT sourcetype=guid_ui_access"""
        target = {'search': ('index=main | SEARCH NOT sourcetype=guid_ui_access',
                             'index=main | filter {"query": "!(sourcetype=\\"guid_ui_access\\")", "fields": ["sourcetype"]}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_inputlookup_filter(self):
        otl = """| otinputlookup test.csv where field1=1 OR (field2>2 AND field3<30) OR field4"""
        target = {'search': ('| otinputlookup test.csv where field1=1 OR (field2>2 AND field3<30) OR field4',
                             '| otinputlookup test.csv where {"query": "field1=\\"1\\" OR (field2>2 AND field3<30) OR (_raw like \'%field4%\')", "fields": ["field1", "field2", "field3"]}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_inputlookup_filter_with_append(self):
        otl = """| otinputlookup test.csv append=t where field1=1 OR (field2>2 AND field3<30) OR field4"""
        target = {'search': ('| otinputlookup test.csv append=t where field1=1 OR (field2>2 AND field3<30) OR field4',
                             '| otinputlookup test.csv append=t where {"query": "field1=\\"1\\" OR (field2>2 AND field3<30) OR (_raw like \'%field4%\')", "fields": ["field1", "field2", "field3"]}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_with_special_symbols_in_filter(self):
        otl = """search index=test_index junkField="asd.a-2:13=123" junkField2!="asd.a-2:13=123" """
        target = {'search': ('search index=test_index junkField="asd.a-2:13=123" junkField2!="asd.a-2:13=123" ',
                             '| read {"test_index": {"query": "junkField=\\"asd.a-2:13=123\\" AND !(junkField2=\\"asd.a-2:13=123\\")", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_field_filter_without_quotes(self):
        otl = """search index=test_index junkField=asd.a-2:13"""
        target = {'search': ('search index=test_index junkField=asd.a-2:13',
                             '| read {"test_index": {"query": "junkField=\\"asd.a-2:13\\"", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_nesting_subsearches_without_empty_quotes(self):
        otl = """| ot ttl=60| otinputlookup append=t pprb_hosts_list.csv| where like(module_name,"%") and p_wildfly!=" "| dedup host_name p_custodian| eval h = mvzip(host_name,p_custodian,":")| eval h_wf = mvzip(host_name,p_wildfly,":")| eval host = host_name| fields host host_name p_custodian p_wildfly module_name kontur stend zone h h_wf| join type=left h [| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!=" " | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h [ search index=pprb_mon server=* | fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian ]]|simple"""
        target = {'search': ('| ot ttl=60| otinputlookup append=t pprb_hosts_list.csv| where like(module_name,"%") and p_wildfly!=" "| dedup host_name p_custodian| eval h = mvzip(host_name,p_custodian,":")| eval h_wf = mvzip(host_name,p_wildfly,":")| eval host = host_name| fields host host_name p_custodian p_wildfly module_name kontur stend zone h h_wf| join type=left h [| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!=" " | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h [ search index=pprb_mon server=* | fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian ]]|simple', '| ot ttl=60| otinputlookup append=t pprb_hosts_list.csv| where like(module_name,"%") and p_wildfly!=" "| dedup host_name p_custodian| eval h = mvzip(host_name,p_custodian,":")| eval h_wf = mvzip(host_name,p_wildfly,":")| eval host = host_name| fields host host_name p_custodian p_wildfly module_name kontur stend zone h h_wf| join type=left h subsearch=subsearch_1089a68852d41d0d3ab132157220ff88d13cf196c992ef7080b09bebf60af5b4|simple'), 'subsearches': {'subsearch_660afa6b674183588d21f731b7924e2202803db0c453ee7fabf92f3d48318515': (' search index=pprb_mon server=* | fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian ', '| read {"pprb_mon": {"query": "(server rlike \'.*\')", "tws": 0, "twf": 0}}| fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian '), 'subsearch_1089a68852d41d0d3ab132157220ff88d13cf196c992ef7080b09bebf60af5b4': ('| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!=" " | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h subsearch=subsearch_660afa6b674183588d21f731b7924e2202803db0c453ee7fabf92f3d48318515', '| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!=" " | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h subsearch=subsearch_660afa6b674183588d21f731b7924e2202803db0c453ee7fabf92f3d48318515')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_nesting_subsearches_with_empty_quotes(self):
        otl = """| ot ttl=60| otinputlookup append=t pprb_hosts_list.csv| where like(module_name,"%") and p_wildfly!=""| dedup host_name p_custodian| eval h = mvzip(host_name,p_custodian,":")| eval h_wf = mvzip(host_name,p_wildfly,":")| eval host = host_name| fields host host_name p_custodian p_wildfly module_name kontur stend zone h h_wf| join type=left h [| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!="" | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h [ search index=pprb_mon server=* | fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian ]]|simple"""
        target = {'search': ('| ot ttl=60| otinputlookup append=t pprb_hosts_list.csv| where like(module_name,"%") and p_wildfly!=""| dedup host_name p_custodian| eval h = mvzip(host_name,p_custodian,":")| eval h_wf = mvzip(host_name,p_wildfly,":")| eval host = host_name| fields host host_name p_custodian p_wildfly module_name kontur stend zone h h_wf| join type=left h [| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!="" | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h [ search index=pprb_mon server=* | fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian ]]|simple', '| ot ttl=60| otinputlookup append=t pprb_hosts_list.csv| where like(module_name,"%") and p_wildfly!=""| dedup host_name p_custodian| eval h = mvzip(host_name,p_custodian,":")| eval h_wf = mvzip(host_name,p_wildfly,":")| eval host = host_name| fields host host_name p_custodian p_wildfly module_name kontur stend zone h h_wf| join type=left h subsearch=subsearch_881af0f41e12b2805e3fc8501414adb90cf872f8560edd79dda8c30c19e05309|simple'), 'subsearches': {'subsearch_660afa6b674183588d21f731b7924e2202803db0c453ee7fabf92f3d48318515': (' search index=pprb_mon server=* | fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian ', '| read {"pprb_mon": {"query": "(server rlike \'.*\')", "tws": 0, "twf": 0}}| fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian '), 'subsearch_881af0f41e12b2805e3fc8501414adb90cf872f8560edd79dda8c30c19e05309': ('| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!="" | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h subsearch=subsearch_660afa6b674183588d21f731b7924e2202803db0c453ee7fabf92f3d48318515', '| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!="" | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h subsearch=subsearch_660afa6b674183588d21f731b7924e2202803db0c453ee7fabf92f3d48318515')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_nesting_subsearches_with_no_subsearch_commands(self):
        otl = """| makeresults | eval rmt = mvappend(1,2,7,3), b ="A" | mvexpand rmt | join rmt type=inner [| makeresults | eval rmt = mvappend(8,9,5,4), t ="X" | mvexpand rmt | foreach *mt [| eval c = sqrt(<<FIELD>>)]]"""
        target = {'search': ('| makeresults | eval rmt = mvappend(1,2,7,3), b ="A" | mvexpand rmt | join rmt type=inner [| makeresults | eval rmt = mvappend(8,9,5,4), t ="X" | mvexpand rmt | foreach *mt [| eval c = sqrt(<<FIELD>>)]]', '| makeresults | eval rmt = mvappend(1,2,7,3), b ="A" | mvexpand rmt | join rmt type=inner subsearch=subsearch_add685562d3f67b82dc1c25773c77cb5368c21e9244789d29bf9be5f8ad29065'), 'subsearches': {'subsearch_add685562d3f67b82dc1c25773c77cb5368c21e9244789d29bf9be5f8ad29065': ('| makeresults | eval rmt = mvappend(8,9,5,4), t ="X" | mvexpand rmt | foreach *mt _hidden_text_a079e7097ef6b03b640b1635ed68ac9e3050e2db6125d8199e7dbb089304b2cc', '| makeresults | eval rmt = mvappend(8,9,5,4), t ="X" | mvexpand rmt | foreach *mt [| eval c = sqrt(<<FIELD>>)]')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_rename_with_bracket(self):
        otl = """search index=main | join host [ search index=main2 | rename bla as host]"""
        target = {'search': ('search index=main | join host [ search index=main2 | rename bla as host]', '| read {"main": {"query": "", "tws": 0, "twf": 0}}| join host subsearch=subsearch_e94ea0468c117b166b32d6e5f7985ac4c1af08bb3bf941a1340a09697943d067'), 'subsearches': {'subsearch_e94ea0468c117b166b32d6e5f7985ac4c1af08bb3bf941a1340a09697943d067': (' search index=main2 | rename bla as host', '| read {"main2": {"query": "", "tws": 0, "twf": 0}}| rename bla as host')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_otstats_some_empty(self):
        otl = """| otstats index=main2 SUCCESS host="h1 bla" OR host="" OR host=h3"""
        target = {'search': ('| otstats index=main2 SUCCESS host="h1 bla" OR host="" OR host=h3',
                             '| otstats {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1 bla\\" OR host=\\"\\" OR host=\\"h3\\"", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_otstats_some_or(self):
        otl = """| otstats index=main2 SUCCESS host="h1" OR host="h2" OR host=h3"""
        target = {'search': ('| otstats index=main2 SUCCESS host="h1" OR host="h2" OR host=h3',
                             '| otstats {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1\\" OR host=\\"h2\\" OR host=\\"h3\\"", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_otstats_many_or(self):
        otl = """| otstats index=main2 SUCCESS host="h1" OR host="h2" OR host=h3 OR host=h4 OR host=h5 OR host=h6 OR host=h7 OR host=h8 OR host=h9 OR host=h10"""
        target = {'search': (
            '| otstats index=main2 SUCCESS host="h1" OR host="h2" OR host=h3 OR host=h4 OR host=h5 OR host=h6 OR host=h7 OR host=h8 OR host=h9 OR host=h10',
            '| otstats {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1\\" OR host=\\"h2\\" OR host=\\"h3\\" OR host=\\"h4\\" OR host=\\"h5\\" OR host=\\"h6\\" OR host=\\"h7\\" OR host=\\"h8\\" OR host=\\"h9\\" OR host=\\"h10\\"", "tws": 0, "twf": 0}}'),
            'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_appendpipe(self):
        otl = """search index=main| appendpipe [stats sum(random_Field) as count by WordField | eval user = "word" ]"""
        target = {'search': (
            'search index=main| appendpipe [stats sum(random_Field) as count by WordField | eval user = "word" ]', '| read {"main": {"query": "", "tws": 0, "twf": 0}}| appendpipe [stats sum(random_Field) as count by WordField | eval user = "word" ]'),
            'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_filter_in_subsearch(self):
        otl = """readFile format=parquet path=oms_v1 | search Type=3 AND geoNum="1500*" | rename geoNum as GEON | eval tag="super_debug" | join type=left GEON [| otstats index="baspro-basp_registrywell" MEST=192 | rename WELLNAME  as GEON | eval IDBaspro=IDWELL | table GEON, IDBaspro | search GEON="1500"]"""
        target = {'search': (
            'readFile format=parquet path=oms_v1 | search Type=3 AND geoNum="1500*" | rename geoNum as GEON | eval tag="super_debug" | join type=left GEON [| otstats index="baspro-basp_registrywell" MEST=192 | rename WELLNAME  as GEON | eval IDBaspro=IDWELL | table GEON, IDBaspro | search GEON="1500"]',
            'readFile format=parquet path=oms_v1 | filter {"query": "Type=\\"3\\" AND (geoNum rlike \'1500.*\')", "fields": ["Type", "geoNum"]}| rename geoNum as GEON | eval tag="super_debug" | join type=left GEON subsearch=subsearch_d1bbcfc889651a1819fef94f275890665d45561574ab282c86a2d3905d84d8c4'),
            'subsearches': {'subsearch_d1bbcfc889651a1819fef94f275890665d45561574ab282c86a2d3905d84d8c4': ('| otstats index="baspro-basp_registrywell" MEST=192 | rename WELLNAME  as GEON | eval IDBaspro=IDWELL | table GEON, IDBaspro | search GEON="1500"', '| otstats {"baspro-basp_registrywell": {"query": "MEST=\\"192\\"", "tws": 0, "twf": 0}}| rename WELLNAME  as GEON | eval IDBaspro=IDWELL | table GEON, IDBaspro | filter {"query": "GEON=\\"1500\\"", "fields": ["GEON"]}')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_macros(self):
        otl = """__getwell__ wellNum=4,12,1898 padNum=4 earliest=2019-11-01:12:12:12 latest=2019-12-31 debit freq"""
        target = {'search': ('__getwell__ wellNum=4,12,1898 padNum=4 earliest=2019-11-01:12:12:12 latest=2019-12-31 debit freq', '| readFile format=parquet path=omds_well_adku_v2 | filter {"query": "wellNum=\\"4\\" OR wellNum=\\"12\\" OR wellNum=\\"1898\\"", "fields": ["wellNum"]}| filter {"query": "_time>=1572599532.0 AND _time<1577739600.0", "fields": ["_time"]}| ``` Джойним номер куста и ID станции управления по ID скважины ``` | join type=inner ID subsearch=subsearch_a5051f659e49f751b49528f3e34be239a9e8f9759a494fbe642ffb00197f6124 | ``` Джойним данные из ШТР по ID скважины ``` | join _time, ID type=outer subsearch=subsearch_abdfd89fe9c449a160c4e8e600faaf421a070c4bdf4c9acc16cf22469c0ad926 | fields _time, ID, adku*, wellop* | ``` Джойним данные со станции управления по ID скважины ``` | join  _time, IDKs type=outer subsearch=subsearch_21dfe1bf755d0e2d3a50a005c1b0224f734c9c3bebcfb98593841a0abfa59052 | sort _time  | table _time, wellNum, padNum, adkuLiquidDebit, adkuTorLiquidDebit, adkuOilDebit, wellopLiquidDebitTM, wellopLiquidDebitTm, wellopCountedOilDebit, wellopIzlivLiquidDebitTm, wellopDefaultLiquidDebitTM, wellopmassomerLiquidDebit, wellopCondensatDebit, wellopAverageLiquidDebit, wellopOilDebitWithoutCond, ksADKUEngineFreq, ksADKUTurboFreq, wellopPumpFreq, wellopPumpCurrentFreq, wellopPumpCurrentFreqTm, wellopPumpFreqTm'), 'subsearches': {'subsearch_21dfe1bf755d0e2d3a50a005c1b0224f734c9c3bebcfb98593841a0abfa59052': ('   | readFile format=parquet path=omds_ks_v3   | search _time>=1572599532.0 AND _time<1577739600.0   | rename ID as IDKs   | table _time, IDKs, ks* ', '   | readFile format=parquet path=omds_ks_v3   | filter {"query": "_time>=1572599532.0 AND _time<1577739600.0", "fields": ["_time"]}| rename ID as IDKs   | table _time, IDKs, ks* '), 'subsearch_abdfd89fe9c449a160c4e8e600faaf421a070c4bdf4c9acc16cf22469c0ad926': ('   | readFile format=parquet path=omds_well_wellop_v15   | search _time>=1572599532.0 AND _time<1577739600.0   | table _time, ID, wellop* ', '   | readFile format=parquet path=omds_well_wellop_v15   | filter {"query": "_time>=1572599532.0 AND _time<1577739600.0", "fields": ["_time"]}| table _time, ID, wellop* '), 'subsearch_a5051f659e49f751b49528f3e34be239a9e8f9759a494fbe642ffb00197f6124': ('   | readFile format=parquet path=oms_v5   | fields IDObj, geoNum, Description, IDPad, IDWell   | eval geoPad = if(Description="Pad", geoNum, null)   | eval idKS = if(Description="controlStation", IDObj, null)   | eventstats min(geoPad) as padNum by IDPad   | eventstats min(idKS) as IDKs by IDWell   | search Description="Well" AND (padNum=4)   | rename IDObj as ID   | fields ID, padNum, IDKs ', '   | readFile format=parquet path=oms_v5   | fields IDObj, geoNum, Description, IDPad, IDWell   | eval geoPad = if(Description="Pad", geoNum, null)   | eval idKS = if(Description="controlStation", IDObj, null)   | eventstats min(geoPad) as padNum by IDPad   | eventstats min(idKS) as IDKs by IDWell   | filter {"query": "Description=\\"Well\\" AND (padNum=\\"4\\")", "fields": ["Description", "padNum"]}| rename IDObj as ID   | fields ID, padNum, IDKs ')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_macros_alias_all_args(self):
        from parsers.otl_resolver.fieldalias import FieldAlias
        target = ['adkuLiquidDebit', 'adkuTorLiquidDebit', 'adkuOilDebit', 'wellopLiquidDebitTM', 'wellopLiquidDebitTm', 'wellopCountedOilDebit', 'wellopIzlivLiquidDebitTm', 'wellopDefaultLiquidDebitTM', 'wellopmassomerLiquidDebit', 'wellopCondensatDebit', 'wellopAverageLiquidDebit', 'wellopOilDebitWithoutCond']
        fieldAlias = FieldAlias('tests/macros/names.csv')
        result = fieldAlias.get_aliases('debit')
        print('result', result)
        print('target', target)
        self.assertListEqual(result, target)

    def test_macros_not_all_args(self):
        otl = """__getwell__ padNum=4 earliest=2019-11-01:12:12:12 latest=2019-12-31 debit freq"""
        target = {'search': ('__getwell__ padNum=4 earliest=2019-11-01:12:12:12 latest=2019-12-31 debit freq', '| readFile format=parquet path=omds_well_adku_v2 | filter {"query": "(wellNum rlike \'.*\')", "fields": ["wellNum"]}| filter {"query": "_time>=1572599532.0 AND _time<1577739600.0", "fields": ["_time"]}| ``` Джойним номер куста и ID станции управления по ID скважины ``` | join type=inner ID subsearch=subsearch_a5051f659e49f751b49528f3e34be239a9e8f9759a494fbe642ffb00197f6124 | ``` Джойним данные из ШТР по ID скважины ``` | join _time, ID type=outer subsearch=subsearch_abdfd89fe9c449a160c4e8e600faaf421a070c4bdf4c9acc16cf22469c0ad926 | fields _time, ID, adku*, wellop* | ``` Джойним данные со станции управления по ID скважины ``` | join  _time, IDKs type=outer subsearch=subsearch_21dfe1bf755d0e2d3a50a005c1b0224f734c9c3bebcfb98593841a0abfa59052 | sort _time  | table _time, padNum, adkuLiquidDebit, adkuTorLiquidDebit, adkuOilDebit, wellopLiquidDebitTM, wellopLiquidDebitTm, wellopCountedOilDebit, wellopIzlivLiquidDebitTm, wellopDefaultLiquidDebitTM, wellopmassomerLiquidDebit, wellopCondensatDebit, wellopAverageLiquidDebit, wellopOilDebitWithoutCond, ksADKUEngineFreq, ksADKUTurboFreq, wellopPumpFreq, wellopPumpCurrentFreq, wellopPumpCurrentFreqTm, wellopPumpFreqTm'), 'subsearches': {'subsearch_21dfe1bf755d0e2d3a50a005c1b0224f734c9c3bebcfb98593841a0abfa59052': ('   | readFile format=parquet path=omds_ks_v3   | search _time>=1572599532.0 AND _time<1577739600.0   | rename ID as IDKs   | table _time, IDKs, ks* ', '   | readFile format=parquet path=omds_ks_v3   | filter {"query": "_time>=1572599532.0 AND _time<1577739600.0", "fields": ["_time"]}| rename ID as IDKs   | table _time, IDKs, ks* '), 'subsearch_abdfd89fe9c449a160c4e8e600faaf421a070c4bdf4c9acc16cf22469c0ad926': ('   | readFile format=parquet path=omds_well_wellop_v15   | search _time>=1572599532.0 AND _time<1577739600.0   | table _time, ID, wellop* ', '   | readFile format=parquet path=omds_well_wellop_v15   | filter {"query": "_time>=1572599532.0 AND _time<1577739600.0", "fields": ["_time"]}| table _time, ID, wellop* '), 'subsearch_a5051f659e49f751b49528f3e34be239a9e8f9759a494fbe642ffb00197f6124': ('   | readFile format=parquet path=oms_v5   | fields IDObj, geoNum, Description, IDPad, IDWell   | eval geoPad = if(Description="Pad", geoNum, null)   | eval idKS = if(Description="controlStation", IDObj, null)   | eventstats min(geoPad) as padNum by IDPad   | eventstats min(idKS) as IDKs by IDWell   | search Description="Well" AND (padNum=4)   | rename IDObj as ID   | fields ID, padNum, IDKs ', '   | readFile format=parquet path=oms_v5   | fields IDObj, geoNum, Description, IDPad, IDWell   | eval geoPad = if(Description="Pad", geoNum, null)   | eval idKS = if(Description="controlStation", IDObj, null)   | eventstats min(geoPad) as padNum by IDPad   | eventstats min(idKS) as IDKs by IDWell   | filter {"query": "Description=\\"Well\\" AND (padNum=\\"4\\")", "fields": ["Description", "padNum"]}| rename IDObj as ID   | fields ID, padNum, IDKs ')}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_inline_parsing(self):
        otl = """makeresults count=10 | scala <#\n_df\n#>"""
        target = {'search': ('makeresults count=10 | scala <#\n_df\n#>', 'makeresults count=10 | scala "Cl9kZgo="'), 'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_inline_parsing_plus(self):
        otl = """makeresults count=10 | spark <#\nSELECT * FROM <dbname>\n#>"""
        target = {'search': ('makeresults count=10 | spark <#\nSELECT * FROM <dbname>\n#>', 'makeresults count=10 | spark "ClNFTEVDVCAqIEZST00gPGRibmFtZT4K"'), 'subsearches': {}}
        result = self.resolver.resolve(otl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)
