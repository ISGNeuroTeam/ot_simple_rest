import unittest

import logging
import parsers.spl_resolver.Resolver as Resolver


class TestResolver(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        self.resolver = Resolver.Resolver(['main', 'main1', 'main2'], 0, 0, no_subsearch_commands='foreach,appendpipe')
        logging.basicConfig(
            level='DEBUG',
            format="%(asctime)s %(levelname)-s PID=%(process)d %(module)s:%(lineno)d \
    func=%(funcName)s - %(message)s")

    def test_read_some_empty(self):
        spl = """search index=main2 SUCCESS host="h1 bla" OR host="" OR host=h3"""
        target = {'search': ('search index=main2 SUCCESS host="h1 bla" OR host="" OR host=h3',
                             '| read {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1 bla\\" OR host=\\"\\" OR host=\\"h3\\"", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_some_or(self):
        spl = """search index=main2 SUCCESS host="h1" OR host="h2" OR host=h3"""
        target = {'search': ('search index=main2 SUCCESS host="h1" OR host="h2" OR host=h3',
                             '| read {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1\\" OR host=\\"h2\\" OR host=\\"h3\\"", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_many_or(self):
        spl = """search index=main2 SUCCESS host="h1" OR host="h2" OR host=h3 OR host=h4 OR host=h5 OR host=h6 OR host=h7 OR host=h8 OR host=h9 OR host=h10"""
        target = {'search': (
            'search index=main2 SUCCESS host="h1" OR host="h2" OR host=h3 OR host=h4 OR host=h5 OR host=h6 OR host=h7 OR host=h8 OR host=h9 OR host=h10',
            '| read {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1\\" OR host=\\"h2\\" OR host=\\"h3\\" OR host=\\"h4\\" OR host=\\"h5\\" OR host=\\"h6\\" OR host=\\"h7\\" OR host=\\"h8\\" OR host=\\"h9\\" OR host=\\"h10\\"", "tws": 0, "twf": 0}}'),
            'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_rex(self):
        spl = """search index=main2 SUCCESS | rex field=host "^(?<host>[^\.\:]+).*\:[0-9]" | stats count by host"""
        target = {'search': (
            'search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host',
            '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host'),
            'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearche_general(self):
        spl = """search index=main1 FAIL | join host [search index=main2 SUCCESS | stats count by host]"""
        target = {'search': ('search index=main1 FAIL | join host [search index=main2 SUCCESS | stats count by host]',
                             '| read {"main1": {"query": "(_raw like \'%FAIL%\')", "tws": 0, "twf": 0}}| join host subsearch=subsearch_1ae3636e1888e78d8be6893c1e7d569b9289d145bdc8e5d33cdc50aa5bf097e7'),
                  'subsearches': {'subsearch_1ae3636e1888e78d8be6893c1e7d569b9289d145bdc8e5d33cdc50aa5bf097e7': (
                      'search index=main2 SUCCESS | stats count by host',
                      '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| stats count by host')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearch_with_rex(self):
        spl = """search index=main FAIL | join host [search index=main2 SUCCESS | rex field=host "^(?<host>[^\.\:]+).*\:[0-9]" | stats count by host]"""
        target = {'search': (
            'search index=main FAIL | join host [search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host]',
            '| read {"main": {"query": "(_raw like \'%FAIL%\')", "tws": 0, "twf": 0}}| join host subsearch=subsearch_44519941c88171a3bdcf5ffe6ae363eadd3e2969d0209252490e76933ed2af26'),
            'subsearches': {'subsearch_44519941c88171a3bdcf5ffe6ae363eadd3e2969d0209252490e76933ed2af26': (
                'search index=main2 SUCCESS | rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host',
                '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| rex field=host "^(?<host>[^\\.\\:]+).*\\:[0-9]" | stats count by host')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_susbearch_with_return(self):
        spl = """search index=main [search index=main2 | return random_Field]"""
        target = {'search': ('search index=main [search index=main2 | return random_Field]',
                             '| read {"main": {"query": "", "tws": 0, "twf": 0}} subsearch=subsearch_f185051077b589c430cff82cd0156cc3da0e2399b8189a21fe2bd626eeb0467a'),
                  'subsearches': {'subsearch_f185051077b589c430cff82cd0156cc3da0e2399b8189a21fe2bd626eeb0467a': (
                      'search index=main2 | return random_Field',
                      '| read {"main2": {"query": "", "tws": 0, "twf": 0}}| return random_Field')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearch_with_otoutputlookup(self):
        spl = """search index=main | otoutputlookup testoutputlookup.csv"""
        target = {'search': ('search index=main | otoutputlookup testoutputlookup.csv',
                             '| read {"main": {"query": "", "tws": 0, "twf": 0}}| otoutputlookup testoutputlookup.csv'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_subsearch_with_append(self):
        spl = """search index=main | table _time, serialField, random_Field, WordField, junkField| append [search index=main junkField="word"]"""
        target = {'search': (
            'search index=main | table _time, serialField, random_Field, WordField, junkField| append [search index=main junkField="word"]',
            '| read {"main": {"query": "", "tws": 0, "twf": 0}}| table _time, serialField, random_Field, WordField, junkField| append subsearch=subsearch_33bbbb794d2b8f066c0479361a5caeb24e3553ebdbccfe2aaf278c158b7fdabb'),
            'subsearches': {'subsearch_33bbbb794d2b8f066c0479361a5caeb24e3553ebdbccfe2aaf278c158b7fdabb': (
                'search index=main junkField="word"',
                '| read {"main": {"query": "junkField=\\"word\\"", "tws": 0, "twf": 0}}')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_several_indexes(self):
        spl = """search index=main1 OR index=main2 SUCCESS"""
        target = {'search': ('search index=main1 OR index=main2 SUCCESS',
                             '| read {"main1": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_read_indexes_with_wildcards(self):
        spl = """search index=main* SUCCESS"""
        target = {'search': ('search index=main* SUCCESS',
                             '| read {"main": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main1": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_read_asterisk_instead_of_indexes(self):
        spl = """search index=* SUCCESS"""
        target = {'search': ('search index=* SUCCESS',
                             '| read {"main": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main1": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_filter_some(self):
        spl = """search index=main2 SUCCESS | search host="h1 bla" OR host="" OR host=h3"""
        target = {'search': ('search index=main2 SUCCESS | search host="h1 bla" OR host="" OR host=h3',
                             '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| filter {"query": "host=\\"h1 bla\\" OR host=\\"\\" OR host=\\"h3\\"", "fields": ["host"]}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_filter_fts(self):
        spl = """search index=main2 SUCCESS | search raw_search """
        target = {'search': ('search index=main2 SUCCESS | search raw_search ',
                             '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| filter {"query": "(_raw like \'%raw_search%\')", "fields": []}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_filter_fts_escaped(self):
        spl = """search index=main2 SUCCESS | search "raw search" """
        target = {'search': ('search index=main2 SUCCESS | search "raw search" ',
                             '| read {"main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}| filter {"query": "(_raw rlike \'raw search\')", "fields": []}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_some_and(self):
        spl = """search index=main2 SUCCESS, FAIL field1=h3, field2="h4", field3="zxc, 123" """
        target = {'search': ('search index=main2 SUCCESS, FAIL field1=h3, field2="h4", field3="zxc, 123" ',
                             '| read {"main2": {"query": "(_raw like \'%SUCCESS%\') AND (_raw like \'%FAIL%\') AND field1=\\"h3\\" AND field2=\\"h4\\" AND field3=\\"zxc, 123\\"", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_several_indexes_with_quotes(self):
        spl = """search index="main1" OR index=main2 SUCCESS"""
        target = {'search': ('search index="main1" OR index=main2 SUCCESS',
                             '| read {"main1": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}, "main2": {"query": "(_raw like \'%SUCCESS%\')", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_otloadjob_with_token(self):
        spl = """| ot ttl=60 | otloadjob spl="| ot ttl=60 | search index=\\"pprbepk_business\\" "  ___token___="host=\\"marica20:1111\\" OR host=\\"marica21:1111\\" OR host=\\"marica22:1111\\" OR host=\\"marica23:1111\\" OR host=\\"marica24:1111\\" OR host=\\"marica25:1111\\" OR host=\\"marica26:1111\\" OR host=\\"marica27:1111\\" OR host=\\"marica28:1111\\" OR host=\\"marica29:1111\\" OR host=\\"marica30:1111\\" OR host=\\"marica31:1111\\" OR host=\\"marica32:1111\\" OR host=\\"marica33:1111\\" OR host=\\"marica34:1111\\" OR host=\\"marica35:1111\\" OR host=\\"marica36:1111\\" OR host=\\"marica37:1111\\"" ___tail___=" | fields - _raw | fields _time, host, ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value | sort 0 host, _time | streamstats first(ucp.Ucp*.success.sampleCount) as ucp.Ucp*.success.sampleCount_prev, first(ucp.Ucp*.errors.sampleCount) as ucp.Ucp*.errors.sampleCount_prev2 window=2 by host| rename ucp.Ucp*.latency.sampleCount as ucp.Ucp*.latency.value_prev3 | addtotals delta_ucp.Ucp*.success.sampleCount | fields - ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value, delta_ucp.Ucp*.success.sampleCount, *_prev, *_prev2 | timechart span=15s sum(Total) as \\"1Количество\\", sum(delta_*.errors.sampleCount) as \\"К2оличество *\\", max(tot_*.latency.value) as \\"max_*\\" | simple" | timechart span=15s sum("Кол3ичество") as "Коли4чество" | simple"""
        target = {'search': (
            '| ot ttl=60 | otloadjob spl="| ot ttl=60 | search index=\\"pprbepk_business\\" "  ___token___="host=\\"marica20:1111\\" OR host=\\"marica21:1111\\" OR host=\\"marica22:1111\\" OR host=\\"marica23:1111\\" OR host=\\"marica24:1111\\" OR host=\\"marica25:1111\\" OR host=\\"marica26:1111\\" OR host=\\"marica27:1111\\" OR host=\\"marica28:1111\\" OR host=\\"marica29:1111\\" OR host=\\"marica30:1111\\" OR host=\\"marica31:1111\\" OR host=\\"marica32:1111\\" OR host=\\"marica33:1111\\" OR host=\\"marica34:1111\\" OR host=\\"marica35:1111\\" OR host=\\"marica36:1111\\" OR host=\\"marica37:1111\\"" ___tail___=" | fields - _raw | fields _time, host, ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value | sort 0 host, _time | streamstats first(ucp.Ucp*.success.sampleCount) as ucp.Ucp*.success.sampleCount_prev, first(ucp.Ucp*.errors.sampleCount) as ucp.Ucp*.errors.sampleCount_prev2 window=2 by host| rename ucp.Ucp*.latency.sampleCount as ucp.Ucp*.latency.value_prev3 | addtotals delta_ucp.Ucp*.success.sampleCount | fields - ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value, delta_ucp.Ucp*.success.sampleCount, *_prev, *_prev2 | timechart span=15s sum(Total) as \\"1Количество\\", sum(delta_*.errors.sampleCount) as \\"К2оличество *\\", max(tot_*.latency.value) as \\"max_*\\" | simple" | timechart span=15s sum("Кол3ичество") as "Коли4чество" | simple',
            '| ot ttl=60 | otloadjob subsearch=subsearch_eeb5ad178a8aae0535fcd70645428e615f9643ba6f4bd55b7521f84758765d51 | timechart span=15s sum("Кол3ичество") as "Коли4чество" | simple'),
            'subsearches': {'subsearch_eeb5ad178a8aae0535fcd70645428e615f9643ba6f4bd55b7521f84758765d51': (
                '| ot ttl=60 | search index="pprbepk_business" host="marica20:1111" OR host="marica21:1111" OR host="marica22:1111" OR host="marica23:1111" OR host="marica24:1111" OR host="marica25:1111" OR host="marica26:1111" OR host="marica27:1111" OR host="marica28:1111" OR host="marica29:1111" OR host="marica30:1111" OR host="marica31:1111" OR host="marica32:1111" OR host="marica33:1111" OR host="marica34:1111" OR host="marica35:1111" OR host="marica36:1111" OR host="marica37:1111" | fields - _raw | fields _time, host, ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value | sort 0 host, _time | streamstats first(ucp.Ucp*.success.sampleCount) as ucp.Ucp*.success.sampleCount_prev, first(ucp.Ucp*.errors.sampleCount) as ucp.Ucp*.errors.sampleCount_prev2 window=2 by host| rename ucp.Ucp*.latency.sampleCount as ucp.Ucp*.latency.value_prev3 | addtotals delta_ucp.Ucp*.success.sampleCount | fields - ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value, delta_ucp.Ucp*.success.sampleCount, *_prev, *_prev2 | timechart span=15s sum(Total) as "1Количество", sum(delta_*.errors.sampleCount) as "К2оличество *", max(tot_*.latency.value) as "max_*" | simple',
                '| ot ttl=60 | filter {"query": "host=\\"marica20:1111\\" OR host=\\"marica21:1111\\" OR host=\\"marica22:1111\\" OR host=\\"marica23:1111\\" OR host=\\"marica24:1111\\" OR host=\\"marica25:1111\\" OR host=\\"marica26:1111\\" OR host=\\"marica27:1111\\" OR host=\\"marica28:1111\\" OR host=\\"marica29:1111\\" OR host=\\"marica30:1111\\" OR host=\\"marica31:1111\\" OR host=\\"marica32:1111\\" OR host=\\"marica33:1111\\" OR host=\\"marica34:1111\\" OR host=\\"marica35:1111\\" OR host=\\"marica36:1111\\" OR host=\\"marica37:1111\\"", "fields": ["host"]}| fields - _raw | fields _time, host, ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value | sort 0 host, _time | streamstats first(ucp.Ucp*.success.sampleCount) as ucp.Ucp*.success.sampleCount_prev, first(ucp.Ucp*.errors.sampleCount) as ucp.Ucp*.errors.sampleCount_prev2 window=2 by host| rename ucp.Ucp*.latency.sampleCount as ucp.Ucp*.latency.value_prev3 | addtotals delta_ucp.Ucp*.success.sampleCount | fields - ucp.Ucp*.success.sampleCount, ucp.Ucp*.errors.sampleCount, ucp.Ucp*.latency.sampleCount, ucp.Ucp*.latency.value, delta_ucp.Ucp*.success.sampleCount, *_prev, *_prev2 | timechart span=15s sum(Total) as "1Количество", sum(delta_*.errors.sampleCount) as "К2оличество *", max(tot_*.latency.value) as "max_*" | simple')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_otloadjob(self):
        spl = """| ot ttl=60 | otloadjob spl="| ot ttl=60 | search index=alerts* sourcetype!=alert_metadata | fields - _raw| dedup full_id| search alert=\\"pprb_*\\" status!=\\"*resolved\\" status!=\\"suppressed\\" app=\\"*\\" urgency=\\"*\\" summary=\\"*kb.main*\\"| stats count(alert) by alert | simple" | where like(alert,"pprb_appcore_cdm%")| stats count | simple"""
        target = {'search': (
            '| ot ttl=60 | otloadjob spl="| ot ttl=60 | search index=alerts* sourcetype!=alert_metadata | fields - _raw| dedup full_id| search alert=\\"pprb_*\\" status!=\\"*resolved\\" status!=\\"suppressed\\" app=\\"*\\" urgency=\\"*\\" summary=\\"*kb.main*\\"| stats count(alert) by alert | simple" | where like(alert,"pprb_appcore_cdm%")| stats count | simple',
            '| ot ttl=60 | otloadjob subsearch=subsearch_ee49b9572c63942b175269e757b88e44bc94d8980181058fbbe6a1482a1f6742 | where like(alert,"pprb_appcore_cdm%")| stats count | simple'),
            'subsearches': {'subsearch_ee49b9572c63942b175269e757b88e44bc94d8980181058fbbe6a1482a1f6742': (
                '| ot ttl=60 | search index=alerts* sourcetype!=alert_metadata | fields - _raw| dedup full_id| search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*" summary="*kb.main*"| stats count(alert) by alert | simple',
                '| ot ttl=60 | filter {"query": "!(sourcetype=\\"alert_metadata\\")", "fields": ["sourcetype"]}| fields - _raw| dedup full_id| filter {"query": "(alert rlike \'pprb_.*\') AND !(status rlike \'.*resolved\') AND !(status=\\"suppressed\\") AND (app rlike \'.*\') AND (urgency rlike \'.*\') AND (summary rlike \'.*kb.main.*\')", "fields": ["alert", "status", "app", "urgency", "summary"]}| stats count(alert) by alert | simple')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_foreach(self):
        spl = """|makeresults 1| eval total=0 | eval test1=1 | eval test2=2 | eval test3=3 | foreach test* [eval total=total + <<FIELD>>]"""
        target = {'search': (
            '|makeresults 1| eval total=0 | eval test1=1 | eval test2=2 | eval test3=3 | foreach test* [eval total=total + <<FIELD>>]',
            '|makeresults 1| eval total=0 | eval test1=1 | eval test2=2 | eval test3=3 | foreach test* [eval total=total + <<FIELD>>]'),
            'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_filter_with_percent_string(self):
        spl = """search index=pprb_stuff sourcetype=oracle_db source=pprb_oracle_state_infra_p2p_* | search METRIC_NAME="Host CPU Utilization (%)"| eval VALUE=round(VALUE,2)| stats last(VALUE) as "Host CPU Utilization (%)" by host"""
        target = {'search': (
            'search index=pprb_stuff sourcetype=oracle_db source=pprb_oracle_state_infra_p2p_* | search METRIC_NAME="Host CPU Utilization (%)"| eval VALUE=round(VALUE,2)| stats last(VALUE) as "Host CPU Utilization (%)" by host',
            '| read {"pprb_stuff": {"query": "sourcetype=\\"oracle_db\\" AND (source rlike \'pprb_oracle_state_infra_p2p_.*\')", "tws": 0, "twf": 0}}| filter {"query": "METRIC_NAME=\\"Host CPU Utilization (%)\\"", "fields": ["METRIC_NAME"]}| eval VALUE=round(VALUE,2)| stats last(VALUE) as "Host CPU Utilization (%)" by host'),
            'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_filter_with_wildcards(self):
        spl = """search index=main | search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*" summary="*kb.main*" """
        target = {'search': (
            'search index=main | search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*" summary="*kb.main*" ',
            '| read {"main": {"query": "", "tws": 0, "twf": 0}}| filter {"query": "(alert rlike \'pprb_.*\') AND !(status rlike \'.*resolved\') AND !(status=\\"suppressed\\") AND (app rlike \'.*\') AND (urgency rlike \'.*\') AND (summary rlike \'.*kb.main.*\')", "fields": ["alert", "status", "app", "urgency", "summary"]}'),
            'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_read_with_not_equal(self):
        spl = """search index=main sourcetype!=alert_metadata"""
        target = {'search': ('search index=main sourcetype!=alert_metadata',
                             '| read {"main": {"query": "!(sourcetype=\\"alert_metadata\\")", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_filter_escaped_quotes(self):
        spl = """search index=main* sourcetype!=alert_metadata| fields - _raw| dedup full_id | search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*"| stats count(alert) by alert |  where like(alert,"pprb_appcore_namedcounter%")| stats count"""
        target = {'search': (
            'search index=main* sourcetype!=alert_metadata| fields - _raw| dedup full_id | search alert="pprb_*" status!="*resolved" status!="suppressed" app="*" urgency="*"| stats count(alert) by alert |  where like(alert,"pprb_appcore_namedcounter%")| stats count',
            '| read {"main": {"query": "!(sourcetype=\\"alert_metadata\\")", "tws": 0, "twf": 0}, "main1": {"query": "!(sourcetype=\\"alert_metadata\\")", "tws": 0, "twf": 0}, "main2": {"query": "!(sourcetype=\\"alert_metadata\\")", "tws": 0, "twf": 0}}| fields - _raw| dedup full_id | filter {"query": "(alert rlike \'pprb_.*\') AND !(status rlike \'.*resolved\') AND !(status=\\"suppressed\\") AND (app rlike \'.*\') AND (urgency rlike \'.*\')", "fields": ["alert", "status", "app", "urgency"]}| stats count(alert) by alert |  where like(alert,"pprb_appcore_namedcounter%")| stats count'),
            'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_filter_with_SEARCH_NOT(self):
        spl = """index=main | SEARCH NOT sourcetype=splunkd_ui_access"""
        target = {'search': ('index=main | SEARCH NOT sourcetype=splunkd_ui_access',
                             'index=main | filter {"query": "!(sourcetype=\\"splunkd_ui_access\\")", "fields": ["sourcetype"]}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(target, result)

    def test_inputlookup_filter(self):
        spl = """| otinputlookup test.csv where field1=1 OR (field2>2 AND field3<30) OR field4"""
        target = {'search': ('| otinputlookup test.csv where field1=1 OR (field2>2 AND field3<30) OR field4',
                             '| otinputlookup test.csv where {"query": "field1=\\"1\\" OR (field2>2 AND field3<30) OR (_raw like \'%field4%\')", "fields": ["field1", "field2", "field3"]}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_inputlookup_filter_with_append(self):
        spl = """| otinputlookup test.csv append=t where field1=1 OR (field2>2 AND field3<30) OR field4"""
        target = {'search': ('| otinputlookup test.csv append=t where field1=1 OR (field2>2 AND field3<30) OR field4',
                             '| otinputlookup test.csv append=t where {"query": "field1=\\"1\\" OR (field2>2 AND field3<30) OR (_raw like \'%field4%\')", "fields": ["field1", "field2", "field3"]}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_with_special_symbols_in_filter(self):
        spl = """search index=test_index junkField="asd.a-2:13=123" junkField2!="asd.a-2:13=123" """
        target = {'search': ('search index=test_index junkField="asd.a-2:13=123" junkField2!="asd.a-2:13=123" ',
                             '| read {"test_index": {"query": "junkField=\\"asd.a-2:13=123\\" AND !(junkField2=\\"asd.a-2:13=123\\")", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_read_field_filter_without_quotes(self):
        spl = """search index=test_index junkField=asd.a-2:13"""
        target = {'search': ('search index=test_index junkField=asd.a-2:13',
                             '| read {"test_index": {"query": "junkField=\\"asd.a-2:13\\"", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_nesting_subsearches_without_empty_quotes(self):
        spl = """| ot ttl=60| otinputlookup append=t pprb_hosts_list.csv| where like(module_name,"%") and p_wildfly!=" "| dedup host_name p_custodian| eval h = mvzip(host_name,p_custodian,":")| eval h_wf = mvzip(host_name,p_wildfly,":")| eval host = host_name| fields host host_name p_custodian p_wildfly module_name kontur stend zone h h_wf| join type=left h [| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!=" " | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h [ search index=pprb_mon server=* earliest=-5m | fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian ]]|simple"""
        target = {'search': ('| ot ttl=60| otinputlookup append=t pprb_hosts_list.csv| where like(module_name,"%") and p_wildfly!=" "| dedup host_name p_custodian| eval h = mvzip(host_name,p_custodian,":")| eval h_wf = mvzip(host_name,p_wildfly,":")| eval host = host_name| fields host host_name p_custodian p_wildfly module_name kontur stend zone h h_wf| join type=left h [| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!=" " | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h [ search index=pprb_mon server=* earliest=-5m | fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian ]]|simple', '| ot ttl=60| otinputlookup append=t pprb_hosts_list.csv| where like(module_name,"%") and p_wildfly!=" "| dedup host_name p_custodian| eval h = mvzip(host_name,p_custodian,":")| eval h_wf = mvzip(host_name,p_wildfly,":")| eval host = host_name| fields host host_name p_custodian p_wildfly module_name kontur stend zone h h_wf| join type=left h subsearch=subsearch_f9d7c27a0c6994d9a8226ff882a18dc09c5f2b4ed6211bfc3d262bf721235e38|simple'), 'subsearches': {'subsearch_30fe2ebc9a59253a985c7c8f649149c594de2facc29d63a7ca94ea5f9daa5a08': (' search index=pprb_mon server=* earliest=-5m | fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian ', '| read {"pprb_mon": {"query": "(server rlike \'.*\') AND earliest=\\"-5m\\"", "tws": 0, "twf": 0}}| fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian '), 'subsearch_f9d7c27a0c6994d9a8226ff882a18dc09c5f2b4ed6211bfc3d262bf721235e38': ('| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!=" " | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h subsearch=subsearch_30fe2ebc9a59253a985c7c8f649149c594de2facc29d63a7ca94ea5f9daa5a08', '| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!=" " | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h subsearch=subsearch_30fe2ebc9a59253a985c7c8f649149c594de2facc29d63a7ca94ea5f9daa5a08')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_nesting_subsearches_with_empty_quotes(self):
        spl = """| ot ttl=60| otinputlookup append=t pprb_hosts_list.csv| where like(module_name,"%") and p_wildfly!=""| dedup host_name p_custodian| eval h = mvzip(host_name,p_custodian,":")| eval h_wf = mvzip(host_name,p_wildfly,":")| eval host = host_name| fields host host_name p_custodian p_wildfly module_name kontur stend zone h h_wf| join type=left h [| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!="" | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h [ search index=pprb_mon server=* earliest=-5m | fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian ]]|simple"""
        target = {'search': ('| ot ttl=60| otinputlookup append=t pprb_hosts_list.csv| where like(module_name,"%") and p_wildfly!=""| dedup host_name p_custodian| eval h = mvzip(host_name,p_custodian,":")| eval h_wf = mvzip(host_name,p_wildfly,":")| eval host = host_name| fields host host_name p_custodian p_wildfly module_name kontur stend zone h h_wf| join type=left h [| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!="" | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h [ search index=pprb_mon server=* earliest=-5m | fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian ]]|simple', '| ot ttl=60| otinputlookup append=t pprb_hosts_list.csv| where like(module_name,"%") and p_wildfly!=""| dedup host_name p_custodian| eval h = mvzip(host_name,p_custodian,":")| eval h_wf = mvzip(host_name,p_wildfly,":")| eval host = host_name| fields host host_name p_custodian p_wildfly module_name kontur stend zone h h_wf| join type=left h subsearch=subsearch_23e15c6ee10a5531b187c2229d6773d46538fa1e08fa43049e65e2ddbaada74c|simple'), 'subsearches': {'subsearch_30fe2ebc9a59253a985c7c8f649149c594de2facc29d63a7ca94ea5f9daa5a08': (' search index=pprb_mon server=* earliest=-5m | fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian ', '| read {"pprb_mon": {"query": "(server rlike \'.*\') AND earliest=\\"-5m\\"", "tws": 0, "twf": 0}}| fields - _raw | dedup server port | eval server = server + ":" + port | rex field=server "(?<h>.*)" | rename server as ezsm_host_custodian | fields h ezsm_host_custodian '), 'subsearch_23e15c6ee10a5531b187c2229d6773d46538fa1e08fa43049e65e2ddbaada74c': ('| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!="" | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h subsearch=subsearch_30fe2ebc9a59253a985c7c8f649149c594de2facc29d63a7ca94ea5f9daa5a08', '| otinputlookup append=t pprb_hosts_list.csv | where like(module_name,"%") and p_wildfly!="" | dedup host_name p_custodian | eval h = mvzip(host_name,p_custodian,":") | command h="element_at(h,0)"| rename host_name as host | fields host h | join type=left h subsearch=subsearch_30fe2ebc9a59253a985c7c8f649149c594de2facc29d63a7ca94ea5f9daa5a08')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_rename_with_bracket(self):
        spl = """search index=main | join host [ search index=main2 | rename bla as host]"""
        target = {'search': ('search index=main | join host [ search index=main2 | rename bla as host]', '| read {"main": {"query": "", "tws": 0, "twf": 0}}| join host subsearch=subsearch_e94ea0468c117b166b32d6e5f7985ac4c1af08bb3bf941a1340a09697943d067'), 'subsearches': {'subsearch_e94ea0468c117b166b32d6e5f7985ac4c1af08bb3bf941a1340a09697943d067': (' search index=main2 | rename bla as host', '| read {"main2": {"query": "", "tws": 0, "twf": 0}}| rename bla as host')}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_otstats_some_empty(self):
        spl = """| otstats index=main2 SUCCESS host="h1 bla" OR host="" OR host=h3"""
        target = {'search': ('| otstats index=main2 SUCCESS host="h1 bla" OR host="" OR host=h3',
                             '| otstats {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1 bla\\" OR host=\\"\\" OR host=\\"h3\\"", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_otstats_some_or(self):
        spl = """| otstats index=main2 SUCCESS host="h1" OR host="h2" OR host=h3"""
        target = {'search': ('| otstats index=main2 SUCCESS host="h1" OR host="h2" OR host=h3',
                             '| otstats {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1\\" OR host=\\"h2\\" OR host=\\"h3\\"", "tws": 0, "twf": 0}}'),
                  'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)

    def test_otstats_many_or(self):
        spl = """| otstats index=main2 SUCCESS host="h1" OR host="h2" OR host=h3 OR host=h4 OR host=h5 OR host=h6 OR host=h7 OR host=h8 OR host=h9 OR host=h10"""
        target = {'search': (
            '| otstats index=main2 SUCCESS host="h1" OR host="h2" OR host=h3 OR host=h4 OR host=h5 OR host=h6 OR host=h7 OR host=h8 OR host=h9 OR host=h10',
            '| otstats {"main2": {"query": "(_raw like \'%SUCCESS%\') AND host=\\"h1\\" OR host=\\"h2\\" OR host=\\"h3\\" OR host=\\"h4\\" OR host=\\"h5\\" OR host=\\"h6\\" OR host=\\"h7\\" OR host=\\"h8\\" OR host=\\"h9\\" OR host=\\"h10\\"", "tws": 0, "twf": 0}}'),
            'subsearches': {}}
        result = self.resolver.resolve(spl)
        print('result', result)
        print('target', target)
        self.assertDictEqual(result, target)
