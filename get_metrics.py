
import datetime
import json
import logging
import os
import pytz
import requests
import sys
import time

from pandas import DataFrame, Series
from requests.packages.urllib3.exceptions import InsecureRequestWarning


requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def set_up_logger():
    log_format = "%(asctime)s [%(name)s] %(levelname)s %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)

set_up_logger()

log = logging.getLogger("zrequest_duration")


class MetricRetriever(object):
    """ Class to retrieve the zrequest_duration metric from opentsdb """

    # @TODO make this less hardcoded
    #URL = "{0}/api/query?start={1}&end={2}&m=max:zrequest_duration{{zope=*,user=*,path=*,action=*}}"
    URL = "{0}/api/query?start={1}&end={2}&m=max:zrequest.duration{{zope=*,user=*,path=*,action=*}}"

    def __init__(self, opentsdb_url):
        self.opentsdb_url = opentsdb_url.rstrip("/ ")

    def _query(self, query_url):
        data = []
        resp=requests.get(query_url, verify=False, stream=True)
        if resp.ok:
            data = json.loads(resp.content)
            resp.close()
        else:
            log.error("Error retrieving data: {0}".format(resp.text))
        return data

    def _parse_raw_datum(self, raw_datum):
        datapoints = []
        for ts, value in raw_datum.get("dps", {}).iteritems():
            datapoint = {}
            datapoint["metric"] = raw_datum["metric"]
            datapoint["ts"] = ts
            datapoint["value"] = value
            tags = raw_datum.get("tags")
            if tags:
                for tag, tag_value in tags.iteritems():
                    datapoint[tag] = tag_value
            datapoints.append(datapoint)
        return datapoints

    def get_zrequest_duration_metrics(self, start, end):
        """ start and end are timestamps and should be in UTC """
        query_url = self.URL.format(self.opentsdb_url, start, end)
        data = self._query(query_url)
        # Lets put the data in a nice format for pandas
        datapoints = []
        for datum in data:
            import pdb; pdb.set_trace()
            datapoints.extend(self._parse_raw_datum(datum))
        return datapoints


class MetricAnalyzer(object):

    def __init__(self, datapoints):
        self.datapoints = datapoints
        self.df = DataFrame(datapoints)

    def get_summary(self):
        summary = {}
        summary["min"] = self.df['value'].min()
        summary["max"] = self.df['value'].max()
        summary["mean"] = self.df['value'].mean()
        summary["count"] = self.df['value'].count()
        log.info(self.df.sort_index(by='value', ascending=False)[["action", "path", "value", "zope"]])
        return summary

def _datetime_to_epoch(date_):
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    utc = pytz.timezone("UTC")
    dt=datetime.datetime.fromtimestamp(time.mktime(time.strptime(date_, date_format)))
    aware_dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, tzinfo=pytz.UTC)
    return int((aware_dt - datetime.datetime(1970,1,1,tzinfo=pytz.UTC)).total_seconds())


def main(base_opentsdb_url, start, end):

    start = _datetime_to_epoch(start)
    end = _datetime_to_epoch(end)
    assert start<end

    log.info("start: {0}  ||  end: {1}".format(start, end))

    metric_retiever = MetricRetriever(base_opentsdb_url)
    datapoints = metric_retiever.get_zrequest_duration_metrics(start, end)
    print "Found {0} datapoints".format(len(datapoints))
    metric_analyzer = MetricAnalyzer(datapoints)
    summary = metric_analyzer.get_summary()
    log.info("-"*50)
    log.info("Summary :  Datapoint count: {0}".format(summary.get("count")))
    log.info("           Response time stats:  min:  {0:0.4f}".format(summary.get("min")))
    log.info("                                 max:  {0:0.4f}".format(summary.get("max")))
    log.info("                                 mean: {0:0.4f}".format(summary.get("mean")))
    log.info("min: {0:0.4f}, max:{1:0.4f}, mean:{2:0.4f}".format(summary.get("min"), summary.get("max"), summary.get("mean")))
    log.info("-"*50)

# python get_metrics.py 1464285106 1464285259

if __name__=="__main__":
    BASE_URL = "https://opentsdb.yard"  # @TODO Hack, make configurable
    if len(sys.argv) == 3:
        #print make sure your timestamps are in UTC !!
        main(BASE_URL, sys.argv[1], sys.argv[2])

