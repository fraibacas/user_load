
#import argparse
import datetime
import json
import logging
import os
import pytz
import requests
import sys
import time

import pandas as pd
from pandas import DataFrame, Series
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
logging.getLogger("requests").setLevel(logging.WARNING)


def set_up_logger():
    log_format = "" # %(asctime)s [%(name)s] %(levelname)s %(message)s"
    #logging.basicConfig(level=logging.INFO, format=log_format)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(log_format))
    logging.getLogger("metric_analyzer").addHandler(console)
    logging.getLogger("metric_analyzer").setLevel(logging.INFO)


set_up_logger()

log = logging.getLogger("metric_analyzer")


class BCOLORS:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'


class MetricRetriever(object):
    """ Class to retrieve the zrequest_duration metric from opentsdb """

    BASE_METRIC_URL = "{0}/api/query?start={1}&end={2}&m=max:{3}{4}"

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
            datapoint["ts"] = float(ts)
            datapoint["value"] = value
            tags = raw_datum.get("tags")
            if tags:
                for tag, tag_value in tags.iteritems():
                    datapoint[tag] = tag_value
            datapoints.append(datapoint)
        return datapoints

    def get_datapoints(self, start, end, metric, tags):
        """ start and end are timestamps and should be in UTC """
        tag_text = "{" + ",".join([ "{0}=*".format(tag) for tag in tags ]) + "}"
        query_url = self.BASE_METRIC_URL.format(self.opentsdb_url, start, end, metric, tag_text)
        data = self._query(query_url)
        datapoints = []
        for datum in data:
            datapoints.extend(self._parse_raw_datum(datum))
        return datapoints


class MetricAnalyzer(object):

    def __init__(self, metric_name, datapoints):
        self.metric_name = metric_name
        self.datapoints = datapoints
        self.df = DataFrame(datapoints)
        self.df["start"] = self.df["ts"] - self.df["value"] #*1000

    def get_summary(self):
        summary = {}
        summary["min"] = self.df['value'].min()
        summary["max"] = self.df['value'].max()
        summary["mean"] = self.df['value'].mean()
        summary["count"] = self.df['value'].count()
        return summary

    def print_metric_summary(self):
        summary = self.get_summary()
        format_text = "Metric {0} => count: {1}  |  min: {2}  |  max:{3}  |  mean:{4}"
        count_text = "{0}".format(summary.get("count")).rjust(10)
        min_text = "{0:0.4f}".format(summary.get("min")).rjust(10)
        max_text = "{0:0.4f}".format(summary.get("max")).rjust(10)
        mean_text = "{0:0.4f}".format(summary.get("mean")).rjust(10)
        log.info(format_text.format(self.metric_name.rjust(20), count_text, min_text, max_text, mean_text))

    def get_top_n_by(self, by_col, cols, n=25, df=None):
        if df is None:
            df = self.df
        return df[ cols ].sort_values(by_col,  ascending=False)[0:n]

    def print_top_n_by(self, by_col, cols, n=25, df=None):
        log.info("{0}--  Top {1} calls sorted by {2}  --  {3}".format(BCOLORS.YELLOW, n, by_col, BCOLORS.ENDC))
        top_n_df = self.get_top_n_by(by_col, cols, n, df)
        for val in top_n_df.values:
            log.info("{0}: {1} => {2}".format(*val))

    def print_timeline_by_zope(self):
        cols = ['start', 'ts', 'zope', 'value', 'action', 'path' ]
        by_zope = self.df.groupby('zope')
        actions = []
        for zope, metrics_df in by_zope:
            values = metrics_df.sort_values(["start", "ts"])[cols].values
            print "\n\n TIMELINE FOR ZOPE {0} | N_CALLS: {1}\n\n".format(zope, len(values))
            for val in values:
                temp = datetime.datetime.fromtimestamp(val[0]).strftime('%Y-%m-%d %H:%M:%S')
                temp2 = datetime.datetime.fromtimestamp(val[1]).strftime('%Y-%m-%d %H:%M:%S')
                #log.info("{0} | {1}".format(val[0], val[1]))
                val = val[2:]
                zope, value, action, path = val
                if value <= 1:
                    pass#continue
                if True: #"-DeviceRouter-getGraphDefs-" in action:
                    color = BCOLORS.GREEN
                    if value > 5 and value < 10:
                        color = BCOLORS.YELLOW
                    elif value >= 10:
                        color = BCOLORS.RED
                    log.info(" {0} {1} | {2} | {3} | {4} | {5} | {6} {7}".format(color, temp, temp2, str(zope).rjust(2), value, action, path, BCOLORS.ENDC))


    def print_timeline(self):
        print "\n\n TIMELINE \n\n"
        cols = ['start', 'ts', 'zope', 'value', 'action', 'path' ]
        import datetime

        for val in self.df.sort_values(["start", "ts"])[cols].values:
            temp = datetime.datetime.fromtimestamp(val[0]).strftime('%Y-%m-%d %H:%M:%S')
            temp2 = datetime.datetime.fromtimestamp(val[1]).strftime('%Y-%m-%d %H:%M:%S')
            #log.info("{0} | {1}".format(val[0], val[1]))
            val = val[2:]
            zope, value, action, path = val
            if value <= 1:
                continue
            if True: #"-DeviceRouter-getGraphDefs-" in action:
                color = BCOLORS.GREEN
                if value > 5 and value < 10:
                    color = BCOLORS.YELLOW
                elif value >= 10:
                    color = BCOLORS.RED
                log.info(" {0} {1} | {2} | {3} | {4} | {5} | {6} {7}".format(color, temp, temp2, str(zope).rjust(2), value, action, path, BCOLORS.ENDC))

    def call_count_per_minute(self):
        start_ts = self.df["ts"].min()


class RequestDurationMetricAnalyzer(MetricAnalyzer):

    def __init__(self, metric_name, datapoints):
        super(RequestDurationMetricAnalyzer, self).__init__(metric_name, datapoints)

    def get_calls_by_action(self, df):
        by_action = df.sort_values(by='value', ascending=False).groupby('action')
        actions = []
        for action, metrics_df in by_action:
            action_data = {}
            action_data['action'] = action
            action_data['mean'] = metrics_df['value'].mean()
            action_data['count'] = metrics_df['value'].count()
            action_data['max'] = metrics_df['value'].max()
            action_data['min'] = metrics_df['value'].min()
            actions.append(action_data)
        return actions

    def get_calls_by_zopes_and_action(self, df):
        by_zope = {}
        for zope_id in df['zope'].unique():
            by_action = self.get_calls_by_action(df[df['zope']==zope_id])
            by_zope[zope_id] = by_action
        return by_zope

    def print_by_action_data(self, by_action):
        for action_data in by_action:
            log.info("Action: {0}".format(action_data.get("action")))
            log.info("\tcount: {0}, min: {1:0.4f}, max:{2:0.4f}, mean:{3:0.4f}".format(action_data.get("count"), action_data.get("min"), action_data.get("max"), action_data.get("mean")))

    def print_getInfo_calls(self, df):
        getInfo_df = df[df["action"]=="-DeviceRouter-getInfo-"][["start", "path", "value"]].sort_values(by='start')
        for val in getInfo_df.values:
            log.info("{0}: {1} => {2}".format(val[0], val[1], val[2]))

    def print_calls_by_start(self, df):
        by_start_df = df[["start", "path", "value"]].sort_values(by='start')
        for val in by_start_df.values:
            log.info("{0}: {1} => {2}".format(val[0], val[1], val[2]))

    def print_top_n_by_duration(self, df, n=20):
        top_n_df = df[["action", "path", "value"]].sort_values(by='value',  ascending=False)[0:n]
        for val in top_n_df.values:
            log.info("{0}: {1} => {2}".format(val[0], val[1], val[2]))


class WaitTimeMetricAnalyzer(MetricAnalyzer):

    def __init__(self, metric_name, datapoints):
        super(WaitTimeMetricAnalyzer, self).__init__(metric_name, datapoints)


def _datetime_to_epoch(date_):
    #date_format = '%Y-%m-%dT%H:%M:%SZ'
    date_format = '%Y/%m/%d-%H:%M:%S'
    utc = pytz.timezone("UTC")
    dt=datetime.datetime.fromtimestamp(time.mktime(time.strptime(date_, date_format)))
    aware_dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, tzinfo=pytz.UTC)
    return int((aware_dt - datetime.datetime(1970,1,1,tzinfo=pytz.UTC)).total_seconds())

def print_header(metric_name):
    log.info("{0}{1}{2}".format(BCOLORS.YELLOW, "-"*100, BCOLORS.ENDC))
    log.info("{0}ANALYSIS FOR {1}{2}".format(BCOLORS.YELLOW, metric_name, BCOLORS.ENDC).center(100))
    log.info("{0}{1}{2}".format(BCOLORS.YELLOW, "-"*100, BCOLORS.ENDC))

'''
def parse_options():
    """Defines command-line options for script """
    parser = argparse.ArgumentParser(version="1.0", description="Hack to remove objects from object_state")
    parser.add_argument("-d", "--detailed", dest="detailed", action="store_true", default=False, help="Do detailed metric analysis.")
    return vars(parser.parse_args())
'''

def main(base_opentsdb_url, start, end, detailed):

    start = _datetime_to_epoch(start)
    end = _datetime_to_epoch(end)
    assert start<end

    log.debug("start: {0}  ||  end: {1}".format(start, end))

    metric_retiever = MetricRetriever(base_opentsdb_url)

    """ ANALYSIS FOR ZREQUEST.DURATION """
    print_header("ZREQUEST.DURATION")
    request_duration_datapoints = metric_retiever.get_datapoints(start, end, "zrequest.duration", ["zope", "user", "path", "action"])
    request_duration_analyzer = RequestDurationMetricAnalyzer("zrequest.duration", request_duration_datapoints)
    request_duration_analyzer.print_metric_summary()
    if detailed:
        request_duration_analyzer.print_top_n_by("value", ["action", "path", "value"], n=100)
        request_duration_analyzer.print_timeline()

    """ ANALYSIS FOR WAIT TIME """
    print_header("WAIT TIME")
    wait_time_datapoints = metric_retiever.get_datapoints(start, end, "waitTime", ["action", "user", "workflow"])
    wait_time_analyzer = WaitTimeMetricAnalyzer("waitTime", wait_time_datapoints)
    wait_time_analyzer.print_metric_summary()
    if detailed:
        wait_time_analyzer.print_top_n_by("value", ["action", "user", "value"], n=50)


if __name__=="__main__":
    BASE_URL = "https://opentsdb.yard"  # @TODO Hack, make configurable
    if len(sys.argv) >= 3:
        # make sure your timestamps are in UTC !!
        # TODO be more flexible with start-end format '%Y/%m/%d-%H:%M:%S'
        detailed = False
        if len(sys.argv) >=4:
            detailed = True
        main(BASE_URL, sys.argv[1], sys.argv[2], detailed)

