#!/usr/bin/env python

import hmac
import argparse
import httplib2
import threading
import time
import os

from lxml import etree
from hashlib import sha256
from urllib import quote_plus
from base64 import b64encode
from datetime import datetime


lock = threading.Lock()


def gen_url(country, start_index, page_size):
    service_host = 'ats.amazonaws.com'
    query = {
        "Action": 'TopSites'
        , "AWSAccessKeyId": access_key_id
        , "Timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        , "ResponseGroup": 'Country'
        , "Start": start_index
        , "Count": page_size
        , "CountryCode": country
        , "SignatureVersion": '2'
        , "SignatureMethod": 'HmacSHA256'
    }
    join = lambda x: '='.join([x[0], quote_plus(str(x[1]))])

    query_str = '&'.join(sorted(map(join, query.iteritems())))

    sign_str = 'GET\n%s\n/\n%s' % (service_host, query_str)
    signature = hmac.new(secret_access_key, sign_str, sha256).digest()

    query_str += '&Signature=' + quote_plus(b64encode(signature).strip())

    url = 'https://%s/?%s' % (service_host, query_str)
    return url


def write_records(file_name, records):
    lock.acquire()
    f = open(file_name, 'a')
    records_str = '\n'.join(records)
    line = "%s\n" % records_str
    f.write(line)
    f.close()
    lock.release()


def write_responses(country, start_index, step, content):
    lock.acquire()
    file_name = '{}/response_{}_{}_{}.xml'.format(response_folder, country, start_index, step)
    f = open(file_name, 'w')
    f.write(content)
    f.close()
    lock.release()


def get_alexa_sites(url, country, start_index, step):
    #    http = httplib2.Http()
    resp, content = http.request(url, 'GET')
    write_responses(country, start_index, step, content)
    xml = etree.fromstring(content)
    namespace_map = {'aws': 'http://ats.amazonaws.com/doc/2005-11-21'}
    entries = xml.xpath('//aws:DataUrl', namespaces=namespace_map)
    entries = [entry.text for entry in entries]
    return entries


def get_alexa_topsites(country):
    result_file_name = '{}_{}_{}.txt'.format(country, count, datetime_str)
    step = 100 if count > 100 else count

    # records = ['physics', 'chemistry', '1997', '2000'];
    # write_records('a.txt', records)
    records_number = 0
    print '\n+++ Start to get top sites of country:%s, start:%d, count:%d' % (country, start, count)
    for i in xrange(start, count, step):
        print '\n## start:%d, count:%d' % (i, step)
        url = gen_url(country, i, step)
        records = get_alexa_sites(url, country, i, step)
        record_len = len(records)
        print '$$ record length %d' % record_len
        records_number += record_len
        if record_len != 0:
            write_records(result_file_name, records)
            time.sleep(sleep_seconds)
        else:
            # means no more records
            break
    print '\n+++ End to get top sites of country:%s, start:%d, count:%d, total records:%d' \
          % (country, start, count, records_number)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Get a range of Alexa Top sites for a specific country')

    parser.add_argument('-k', action='store', dest='access_key_id', required=True)
    parser.add_argument('-s', action='store', dest='secret_access_key', required=True)
    parser.add_argument('-c', action='store', dest='country_code', required=True,
                        help='if multiple, separate with comma(,) like "ZA,ID,IN"')
    parser.add_argument('-a', action='store', dest='start', help="start index of top sites", type=int, default=1)
    parser.add_argument('-z', action='store', dest='count', help="total number of top sites", type=int, required=True)
    parser.add_argument('-i', action='store', dest='interval', type=int, default=5,
                        help="sleep interval seconds per request, request rate control")

    args = parser.parse_args()

    if not args.count:
        parser.print_help()
        exit(2)
    access_key_id = args.access_key_id
    secret_access_key = args.secret_access_key
    country_code = args.country_code

    if args.start is None or args.start == 0:
        start = 1
    else:
        start = args.start

    count = args.count

    if args.interval is None or args.interval == 0:
        sleep_seconds = 5
    else:
        sleep_seconds = args.interval

    http = httplib2.Http()
    datetime_str = datetime.utcnow().strftime("%Y-%m-%d")
    country_list = country_code.split(",")

    for country_item in country_list:
        if country_item:
            country_item = country_item.strip().upper()
            response_folder = 'responses/{}/{}_{}_{}'.format(datetime_str, country_item, start, count)
            if not os.path.exists(response_folder):
                os.makedirs(response_folder)
            get_alexa_topsites(country_item)

    print '\n ++++All Finished.++++'
