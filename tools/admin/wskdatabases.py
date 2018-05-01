#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import abc
import hashlib
import json
import sys
if sys.version_info.major >= 3:
    from urllib.parse import quote_plus
else:
    from urllib import quote_plus
from wskutil import request

DB_PROTOCOL = 'DB_PROTOCOL'
DB_HOST     = 'DB_HOST'
DB_PORT     = 'DB_PORT'

DB_CONNECT_STRING = 'DB_CONNECT_STRING'
DB_REPLICA_SET    = 'DB_REPLICA_SET'
DB_DATABASE       = 'DB_DATABASE'

DB_USERNAME = 'DB_USERNAME'
DB_PASSWORD = 'DB_PASSWORD'


class NotSupportedOperation(Exception):
    pass


class UnsupportedQueryKeys(Exception):
    pass


class Response(object):
    """unified response of database operation"""
    def __init__(self, return_status, data):
        self.status = return_status
        self.data = data

    def read(self):
        return self.data


class Database(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get(self, table, doc_id, verbose=False):
        return

    @abc.abstractmethod
    def insert(self, table, doc, verbose=False):
        return

    @abc.abstractmethod
    def delete(self, table, doc, verbose=False):
        return

    @abc.abstractmethod
    def list(self, table, view, key, verbose=False):
        return

    @abc.abstractmethod
    def get_by_view(self, table, view=None, include_docs=False, verbose=False):
        return


class SubjectView(object):
    def __init__(self, namespace, uuid, key):
        self.namespace = namespace
        self.uuid = uuid
        self.key = key


class SubjectViewHelper(object):
    def _transform_subjects_identities_view(self, key, result):
        _filter = None
        if len(key) == 1:
            _filter = lambda s: s.namespace == key[0]
        elif len(key) == 2:
            _filter = lambda s: s.uuid == key[0] and s.key == key[1]

        subjects = []
        for r in result:
            temp = []
            if all([i in r for i in['subject', 'uuid', 'key']]):
                temp = [SubjectView(r['subject'], r['uuid'], r['key'])]
            elif 'namespaces' in r:
                temp = [SubjectView(ns['name'], ns['uuid'], ns['key']) for ns in r['namespaces']]

            subjects.extend(
                map(lambda s: {
                    'id': r['_id'],
                    'key': key,
                    'value': {
                        '_id': '%s/limits' % s.namespace,
                        'namespace': s.namespace,
                        'uuid': s.uuid,
                        'key': s.key
                    }}, filter(_filter, temp))
            )

        return subjects

    def _filter_for_subjects_identities(self, key):
        if len(key) == 1:
            return {"$and": [
                {"blocked": {"$ne": True}},
                {"$or": [{"subject": key[0]}, {"namespaces.name": key[0]}]}
            ]}
        elif len(key) == 2:
            return {"$and": [
                {"blocked": {"$ne": True}},
                {"$or": [
                    {"uuid": key[0], "key": key[1]},
                    {"namespaces.uuid": key[0], "namespaces.key": key[1]}
                ]}
            ]}
        else:
            raise UnsupportedQueryKeys("%s on subject/identities" % key)

    def transform_view_result(self, view, key, result):
        if view == 'subjects/identities':
            return self._transform_subjects_identities_view(key, result)
        else:
            raise NotSupportedOperation("this MongoDB driver doesn't support view %s yet" % view)

    def transform_filter(self, view, key):
        if view == "subjects/identities":
            return self._filter_for_subjects_identities(key)
        else:
            raise NotSupportedOperation("this MongoDB driver doesn't support view %s yet" % view)


class MongoDB(Database):
    required_props = [DB_CONNECT_STRING, DB_REPLICA_SET, DB_DATABASE, DB_USERNAME, DB_PASSWORD]

    def __init__(self, props):
        from pymongo import MongoClient
        uri = "mongodb://%(username)s:%(password)s@%(hosts)s/?replicaSet=%(replicaSet)s&authSource=%(database)s" % {
            'username': quote_plus(props[DB_USERNAME]),
            'password': quote_plus(props[DB_PASSWORD]),
            'hosts': props[DB_CONNECT_STRING],
            'replicaSet': props[DB_REPLICA_SET],
            'database': props[DB_DATABASE]
        }

        self.client = MongoClient(uri)
        self.database = self.client[props[DB_DATABASE]]
        self.view_helpers = {
            props['DB_WHISK_AUTHS']: SubjectViewHelper()
        }

    # for compatible with couchdb
    def _calculate_revision(self, doc):
        md5 = hashlib.md5()
        md5.update(json.dumps(doc))
        pre_rev = doc.get("_rev", "0-xxx")
        next_rev = int(pre_rev.split("-")[0]) + 1
        return "%d-%s" % (next_rev, md5.hexdigest())

    def get(self, table, doc_id, verbose=False):
        coll = self.database[table]
        res = coll.find_one({"_id": doc_id})
        compatible_status = 200 if res else 404
        response = res if res else {"error": "not found", "reason": "missing"}
        return (res, Response(compatible_status, json.dumps(response)))

    def insert(self, table, doc, verbose=False):
        coll = self.database[table]
        doc['_rev'] = self._calculate_revision(doc)
        coll.replace_one({"_id": doc["_id"]}, doc, upsert=True)
        return Response(201, json.dumps({"response": "201 Created"}))

    def delete(self, table, doc, verbose=False):
        coll = self.database[table]
        res = coll.delete_one({'_id': doc['_id']})
        if res.deleted_count == 1:
            return Response(200, json.dumps({"response": "Deleted"}))
        else:
            return Response(404, json.dumps({"response": "Document: %s not found" % doc['_id']}))

    def list(self, table, view, key, verbose=False):
        coll = self.database[table]
        view_helper = self.view_helpers.get(table)
        if not view_helper:
            raise NotSupportedOperation("this MongoDB driver doesn't support view %s yet" % view)

        filters = view_helper.transform_filter(view, key)
        res = view_helper.transform_view_result(view, key, coll.find(filters))
        return Response(200, json.dumps(res))

    def get_by_view(self, table, view=None, include_docs=False, verbose=False):
        res = None
        if view is None:
            coll = self.database[table]
            if include_docs:
                res = coll.aggregate(
                    [
                        {"$lookup": {"from": table, "localField": "_id", "foreignField": "_id", "as": "doc"}},
                        {"$project": {"key": "$_id", "id": "$_id", "_id": 0, "doc": {"$arrayElemAt": ["$doc", 0]}}}
                    ]
                )
            else:
                res = coll.aggregate([{"$project": {"key": "$_id", "id": "$_id", "_id": 0}}])
        else:
            raise NotSupportedOperation("this MongoDB driver doesn't support to get view")

        print(json.dumps(list(res), sort_keys=True, indent=4, separators=(',', ': ')))
        return 0


class CouchDB(Database):
    required_props = [DB_PROTOCOL, DB_HOST, DB_PORT, DB_USERNAME, DB_PASSWORD]

    def __init__(self, props):
        self.auth     = "%s:%s" % (props[DB_USERNAME], props[DB_PASSWORD])
        self.base_url = "%(protocol)s://%(host)s:%(port)s" % {
            'protocol': props[DB_PROTOCOL],
            'host': props[DB_HOST],
            'port': props[DB_PORT]
        }
        self.headers = {
            'Content-Type': 'application/json',
        }

    def _do_request(self, method, url, body='', verbose=False):
        return request(method, url, headers=self.headers, body=body, auth=self.auth, verbose=verbose)

    def get(self, table, doc_id, verbose=False):
        doc_id = quote_plus(doc_id)
        url = '%(base)s/%(database)s/%(subject)s' % {
            'base'    : self.base_url,
            'database': table,
            'subject' : doc_id
        }

        res = self._do_request('GET', url, verbose=verbose)
        if res.status == 200:
            doc = json.loads(res.read())
            return (doc, Response(res.status, res.read()))
        else:
            return (None, Response(res.status, res.read()))

    def insert(self, table, doc, verbose=False):
        url = '%(base)s/%(database)s' % {
            'base'    : self.base_url,
            'database': table
        }
        body = json.dumps(doc)

        res = self._do_request('POST', url, body=body, verbose=verbose)
        return Response(res.status, res.read())

    def delete(self, table, doc, verbose=False):
        url = '%(base)s/%(database)s/%(docid)s?rev=%(rev)s' % {
            'base'    : self.base_url,
            'database': table,
            'docid'   : quote_plus(doc['_id']),
            'rev'     : doc['_rev']
        }
        res = self._do_request('DELETE', url, verbose=verbose)
        return Response(res.status, res.read())

    def list(self, table, view, key, verbose=False):
        try:
            parts = view.split('/')
            designdoc = parts[0]
            viewname  = parts[1]
        except Exception:
            print('view name "%s" is not formatted correctly, should be design/view' % view)
            return 2

        url = '%(base)s/%(database)s/_design/%(design)s/_view/%(view)s?key=%(key)s' % {
            'base'    : self.base_url,
            'database': table,
            'design'  : designdoc,
            'view'    : viewname,
            'key'     : str(key).replace(' ', '').replace("'", '"')
        }

        res = self._do_request('GET', url, verbose=verbose)

        data = []
        if res.status == 200:
            doc = json.loads(res.read())
            if 'rows' in doc and len(doc['rows']) > 0:
                data = doc['rows']
        return Response(res.status, json.dumps(data))

    def get_by_view(self, table, view=None, include_docs=False, verbose=False):
        if view:
            try:
                parts = view.split('/')
                designdoc = parts[0]
                viewname  = parts[1]
            except Exception:
                print('view name "%s" is not formatted correctly, should be design/view' % view)
                return 2

        url = '%(base)s/%(database)s%(design)s/%(index)s?reduce=false&include_docs=%(docs)s' % {
            'base'    : self.base_url,
            'database': table,
            'design'  : '/_design/' + designdoc + '/_view' if view else '',
            'index'   : viewname if view else '_all_docs',
            'docs'    : 'true' if include_docs else 'false'
        }

        print('getting contents for %s (%s)' % (table, view if view else 'primary index'))
        res = self._do_request('GET', url, verbose=verbose)
        if res.status == 200:
            table = json.loads(res.read())
            print(json.dumps(table, sort_keys=True, indent=4, separators=(',', ': ')))
            return 0
        print('Failed to get database (%s)' % res.read().strip())
        return 1


AVAILABLE_DATABASE_DRIVERS = {
    "couchdb": CouchDB,
    "mongodb": MongoDB
}


def init_driver(database_backend, props):
    try:
        return AVAILABLE_DATABASE_DRIVERS[database_backend.lower()](props)
    except KeyError:
        raise Exception("Not supported database backend: %s" % (database_backend.lower()))
