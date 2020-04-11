import time
import json
import warnings
import importlib

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

import pandas as pd

import psycopg2
import psycopg2.extras
import psycopg2.sql
import psycopg2.extensions


class SQL(object):
    def __init__(self, address):
        self.address = address

        _parse = urlparse.urlparse(address)
        self.connection = psycopg2.connect(dbname=_parse.path[1:],
                                           user=_parse.username,
                                           password=_parse.password,
                                           host=_parse.hostname,
                                           port=_parse.port)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

        self.cur_result = None
        return
        
    def __call__(self, request, args=None):
        con = self.connection
        if con.closed:
            con.connect()

        self.cursor.execute(request, args)
        _descr = self.cursor.description
        self.cur_result = self.cursor.fetchall() if _descr is not None else None
        return self
    
    def to_pandas(self):
        if self.cursor.description is None:
            return None

        _keys = [_el[0] for _el in self.cursor.description]
        if len(_keys) > 0:
            _rows = [_row for _row in self.cur_result]
            _rows = _rows if len(_rows) != 0 else None
            # create dataframe
            df = pd.DataFrame(data=_rows, columns=_keys)
        else:
            df = None
        return df
    
    def _repr_html_(self):
        df = self.to_pandas()
        if df is None:
            return "None"
        return df._repr_html_()
    
    @property
    def rowcount(self):
        if self.cur_result is None:
            return -1
        else:
            return len(self.cur_result)

    def show_active_connections(self):
        return self("SELECT * FROM pg_stat_activity")
        
    def close(self):
        self.connection.close()
        return self


class CBStorage(object):
    """
    Computational Block Storage
    %%sql
    drop table if exists tab0;
    create table tab0 (
        id serial not null primary key,
        json jsonb,
        bin bytea,
        create_date timestamp default current_timestamp,
        update_date timestamp default current_timestamp,
        read_date timestamp default current_timestamp
    );
    """
    
    def __init__(self, db_path, table_name):
        self.db_path = db_path
        self.table_name = table_name
        
        self.sql = SQL(db_path)
        self.initialize()
        return

    def initialize(self):
        self.sql(
            "create or replace function check_patch(pl integer, dbpl integer, key text) returns void as $$\n"
            "begin\n"
            "if pl != dbpl then\n"
            "raise exception 'Patch for %: len(%)!=patch start position : %!=%', key, key, dbpl, pl;\n"
            "end if;\n"
            "end; $$ language plpgsql;"
        )
        return

    def close(self):
        self.sql.close()
        return self

    ################################################
    # Storage visualizations
    ################################################

    def select(self, what="*", where=None, order_by=None):
        _request = "select %s from %s" % (what, self.table_name)
        if where is not None:
            _request += " where %s" % where

        if order_by is not None:
            _request += " order by %s" % order_by

        return self.sql(_request)

    def show(self):
        return self.select("*")

    def show_class(self, cls, what="*", where=None, order_by=None):
        _class_name = "%s.%s" % (cls.__module__, cls.__name__)
        _where = "json->>'__classname__' = '%s'" % _class_name
        where = "%s and %s" % (where, _where) if where else _where
        return self.select(what, where, order_by)

    ################################################
    # Save/load methods
    ################################################

    def _load(self, what, block_id):
        query = "update %%(table_name)s set read_date=current_timestamp where id=%%(id_value)s;" \
                "select %s from %%(table_name)s where id=%%(id_value)s;" % what
        _res = self.sql(query, dict(table_name=psycopg2.extensions.AsIs(self.table_name), id_value=block_id))
        if _res.rowcount == 0:
            raise ValueError("No such block_id: %s" % str(block_id))

        return _res

    def load_json(self, block_id):
        return self._load("json", block_id)

    def load_binary(self, block_id):
        return self._load("binary", block_id)

    def load(self, block_id):
        return self._load("*", block_id)

    def pull_patch_props(self, patch_names, last_patches, block_id):
        """
        From json column load patch-props with patch_names updates
            (positions after last_patches)
        """
        _query = "select "
        _lp = last_patches
        for _ind, _pn in enumerate(patch_names):
            _query += "jsonb_path_query_array(json->'%s', '$[%d to LAST]') as %s" % (_pn, _lp[_pn], _pn)
            if _ind < len(patch_names)-1:
                _query += ", "
        _query += " from %s where id=%d;" % (self.table_name, block_id)
        _res = self.sql(_query)
        if _res.rowcount == 0:
            raise ValueError("No such block_id: %s" % str(block_id))
        return _res

    def push_patch_props(self, patches, last_patches, block_id):
        """
        Update json column patch-props from patches. last_patches must be the length
            of patch-props in storage.
        """
        # remove empty updates
        patches = {_k: _val for _k, _val in patches.items() if len(_val) > 0}
        _query = "begin;\n"
        # first we need check-request: db_length == last_patches
        _lp = last_patches
        for _pn in patches:
            _query += "select check_patch(%d, jsonb_array_length(json->'%s'), '%s')"\
                      " from %s where id=%d;\n" % \
                      (_lp[_pn], _pn, _pn, self.table_name, block_id)
        for _pn in patches:
            _query += "update %s set "\
                      "json=jsonb_set(json, '{%s}', json->'%s' || %%(%s)s)"\
                      "where id=%d;\n" % (self.table_name, _pn, _pn, _pn, block_id)
        _query += "commit;"
        _res = self.sql(_query, {_pn: psycopg2.extras.Json(_val) for _pn, _val in patches.items()})
        return

    def save_json(self, block_json, block_id=None):
        """
        Parameters
        ==========
        block_json: object
            object we can dump to json format
        block_id: int or None
        """
        if block_id is None:
            query = "begin; insert into %s (json)" \
                    " values (%%(json_value)s) returning id; commit;" % self.table_name
            ids = self.sql(query, dict(json_value=psycopg2.extras.Json(block_json))).to_pandas()
            return ids['id'].values
        else:
            query = "begin; update %s set json=%%(json_value)s, "\
                    "update_date=current_timestamp where id=%%(block_id)s; commit;" % self.table_name
            self.sql(query, dict(json_value=psycopg2.extras.Json(block_json),
                                 block_id=block_id))
            return [block_id]

    def save_binary(self, block_binary, block_id=None):
        """
        Parameters
        ==========
        block_binary: binary str
            binary data converted to string
        block_id: int or None
        """
        if block_id is None:
            query = "begin; insert into %s (bin)" \
                    " values (%%(bin_value)s) returning id; commit;" % self.table_name
            ids = self.sql(query, dict(bin_value=psycopg2.Binary(block_binary))).to_pandas()
            return ids['id'].values
        else:
            query = "begin; update %s set bin=%%(bin_value)s, "\
                    "update_date=current_timestamp where id=%%(block_id)s; commit;" % self.table_name
            self.sql(query, dict(bin_value=psycopg2.Binary(block_binary),
                                 block_id=block_id))
            return [block_id]

    def save(self, block_json, block_binary, block_id=None):
        """
        Parameters
        ==========
        block_json: object
            object we can dump to json format
        block_binary: binary str
            binary data converted to string
        block_id: int or None

        Notes
        =====
        make available lists in json and binary
        """
        if block_id is None:
            query = "begin; insert into %s (json, bin)" \
                    " values (%%(json_value)s, %%(bin_value)s) returning id; commit;" % self.table_name
            ids = self.sql(query, dict(json_value=psycopg2.extras.Json(block_json),
                                       bin_value=psycopg2.Binary(block_binary))).to_pandas()
            return ids['id'].values
        else:
            query = "begin; update %s set json=%%(json_value)s, bin=%%(bin_value)s, "\
                    "update_date=current_timestamp where id=%%(block_id)s; commit;" % self.table_name
            self.sql(query, dict(json_value=psycopg2.extras.Json(block_json),
                                 bin_value=psycopg2.Binary(block_binary),
                                 block_id=block_id))
            return [block_id]

    ################################################
    # Storage manipulations
    ################################################

    def create_storage(self):
        self.sql("""
        create table if not exists %s (
        id bigserial not null primary key,
        json jsonb,
        bin bytea,
        create_date timestamp default current_timestamp,
        update_date timestamp default current_timestamp,
        read_date timestamp default current_timestamp        
        );
        """ % self.table_name)
        return self
    
    def clear_storage(self):
        self.sql("begin; delete from %s where id>-1; alter"
                 "alter sequence %s_id_seq restart with 1; commit;" %
                 (self.table_name, self.table_name))
        return self

    def delete_ids(self, id_list):
        id_list = ", ".join([str(_id) for _id in id_list])
        self.sql("delete from %s where id in (%s);" % (self.table_name, id_list))
        return self


##############################################
# Computational block section
##############################################

def calculate_timer(func):
    def wrapped(self, *args, **kwargs):
        _start = time.time()
        _res = func(self, *args, **kwargs)
        self.time = time.time() - _start
        return _res
    return wrapped


class ComputationalBlock(object):
    # computational block properties names list
    cb_props = set([])
    patch_props = set([])

    def __init__(self, storage):
        self.storage = storage
        self.id_history = []
        self.id = None
        self.time = None

        self._updates = None

        # property for managing patch-updates (pulls, pushes)
        self._last_patches = {_k: 0 for _k in self.__class__.patch_props}
        return

    def get_patch_props_json(self, updates_only=True):
        _lp = self._last_patches
        cls = self.__class__
        if updates_only:
            return {_k: getattr(self, _k)[_lp[_k]:] for _k in cls.patch_props}
        return {_k: getattr(self, _k) for _k in cls.patch_props}

    def _update_last_patches(self, names=None):
        names = self.__class__.patch_props if names is None else names
        _lp = self._last_patches
        for _k in names:
            _lp[_k] = len(getattr(self, _k))
        return

    def _set_patch_props(self, patches, updates_only=True):
        cls = self.__class__
        for _k, _p in patches.items():
            assert _k in cls.patch_props
            if updates_only:
                getattr(self, _k).extend(_p)
            else:
                setattr(self, _k, _p)
        return self

    def push_patch_props(self, names=None):
        names = self.__class__.patch_props if names is None else names
        _pp = self.get_patch_props_json(updates_only=True)
        _pp = {_k: _pp[_k] for _k in names}
        self.storage.push_patch_props(_pp, self._last_patches, block_id=self.id)
        self._update_last_patches(names=names)
        return self

    def pull_patch_props(self, names=None):
        names = self.__class__.patch_props if names is None else names
        _pp = self.storage.pull_patch_props(names, self._last_patches, block_id=self.id)
        _pp = _pp.to_pandas()
        _pp = {_n: _pp[_n][0] for _n in names}
        self._set_patch_props(_pp, updates_only=True)
        self._update_last_patches(names=names)
        return self

    def _pre_save(self, update=True):
        _id = self.id
        if not update and _id is not None:
            self.id_history.append(_id)
            _id = self.id = None
        return _id

    def save_json(self, update=True):
        _id = self._pre_save(update=update)
        self._update_last_patches()
        self.id = int(self.storage.save_json(
            self.get_json(), block_id=_id)[0])
        return self

    def save_binary(self, update=True):
        _id = self._pre_save(update=update)
        self.id = int(self.storage.save_binary(
            self.get_binary(), block_id=_id)[0])
        return self

    def save(self, update=True):
        _id = self._pre_save(update=update)
        self._update_last_patches()
        self.id = int(self.storage.save(
            self.get_json(), self.get_binary(),
            block_id=_id)[0])
        return self

    def _repr_html_(self):
        return self.to_pandas()._repr_html_()
    
    @classmethod
    def load(cls, storage, block_id, strict=True, full=True):
        block_id = int(block_id)
        res = storage.load(block_id).to_pandas()
        _json = res['json'][0]
        _json['id'] = block_id
        _bin = res['bin'][0].tobytes()
        _obj = cls.from_json_binary(storage, _json, _bin,
                                    strict=strict, full=full)
        _obj._update_last_patches()
        return _obj

    def test(self):
        _json = self.get_json()
        _bin = self.get_binary()
        new_cb = self.__class__.from_json_binary(self.storage, _json, _bin)
        return new_cb.get_json(to_str=True) == json.dumps(_json) and new_cb.get_binary() == _bin

    ############################################
    # Methods to implement/reimplement
    ############################################
    
    @classmethod
    def from_json_binary(cls, storage, block_json, block_binary, strict=True, full=True):
        class_name = "%s.%s" % (cls.__module__, cls.__name__)
        if block_json["__classname__"] != class_name:
            if strict:
                raise ValueError("Trying to load class: '%s' with '%s'" %
                                 (block_json["__classname__"], class_name))
            else:
                warnings.warn("Trying to load class: '%s' with '%s'" %
                              (block_json["__classname__"], class_name))

        obj = cls(storage)
        for _key in block_json:
            if hasattr(obj, _key):
                setattr(obj, _key, block_json[_key])
            elif _key not in ["__classname__", "__commit__", "__version__"]:
                warnings.warn("ComputationalBlock: ignore key '%s'" % _key)

        # load computational block properties
        if full:
            for _el in cls.cb_props:
                obj_el = getattr(obj, _el)
                id = obj_el["id"]
                cls_name = obj_el["__classname__"]
                _split = cls_name.split(".")
                _module_name = ".".join(_split[:-1])
                _cls_name = _split[-1]
                module = importlib.import_module(_module_name)
                klass = getattr(module, _cls_name)
                setattr(obj, _el, klass.load(storage, id))

        return obj
 
    def get_version(self):
        return ["", 0, 0, 0]

    def get_commit(self):
        return ""

    def get_json(self, to_str=False):
        _cls = self.__class__
        _res = {
            "__classname__": "%s.%s" % (_cls.__module__, _cls.__name__),
            "__version__": self.get_version(),
            "__commit__": self.get_commit(),
            "id": self.id,
            "id_history": self.id_history,
            "time": self.time
        }

        # add computational block properties json info
        for _el in self.__class__.cb_props:
            _res[_el] = getattr(self, _el).get_json(to_str=False)

        if to_str:
            return json.dumps(_res)
        return _res
        
    def get_binary(self):
        return b""
        
    def to_pandas(self):
        return pd.DataFrame({_k: [_v] for _k, _v in self.get_json().items()})

    @calculate_timer
    def calculate(self):
        return self


class CBUpdate(object):
    def __init__(self, storage, block_id):
        self.storage = storage
        self.block_id = block_id

        self.json = None
        self.binary = None
        return

    def load(self):
        res = self.storage.load(self.block_id).to_pandas()
        self.json = res['json'][0]
        self.binary = res['bin'][0]
        return self

    def save(self):
        self.storage.save(self.json, self.binary, block_id=self.block_id)
        return self

    def update(self, save=False):
        if self.json is None and self.binary is None:
            self.load()

        _update = self.make_update()
        if _update:
            self.json["_update"] = self.make_update()

            if save:
                self.save()

            return self
        else:
            return None

    def to_pandas(self):
        return pd.DataFrame({_k: [_v] for _k, _v in self.json.items()})

    def _repr_html_(self):
        return self.to_pandas()._repr_html_()

    ############################################
    # Methods to implement/reimplement
    ############################################
    
    def make_update(self):
        """
        We think, that we can only read self.json, self.binary.
        """
        _update = {}
        return _update
