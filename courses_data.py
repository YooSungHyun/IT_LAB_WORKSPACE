# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 21:28:39 2017
@author: Nikodemos
"""

import cx_Oracle
import pandas as pd
import numpy as np
from tensorflow.contrib.learn.python.learn.datasets import base
from tensorflow.python.framework import dtypes

class DataSet(object):

    def __init__(self,
                 inputs,
                 targets,
                 sqnlens,
                 dtype=dtypes.int32):

        dtype = dtypes.as_dtype(dtype).base_dtype
        if dtype not in (dtypes.int32, dtypes.int64):
            raise TypeError('Invalid image dtype %r, expected int32 or int64' %dtype)
        self._num_examples = inputs.shape[0]
        
        self._inputs = inputs
        self._targets = targets
        self._sqnlens = sqnlens
        self._epochs_completed = 0
        self._index_in_epoch = 0
        pass
    
    @property
    def inputs(self):
        return self._inputs

    @property
    def targets(self):
        return self._targets
    
    @property
    def sqnlens(self):
        return self._sqnlens
    
    @property
    def num_examples(self):
        return self._num_examples

    @property
    def epochs_completed(self):
        return self._epochs_completed
    
    def next_batch(self, batch_size):
        start = self._index_in_epoch
        self._index_in_epoch += batch_size
        if self._index_in_epoch > self._num_examples:
            # Finished epoch
            self._epochs_completed += 1
            # Shuffle the data
            perm = np.arange(self._num_examples)
            np.random.shuffle(perm)
            self._inputs = self._inputs[perm]
            self._targets = self._targets[perm]
            self._sqnlens = self._sqnlens[perm]
            # Start next epoch
            start = 0
            self._index_in_epoch = batch_size
            assert batch_size <= self._num_examples
        end = self._index_in_epoch
        return self._inputs[start:end], self._targets[start:end], self._sqnlens[start:end]

def _connect_db(db_sid):
    if db_sid == 'CRM':
        _ip = '203.233.63.250'
        _port = 1521
        _sid = 'CRM'
        _id = 'crm'
        _passwd = 'crm'
    elif db_sid == 'DKQ':
        _ip = '203.233.63.202'
        _port = 1527
        _sid = 'DKQ'
        _id = 'FED'
        _passwd = 'sdman'
    dsn_tns = cx_Oracle.makedsn(_ip, _port, _sid)
    con = cx_Oracle.connect(_id, _passwd, dsn_tns)
    return con

def _get_dataframe(qry, con):
    df_ora = pd.read_sql(qry, con=con)
    return df_ora

def load_courses(shuffle=True):
    connection = _connect_db('DKQ')
#    query = """SELECT CSIDS, SEQLEN FROM YHD_MBR_UPLOAD where rownum < 1000"""
    query = """SELECT CSIDS, SEQLEN FROM YHD_MBR_UPLOAD"""
    df = _get_dataframe(query, connection)
    connection = None
    return df

def load_course(member_id):
    connection = _connect_db('DKQ')
    param = {'kunnr':member_id}
    query = "SELECT CSIDS, SEQLEN FROM YHD_MBR_UPLOAD WHERE KUNNR=:kunnr"
    df = pd.read_sql(query,connection,params=param)
    connection = None
    return df

def _shuffle_df(df):
    shuffled_df = df.sample(frac=1).reset_index(drop=True)
    return shuffled_df
    
def load_dict():
    connection = _connect_db('DKQ')
    query = """SELECT COURSE_STEP, CSID, CNT FROM YHD_COURSESTEP_DICT"""
    df = _get_dataframe(query, connection)
    df_len = df.shape[0]
    
    cdict = {}
    reverse_cdict = {}
    for i in range(df_len):
        cdict[df['CSID'].values[i]]=df['COURSE_STEP'].values[i]
        reverse_cdict[df['COURSE_STEP'].values[i]]=df['CSID'].values[i]
    df = None
    connection = None
    return cdict, reverse_cdict

def load_grade_dict():
    grade_list = ['K1','K2','K3','K4','K5','K6',
                  'P1','P2','P3','P4','P5','P6',
                  'M1','M2','M3','H1','H2','H3','A']
    gdict = {}
    reverse_gdict = {}
    for i, gl in enumerate(grade_list):
        gdict[i] = gl
        reverse_gdict[gl] = i
        
    return gdict, reverse_gdict

def split_data(inputs, targets, sqnlens):
    
    train_portion = 0.9
    test_portion = 0.08
        
    max_len = inputs.shape[0]

    tr_len = np.floor(max_len*train_portion).astype(int)
    te_len = np.floor(max_len*test_portion).astype(int)

    tr_inputs, tr_targets, tr_sqnlens = inputs[:tr_len], targets[:tr_len], sqnlens[:tr_len]
    te_inputs, te_targets, te_sqnlens = inputs[tr_len:tr_len+te_len], targets[tr_len:tr_len+te_len], sqnlens[tr_len:tr_len+te_len]
    vl_inputs, vl_targets, vl_sqnlens = inputs[tr_len+te_len:], targets[tr_len+te_len:], sqnlens[tr_len+te_len:]
    
    tr = DataSet(tr_inputs, tr_targets, tr_sqnlens)
    te = DataSet(te_inputs, te_targets, te_sqnlens)
    vl = DataSet(vl_inputs, vl_targets, vl_sqnlens)
#    print(tr,te,vl)
    return tr, te, vl

def extract_data(df, forward_sequences=False):
    maxlen = df['SEQLEN'].max()
    inputs=[]
    targets=[]
    sqnlens=[]

    for i, csid in enumerate(df['CSIDS']):
        csid = list(map(int,csid.split(",")))
#        진행방향대로 자르기
#        ex) 4A01,4A02,4A03 ==> [4A01],[4A01,4A02],[4A01,4A02,4A03]
        if forward_sequences == True:
            for j in range(len(csid)-1):
                _x = csid[:j+1]+([0]*(maxlen-j-2))  #input에 대한 sequence수이므로, 전체 길이에서 1이 아닌 2를 뺀다.
                _y = csid[j+1]

                inputs.append(_x)
                targets.append(_y)
                sqnlens.append(len(_x))
        else:
            _seq = df['SEQLEN'].values[i]
            _x = csid[:_seq-1]+([0]*(maxlen-_seq))
            _y = csid[-1]  #끝만 가지고 올
            inputs.append(_x)
            targets.append(_y)
            sqnlens.append(len(_x))    #xdata가 하나씩 모자랄테니 길이를 보정
    return np.asarray(inputs), np.asarray(targets), np.asarray(sqnlens)
    
def read_data_sets(shuffle=True):
    df = load_courses()
    if shuffle:
        df = _shuffle_df(df)
    inputs, targets, sqnlens = extract_data(df, forward_sequences=False)
    train, test, validation = split_data(inputs, targets, sqnlens)
    
    return base.Datasets(train=train, validation=validation, test=test)
    
def get_member_course(member_id, prediction_mode=False):
    connection = _connect_db('DKQ')
    param = {'kunnr':member_id}
    query = """
        SELECT 
            KUNNR, ZPROC_SEQ,
            COURSE_STEP,
            NVL((SELECT CSID FROM YHD_COURSESTEP_DICT WHERE COURSE_STEP = T2.COURSE_STEP),0) AS CSID
        FROM (
            SELECT 
                KUNNR, ZPROC_SEQ,
                CASE WHEN LENGTH(ZPROC)=1 THEN ZPROC||' ' ELSE ZPROC END ||
                CASE WHEN LENGTH(ZSETNO)=1 THEN '0'||ZSETNO ELSE ZSETNO END ||
                (SELECT MAX(ZDTL_SEQ) FROM ZSD7_STDPROC_DTL WHERE MANDT=T1.MANDT AND KUNNR=T1.KUNNR AND MATNR=T1.MATNR AND ZPROC_SEQ=T1.ZPROC_SEQ) AS COURSE_STEP
            FROM ZSD7_STDPROC_DTL T1 
            WHERE MANDT='300' AND MATNR='M'
            AND KUNNR=:kunnr
            AND ZDTL_SEQ='1'
        )T2 ORDER BY KUNNR, ZPROC_SEQ
        """
    
    df = pd.read_sql(query,connection,params=param)
    connection = None
    maxlen = df['ZPROC_SEQ'].shape[0]-1
#    print(maxlen)
    inputs = []
    targets = []
    sqnlens = [maxlen]

    for i, csid in enumerate(df['CSID']):
        if prediction_mode:
            inputs.append(csid)
        else:
            if i < maxlen:
                inputs.append(csid)
            else:
                targets.append(csid)
    
    inputs = np.asarray(inputs)
    inputs = np.expand_dims(inputs,axis=0)
    targets = np.asarray(targets)
    sqnlens = np.asarray(sqnlens)
    
    return inputs, targets, sqnlens

def get_prdt_mst():
    connection = _connect_db('DKQ')
    query = """
        SELECT 
            CASE WHEN LENGTH(ZPROC)=1 THEN ZPROC||' ' ELSE ZPROC END    AS ZPROC,
            TO_NUMBER(ZEDSETNO) AS ZEDSETNO
        FROM zsd7_prdt_proc WHERE MANDT='300' AND MATNR='M' AND ZSTDAREA_ID='1'
        ORDER BY ZTXT_SEQ
        """
    df = _get_dataframe(query, connection)
    connection = None
    
    prdt_mst, reverse_prdt_mst = {}, {}
    cnt = 0
    for i, zproc in enumerate(df['ZPROC']):
        for j in range(df['ZEDSETNO'].values[i]):
            formatted_setno = zproc+"%02d"%(j+1,)
            prdt_mst[cnt] = formatted_setno
            reverse_prdt_mst[formatted_setno] = cnt
            cnt += 1
    return prdt_mst, reverse_prdt_mst
    
if __name__ == "__main__":
#    pm, rpm = get_prdt_mst()
#    print(pm, rpm)
#
    batch_size = 100
    num_epochs = 2
#    
#    start = time.time()
    
#    get_member_course('H438020553')
#    inputs, targets, sqnlen = get_member_course('0054119200')
#    print(inputs, targets, sqnlen)
    
#    df = load_course('0054273437')
#    x, y, sqnlen = extract_data(df)
#    print(x, y, sqnlen)
    
    data = read_data_sets(shuffle=True)
    print(data.train.next_batch(3))
#    
#    total_batch = int(data.train.num_examples / batch_size) 
#    assert batch_size < data.train.num_examples
#    print(total_batch, data.train.num_examples)
#    for i in range(num_epochs):
#        for j in range(total_batch):
#            inputs, targets, sqnlens = data.train.next_batch(batch_size)
#            print(i, j, inputs, targets, sqnlens)
#            print(i, j, inputs.shape, targets.shape, sqnlens.shape)
    
#    end = time.time() - start     # end에 코드가 구동된 시간 저장
#    
#    print(end,'secs. elapsed')