# -*- coding: utf-8 -*-
"""
@author: YSH
"""
import os
import tensorflow as tf
import numpy as np
import sys


def get_data_filenames(dirname):
    r"""dirname 파라미터로 들어온 경로에 모든 bsv 파일 경로를 얻는다.

      Args:
        dirname: BSV 파일이 있을 것으로 예상되는 String 형태의 디렉토리 경로를 넣는다.
      Returns:
        string 타입의 bsv 파일 경로가 나온다.
      """
    filenames = os.listdir(dirname)
    file_list = []
    for filename in filenames:
        full_filename = os.path.join(dirname, filename)
        ext = os.path.splitext(full_filename)[-1]
#        ext = os.path.splitext(filename)[-1]
        if ext == '.bsv':
            file_list.append(full_filename)
#            print(full_filename)
    return file_list

def process_list_column(list_column, dtype=tf.float32, delim=','):
    r"""Tensor(List) 형태로 들어온 값을 딜리미터로 잘라서 dtype 형태로 변환한다.
        현재 소스에서는 사용하지 않는다. 동작은 잘 되지만, SparseTensor 형태의 return값을 활용하기엔
        레퍼런스도 많지 않고, 지원하는 메소드도 협소해 사용자가 원하는 형태로써의 가공이 매우 힘들다.

      Args:
        list_column: 딜리미터로 된 string의 텐서노드의 리스트가 들어간다.
        dtype: Return될 값의 자료형을 결정한다.
        delim : split할 문자열 default=','
      Returns:
        ',' 딜리미터로 전부 split된 dtype 파라미터의 SparseTensor가 나온다.
      """
    sparse_strings = tf.string_split(list_column, delimiter=delim)
    return tf.SparseTensor(indices=sparse_strings.indices,
                         values=tf.string_to_number(sparse_strings.values,
                                                    out_type=dtype),
                         dense_shape=sparse_strings.dense_shape)


def read_feed_data(file_path_name, delim='|'):
    r"""파일을 읽어, Delimiter로 쪼갠 텐서 노드를 생성하고, 배치사이즈만큼 돌면서 큐를 이용해 파일을 읽어온다.
        읽어온 최종 데이터는 Numpy Array형태가 된다.

    Args:
        file_path_name : 변환하고자하는 string 파일경로.
        delim : split할 문자열 default='|'
    Returns:
        2x2의 '|' Splited ndarray (dtype = String)
        [[X_Y_Data, sqnlen]]
        returnArray[n][0] = X_Y_DATA
        returnArray[n][1] = sqnlen
    """

    QUEUE_LENGTH = 10
    BATCH_SIZE = 10
    TOTAL_DATA_ROW_CNT = 10
    TOTAL_BATCH = int(TOTAL_DATA_ROW_CNT / BATCH_SIZE)

    totData = np.array([["", ""]], dtype=np.string_)  # split 데이터가 쌓일 총 데이타 Numpy array (2-D Array)
    record_defaults = [[""]] * 2

    try:
        # 1. BSV파일을 읽어온다.
        csv_file = tf.train.string_input_producer(file_path_name, name='filename_queue')
        textReader = tf.TextLineReader()
        _, line = textReader.read(csv_file)

        # 2. 읽어온 Line 데이터를 deilm으로 split한다.
        #    이때, line_data에는 [[콤마로 된 X_Y_DATA string][sqnlen]]형태가 완성된다.
        line_data = tf.decode_csv(line, record_defaults=record_defaults, field_delim=delim)
        batch_data = tf.train.batch([line_data], batch_size=BATCH_SIZE) # 배치 사이즈 만큼 나눠서 읽어올 것임.

        # 3. 속도를 높이기 위한 Queue를 사용한다. 쓰레드와 함께 사용해야지 의미가 있다.
        #    SparseTensor를 이용하지 못하는 첫번째 이유이다. FIFOQueue안에 들어갈 many값(enqueue_many)은 꼭 Tensor노드 여야한다.
        #    SparseTensor는 엄연히 텐서 노드들을 행렬의 형태로 구성해놓은 집합체와 같은 자료형이어서, enqueue_many에 파라미터로 직접 쓰지 못한다.
        q = tf.FIFOQueue(QUEUE_LENGTH, dtypes=[tf.string])
        enq_ops = q.enqueue_many([batch_data])
        qr = tf.train.QueueRunner(q, [enq_ops])

        # 4. 1,2,3번에서 만들어놓은 텐서 노드들을 실행하기 위한 Session을 만들고, 쓰레드를 이용하여 값을 읽어온다.
        with tf.Session() as sess:
            sess.run(tf.global_variables_initializer())
            # try:

            # 쓰레드를 이용한 queue 처리.
            coord = tf.train.Coordinator()
            threads = tf.train.start_queue_runners(sess=sess, coord=coord, start=True)

            # 배치 사이즈만큼 Tensor Node의 batch_data를 읽어와서, Numpy Array에 쌓는다.
            for i in range(TOTAL_BATCH + 1):
                # 매 배치가 돌때마다 [배치사이즈, 2(X_Y_DATA, sqnlen)] 형태의 배열을 얻을 수 있다.
                # 우리는 모든 배치데이터가 쌓인 총 배열을 알고 싶으므로, totData에 전부 쌓도록(vstack) 한다. 2-D totData ([ [X_Y_DATA,sqnlen]...[X_Y_DATA,sqnlen]])
                totData = np.vstack((sess.run(batch_data), totData))
            print('stop batch')

            # 스레드의 종료
            coord.request_stop()
            coord.join(threads)
            # except:
            #    print("Unexpected error 2:", sys.exc_info()[0])

        # Numpy Array는 2차원 이상의 경우 무조건 초기값을 요구한다.
        # vstack으로 쌓는경우, 초기값 위에 쌓게 되므로 맨 최종에는 빈 초기 데이터가 남게 되는데 해당 데이터를 삭제하여 깔끔하게 읽어온 데이터만 사용한다.
        totXY = np.delete(totData, np.where(totData == [b'', b'']), axis=0)  # Default totData Value '' is deleted
    except:
        print ("Unexpected error read_feed_data:", sys.exc_info()[0])
        exit()

    return totXY

def ndarraySplitPadZero(sourceArray, padsize, delim=','):
    r"""Numpy Array를 받아와 delimiter로 자르고 Zero Padding을 실시한다.

    Args:
        sourceArray : split하고 padding해야되는 string문자열이 들어있는 Numpy Array
        padsize : 패딩을 몇자리까지 붙힐지
                  ex) [1,2,3] padsize=10 -> [1,2,3,0,0,0,0,0,0,0]
    Returns:
        split한 뒤 Zero Padding까지 완성된 Numpy Array가 나온다.
    """
    rslt_x = np.array([0 for idx in range(padsize)], dtype=np.int32)
    rslt_y = np.array([], dtype=np.int32)
    rslt_sqnlen = np.array([], dtype=np.int32)
    # Numpy에서의 split은 List 형태의 자료를 똑똑하게 split해주지는 못하고, Line형태의 한줄 String만 처리가 가능하다.
    # List형태의 자료를 똑똑하게 split하는 것은 텐서플로우 메소드를 이용하는 process_list_column 메소드로는 가능하나, 사용하지 못하는 이유는 주석으로 남겨놓았다.
    # for문으로 모든 라인에 대해서 split하기 때문에, split과 padding에는 속도가 느리지 않으나, 수 많은 for문을 진행하는 곳에서 속도가 매우 저하되는 문제가 있다.
    for i in range(len(sourceArray)):
        split_x_y_line = np.array(np.chararray.split(sourceArray[i][0],delim).tolist(),dtype=np.int32)

        x_line = split_x_y_line[0:-1]
        y_line = split_x_y_line[-1]
        sqnlen_line = sourceArray[i][1]

        split_pad_x_line = np.pad(x_line, (0, padsize - len(x_line)), 'constant')
        print('processing : ', i/len(sourceArray)*100,'%')
        rslt_x = np.vstack((split_pad_x_line, rslt_x))
        rslt_y = np.append(y_line, rslt_y)
        rslt_sqnlen = np.append(sqnlen_line, rslt_sqnlen)
    return rslt_x[0:-1,:], rslt_y, rslt_sqnlen.astype(np.int32)   # rslt_x_array에서 맨마지막 행을 제외하는 이유는 마지막 행에 default 0인 방이 들어가므로 제외.

if __name__ == "__main__":
    print('File Data Input Start!!')

    # 사용법
    # 1. 파일이름을 전부 불러온다.
    file_path_name=get_data_filenames(os.path.join(os.path.realpath( os.path.dirname(__file__) ),'data/'))
    print(file_path_name)

    # 2. 배치를 돌려 배열을 얻는다
    totXY = read_feed_data(file_path_name,'|')

    # 4. x데이터, y데이터, sqnlen을 학습시키고자하는 형식에 맞춘다.
    x, y, sqnlen = ndarraySplitPadZero(totXY, 600, b',')