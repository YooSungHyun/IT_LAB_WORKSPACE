# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 16:25:53 2017

@author: Nikodemos
"""
import tensorflow as tf
import numpy as np

def get_maxval(seq_list):
    temp_val = 0
    for val in seq_list:
        if temp_val < len(val): temp_val = len(val)
    return temp_val

def make_batch(seq_data):
    input_batch = []
    target_batch = []
    
    for seq in seq_data:
        input = [num_dic[n] for n in seq[:-1]]
        input = input + ([0]*(seqmax-len(input)))
        target = num_dic[seq[-1]]
        input_batch.append(np.eye(dic_len)[input])
        target_batch.append(target)
        
    return input_batch, target_batch

def length(sequence):
    used = tf.sign(tf.reduce_max(tf.abs(sequence), 2))
    length = tf.reduce_sum(used, 1)
    length = tf.cast(length, tf.int32)
    return length

#def pad_data()
def reset_graph():
    if 'sess' in globals() and sess:
        sess.close()
    tf.reset_default_graph()

def build_graph(n_hidden,
                n_step,
                n_input,
                n_class,
                dic_len,
                learning_rate):
    
    reset_graph()
#   X[batch size, steps, one-hots], Y[batchsize]<--use the sparse function, no need 1hots
    X = tf.placeholder(tf.float32, [None, n_step, n_input])
    Y = tf.placeholder(tf.int32, [None])
    
    W = tf.Variable(tf.random_normal([n_hidden, n_class]))
    b = tf.Variable(tf.random_normal([n_class]))
    
    cell1 = tf.nn.rnn_cell.BasicLSTMCell(n_hidden)
    cell1 = tf.nn.rnn_cell.DropoutWrapper(cell1, output_keep_prob=0.7)
    cell2 = tf.nn.rnn_cell.BasicLSTMCell(n_hidden)
    
#   create Deep RNN
    multi_cell = tf.nn.rnn_cell.MultiRNNCell([cell1,cell2])
    outputs, states = tf.nn.dynamic_rnn(multi_cell, X, sequence_length=length(X), dtype=tf.float32)
    
#   output = [batch_size, max_time, cell.output_size]
    outputs = tf.transpose(outputs, [1, 0, 2])
    outputs = outputs[-1]
    model = tf.matmul(outputs, W) + b
    
    cost = tf.reduce_mean(
            tf.nn.sparse_softmax_cross_entropy_with_logits(logits = model, labels=Y))
    optimizer = tf.train.AdamOptimizer(learning_rate).minimize(cost)
    
    prediction = tf.cast(tf.argmax(model, 1), tf.int32)
    prediction_check = tf.equal(prediction, Y)
    accuracy = tf.reduce_mean(tf.cast(prediction_check, tf.float32))
    
    return {
        'X' : X,
        'Y' : Y,
        'model' : model,
        'cost' : cost,
        'outputs' : outputs,
        'optimizer' : optimizer,
        'prediction' : prediction,
        'accuracy' : accuracy
        }
    
def train_graph(graph, sess, learning_rate=1e-4, total_epoch=30):
    
        sess.run(tf.global_variables_initializer())
        
        input_batch, target_batch = make_batch(seq_data)
        print(input_batch)
        feed = {g['X']: input_batch, g['Y']: target_batch}
        for epoch in range(total_epoch):
            _, loss = sess.run([g['optimizer'], g['cost']], feed_dict = feed)
#            print(sess.run(length(g['X']),feed_dict={g['X']: input_batch}))
#            print(sess.run(g['outputs'],feed_dict=feed))

            print('Epoch: ', '%04d' % (epoch+1), 'cost: ', '{:.6f}'.format(loss))

def test_graph(graph, sess):
    input_batch, target_batch = make_batch(seq_data)
    predict, accuracy_val = sess.run([g['prediction'], g['accuracy']],
                                     feed_dict = {g['X']: input_batch, g['Y']: target_batch})
    
    predict_words = []
    for idx, val in enumerate(seq_data):
        last_char = char_arr[predict[idx]-1]
        predict_words.append(val[:-1] + last_char)
    
    print('\n=== results ===')
    print('input value: ', [w[:-1] + ' ' for w in seq_data])
    print('predicted value: ', predict_words)
    print('accuracy: ', accuracy_val)


char_arr = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
#seq_data = ['word','wood','deep','dive','cold','cool','load','love','kiss','kind']
#seq_data = ['was','web','cos','sin','tan','keg','can','gap','you','him']
seq_data = ['facts','sapiens','word','wood','art','brain','synaptic']

num_dic = {n: i+1 for i, n in enumerate(char_arr)}
dic_len = len(num_dic)

seqmax = get_maxval(seq_data)-1
#print('num_dic : ',num_dic)
#print('dic_len : ',dic_len)
#input_batch, target_batch = make_batch(seq_data)
#print('input_batch: ',input_batch)
#print('target_batch: ',target_batch)

learning_rate = 0.01
n_hidden = 128
total_epoch = 30

#n_step : 처음 n글자씩 학습(최대 글자수-1)
#input values 와 output values는 원핫인코딩됨으로 dictionary size와 같음
#sparse 계열 함수를 사용하더라도 비교를 위한 예측모델의 출력값은 원핫인코딩을 사용
n_step = seqmax
n_input = n_class = dic_len


g = build_graph(n_hidden, n_step, n_input, n_class, dic_len, learning_rate)

with tf.Session() as sess:
    train_graph(g, sess, learning_rate, total_epoch)
    test_graph(g, sess)
    
