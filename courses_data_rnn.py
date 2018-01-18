# -*- coding: utf-8 -*-
#"""
#Created by Nikodemos
#2017/12/22 15:20
#"""
import tensorflow as tf

class Model:

    def __init__(self, sess, name,
                 batch_size = 100,
                 state_size = 128,
                 dict_size = 5000,
                 num_classes = 5000,
                 learning_rate = 1e-3):
        
        # Session and model ID
        self.sess = sess
        self.name = name

        # Hyper parameters
        self.batch_size = batch_size
        self.state_size = state_size
        self.dict_size = dict_size
        self.num_classes = num_classes
        self.learning_rate = learning_rate
       
        self._build_graph()
        self.merged_all()
        
    def _build_graph(self):
        with tf.variable_scope(self.name):

            # Placeholders
            self.x = tf.placeholder(tf.int32, [self.batch_size, None]) # [batch_size, num_steps]
            self.sqnlen = tf.placeholder(tf.int32, [self.batch_size])
            self.y = tf.placeholder(tf.int32, [self.batch_size])
            self.keep_prob = tf.placeholder_with_default(1.0,[])
    
            # Embedding layer
            embeddings = tf.get_variable('embedding_matrix', [self.dict_size, self.state_size])
            rnn_inputs = tf.nn.embedding_lookup(embeddings, self.x)
        
            # RNN
            cell = tf.nn.rnn_cell.GRUCell(self.state_size)
            init_state = tf.get_variable('init_state', [1, self.state_size],
                                         initializer=tf.constant_initializer(0.0))
            init_state = tf.tile(init_state, [self.batch_size, 1])
            rnn_outputs, final_state = tf.nn.dynamic_rnn(cell, rnn_inputs, sequence_length=self.sqnlen,
                                                         initial_state=init_state)
    
            # Add dropout, as the model otherwise quickly overfits
            rnn_outputs = tf.nn.dropout(rnn_outputs, self.keep_prob)
        
            idx = tf.range(self.batch_size)*tf.shape(rnn_outputs)[1] + (self.sqnlen - 1)
            last_rnn_output = tf.gather(tf.reshape(rnn_outputs, [-1, self.state_size]), idx)
    
            # Softmax layer
            with tf.variable_scope('softmax'):
                W = tf.get_variable('W', [self.state_size, self.num_classes])
                b = tf.get_variable('b', [self.num_classes], initializer=tf.constant_initializer(0.0))
                tf.summary.histogram("weights", W)
                tf.summary.histogram("bias", b)
            
            self.logits = tf.matmul(last_rnn_output, W) + b
            
            with tf.name_scope("loss"):
            # Compute cross entropy as our loss function
                self.loss = tf.reduce_mean(tf.nn.sparse_softmax_cross_entropy_with_logits(logits=self.logits, labels=self.y))
                tf.summary.scalar("loss", self.loss)
        
            with tf.name_scope("train"):
            # Use an AdamOptimizer to train the network
                self.optimizer = tf.train.AdamOptimizer(self.learning_rate).minimize(self.loss)
            
            with tf.name_scope("accuracy"):
                self.preds = tf.nn.softmax(self.logits)
                correct_prediction = tf.equal(tf.cast(tf.argmax(self.preds,1),tf.int32), self.y)
                self.accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))
                tf.summary.scalar("accuracy", self.accuracy)
            
    @property
    def get_logits(self):
        return self.logits
    
    # Summary for tensorboard
    def merged_all(self):
        self.merged = tf.summary.merge_all()
        
    def predict(self, x_test, sqnlen_data, keep_prop=1.0):
        return self.sess.run(self.preds, feed_dict={self.x: x_test, self.sqnlen: sqnlen_data, self.keep_prob: keep_prop})

    def get_accuracy(self, x_test, y_test, sqnlen_data, keep_prop=1.0):
        return self.sess.run(self.accuracy, feed_dict={self.x: x_test, self.y: y_test, self.sqnlen: sqnlen_data, self.keep_prob: keep_prop})

    def train(self, x_data, y_data, sqnlen_data, keep_prop=0.7):
        return self.sess.run([self.merged, self.loss, self.optimizer], feed_dict={
            self.x: x_data, self.y: y_data, self.sqnlen: sqnlen_data, self.keep_prob: keep_prop})
            
#if __name__ == "__main__":
#    sess = tf.Session()
#    m1 = Model(sess, "m1",
#               batch_size = 100,
#               state_size = 128,
#               dict_size = 5000,
#               num_classes = 5000,
#               learning_rate = 1e-3)
#    sess.close()
    
