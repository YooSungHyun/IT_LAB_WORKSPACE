# -*- coding: utf-8 -*-
"""
Created on Tue Jan  9 20:06:03 2018

@author: Nikodemos
"""

import courses_data
import tensorflow as tf

import logging
import logging.config
import json

from courses_data_rnn import Model

class PredictionAPI():
    def __init__(self, sess, check_point_dir='./tb/course01/'):
        
        self.sess = sess
        self.CHECK_POINT_DIR = self.TB_SUMMARY_DIR = check_point_dir
        
        self.logger = logger_initializer()
        self.cdict, self.reverse_cdict = courses_data.load_dict()
        self.pmst, self.reverse_pmst = courses_data.get_prdt_mst()
    
        _batch_size = 1
        _state_size = 128
        _learning_rate = 1e-3
        _dict_size = _num_classes = len(self.cdict)
        
        self.model = Model(self.sess,"m1",_batch_size,_state_size,
                           _dict_size,_num_classes,_learning_rate)    
        self.sess.run(tf.global_variables_initializer())
        
        self.sess = self.restore_saver()
    
    def restore_saver(self):
        saver = tf.train.Saver()
        checkpoint = tf.train.get_checkpoint_state(self.CHECK_POINT_DIR)
            
        if checkpoint and checkpoint.model_checkpoint_path:
            try:
                saver.restore(self.sess, checkpoint.model_checkpoint_path)
                self.logger.info("Successfully loaded:"+checkpoint.model_checkpoint_path)
            except:
                self.logger.error("Error on loading old network weights")
        else:
            self.logger.error("Could not find old network weights")
            
    def next_course(self, member_id):
        x, y, sqnlens = courses_data.get_member_course(member_id,prediction_mode=True)
        preds = self.model.predict(x, sqnlens)
        _raw_course = self.cdict[preds.argmax()]
        _course, _offset = _raw_course[:4],_raw_course[-1]
        
        _course_index = self.reverse_pmst[_course]
        nc = []
        for i in range(int(_offset)):
            nc.append(self.pmst[_course_index])
            _course_index += 1
        return nc
        
def logger_initializer():
    with open('logging.json', 'rt') as f:
        config = json.load(f)
    
    logging.config.dictConfig(config)
    lg = logging.getLogger()
    return lg

def reset_graph():
    # initialize for tensorflow
    tf.set_random_seed(777)  # reproducibility
    tf.reset_default_graph()    # reset tf graph
    
if __name__ == "__main__":
    
    reset_graph()
    
    with tf.Session() as sess:
        
        papi = PredictionAPI(sess, check_point_dir='./tb/course01/')
        next_courses = papi.next_course('0052741078')
        print(next_courses)
        
#        x, y, sqnlens = courses_data.get_member_course('0051689801')
#        
#        preds = m1.predict(x, sqnlens)
#        
#        print("real course: ",cdict[y[0]],"\nrecommand course: ", cdict[preds.argmax()])
        