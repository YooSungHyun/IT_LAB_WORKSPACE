# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 13:35:58 2017

@author: Nikodemos
"""
import courses_data
import tensorflow as tf

import logging
import logging.config
import json

from course_prediction_app import Application
from courses_data_rnn import Model

if __name__ == "__main__":
    
    with open('logging.json', 'rt') as f:
        config = json.load(f)
    
    logging.config.dictConfig(config)
    logger = logging.getLogger()

    cdict, reverse_cdict = courses_data.load_dict()
    
    CHECK_POINT_DIR = TB_SUMMARY_DIR = './tb/course01/'
    
    # initialize for tensorflow
    tf.set_random_seed(777)  # reproducibility
    tf.reset_default_graph()    # reset tf graph
    
    batch_size = 1
    state_size = 128
    learning_rate = 1e-3
    num_epochs = 1
    dict_size = num_classes = len(cdict)
    
    with tf.Session() as sess:
        
        m1 = Model(sess,"m1",
                   batch_size,
                   state_size,
                   dict_size,
                   num_classes,
                   learning_rate)
        sess.run(tf.global_variables_initializer())
        
        saver = tf.train.Saver()
        
        checkpoint = tf.train.get_checkpoint_state(CHECK_POINT_DIR)
        
        if checkpoint and checkpoint.model_checkpoint_path:
            try:
                saver.restore(sess, checkpoint.model_checkpoint_path)
                logger.info("Successfully loaded:"+checkpoint.model_checkpoint_path)
            except:
                logger.error("Error on loading old network weights")
        else:
                    logger.error("Could not find old network weights")
        
        pba = Application(m1)
        pba.root.mainloop()

#        x, y, sqnlens = courses_data.get_member_course('0051689801')
#        
#        preds = m1.predict(x, sqnlens)
#        
#        print("real course: ",cdict[y[0]],"\nrecommand course: ", cdict[preds.argmax()])
        