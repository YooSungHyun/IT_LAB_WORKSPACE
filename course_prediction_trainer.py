# -*- coding: utf-8 -*-
"""
Created on Thu Nov 30 14:27:10 2017
# Training Only
# check point directory에 기존 모델이 있는지 확인하고 없으면 epoch 0 부터 새로시작
# last_epoch 변수에 기존 epcoch 수 저장
# 모델을 여러개 동시 경쟁 수행 가능
@author: Nikodemos
"""
import tensorflow as tf
import courses_data
import os
import logging
import logging.config
import json
import argparse


from courses_data_rnn import Model

if __name__ == "__main__":
    
    # get a number of epochs by argument
    parser = argparse.ArgumentParser()
    parser.add_argument("epochs", 
                        help="[integer]number of training epochs", 
                        type=int)
    args = parser.parse_args()

    # hyper parameters
    num_epochs = args.epochs
    
    # preset for logging
    with open('logging.json', 'rt') as f:
        config = json.load(f)
    
    logging.config.dictConfig(config)
    logger = logging.getLogger()
    
    # initialize for tensorflow
    tf.set_random_seed(777)  # reproducibility
    tf.reset_default_graph()    # reset tf graph
    last_epoch = tf.Variable(0, name='last_epoch')  #save the last epochs
    
    data = courses_data.read_data_sets()
    cdict, reverse_cdict = courses_data.load_dict()
    
    CHECK_POINT_DIR = TB_SUMMARY_DIR = './tb/course01/'
    
    batch_size = 256
    state_size = 128
    dict_size = num_classes = len(cdict)
    learning_rate = 1e-3
    

    # load the model(from the courses_data_rnn)
    sess = tf.Session()
    m1 = Model(sess,"m1",
               batch_size,
               state_size,
               dict_size,
               num_classes,
               learning_rate)
    sess.close()
    
    sess.run(tf.global_variables_initializer())
    
    # Create summary writer
    writer = tf.summary.FileWriter(TB_SUMMARY_DIR)
    writer.add_graph(sess.graph)    #add graph to summary writer
    
    # Saver and Restore
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
        
    start_from = sess.run(last_epoch)
    # train my model
    logger.info('Start learning from epoch: '+str(start_from))
    
    tr = data.train
    te = data.test
    
    for epoch in range(start_from, num_epochs):
        avg_cost = 0
        total_batch = int(tr.num_examples / batch_size) 
        assert batch_size < tr.num_examples
        for i in range(total_batch):
            inputs, targets, sqnlens = tr.next_batch(batch_size)
            summary_, cost_, _ = m1.train(inputs, targets, sqnlens)
            avg_cost += (cost_/total_batch)
            writer.add_summary(summary_, i)
        
        logger.info('Epoch:'+str(epoch+1)+'  cost = '+str(avg_cost))
        
        test_batch = int(te.num_examples / batch_size) 
        accuracy = 0
        for j in range(test_batch):
            te_inputs, te_targets, te_sqnlens = te.next_batch(batch_size)
            accuracy_ = m1.get_accuracy(te_inputs, te_targets, te_sqnlens)
            accuracy += (accuracy_/test_batch)

        logger.info("Accuracy : " + str(accuracy))
        
        sess.run(last_epoch.assign(epoch + 1))
        
        if not os.path.exists(CHECK_POINT_DIR):
            os.makedirs(CHECK_POINT_DIR)
        logger.info("Saving network.")
        saver.save(sess, CHECK_POINT_DIR + "/model", global_step=i)
