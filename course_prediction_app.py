# -*- coding: utf-8 -*-
#"""
#    <Script Info.>
#    GUI Application 클래스
#    Instance 생성시 CNN Model Instance 전달 받음.,
#    50x50 Image 입력 -> 배열 변환 -> 모델 적용(predict함수)
#    
#    <Methods>
#    __init__ : 초기화 시 CNN 모델 적용
#    arange() : 화면 구성
#    binding_widget : 화면 입력 위젯과 이벤트 연결
#    take_snapshot() : Image Array를 모델에 Query
#    clear() : Canvas, Image 모두 초기화
#    b1up(), b1down(), motion() : Canvas, Image 입력
#    destructor() : 화면 root 삭제(마지막 호)
#"""
import tkinter as tk
import numpy as np

import courses_data

import logging
import logging.config
import json

class Application:
    def __init__(self, model):
        
        self.logger = self.load_logger()
        self.logger.info('Load the App')
        
        self.model = model
        
        self.arrange()
        self.entry_member_id.focus()
        self.entry_member_id.insert(0,"0051689801")
        
        self.cdict, self.reverse_cdict = courses_data.load_dict()
        
        self.x = None
        self.y = None
        self.sqnlens = None
        
    def load_logger(self):
        # preset for logging
        with open('logging.json', 'rt') as f:
            config = json.load(f)
        
        logging.config.dictConfig(config)
        lg = logging.getLogger()
        return lg
        
    def arrange(self):
        
        self.root = tk.Tk()  # initialize root window
        self.root.title("E-level Math. Course Prediction")  # set window title
        # self.destructor function gets fired when the window is closed
        self.root.protocol('WM_DELETE_WINDOW', self.destructor)
        
        self.frame01 = tk.Frame(self.root)
        self.frame01.pack()
        
        self.lbl01 = tk.Label(self.frame01, text='Member id.', width=10)
        self.lbl01.pack(side='left', padx=5, pady=5)
        
        self.entry_member_id = tk.Entry(self.frame01, width=10, text='0051689801')
        self.entry_member_id.pack(side='left', padx=5, pady=5)
        
        self.button_member_id = tk.Button(self.frame01, text='Load Course Datas', command=self.load_course_datas)
        self.button_member_id.pack(side='left', padx=5, pady=5)
        
        self.btn_clear = tk.Button(self.frame01, text="Clear", command=self.clear)
        self.btn_clear.pack(fill="both", expand=True, padx=5, pady=5)
        
        self.frame02 = tk.Frame(self.root)
        self.frame02.pack()
        
        self.text_input_datas = tk.Text(self.frame02, width=120, height=50, padx=5, pady=5)
        self.text_input_datas = tk.Text(self.frame02)
        self.text_input_datas.pack()
        
        self.frame03 = tk.Frame(self.root)
        self.frame03.pack()
        
        self.lbl02 = tk.Label(self.frame03, text='Predicted Course:', width=13)
        self.lbl02.pack(side='left', padx=5, pady=5)
        self.sv_predicted = tk.StringVar(self.frame03)
        
        self.entry_predidcted_value = tk.Entry(self.frame03, width=5, textvariable=self.sv_predicted)
        self.entry_predidcted_value.pack(side='left', padx=5, pady=5)
        
        self.lbl03 = tk.Label(self.frame03, text='Actual Course:', width=13)
        self.lbl03.pack(side='left', padx=5, pady=5)
        self.sv_actual = tk.StringVar(self.frame03)
        self.entry_real = tk.Entry(self.frame03, width=5, textvariable=self.sv_actual)
        self.entry_real.pack(side='left', padx=5, pady=5)
        
        self.frame04 = tk.Frame(self.root)
        self.frame04.pack()
        
        self.btn_clear = tk.Button(self.frame04, text="Apply & Predict", command=self.predict_course)
        self.btn_clear.pack(fill="both", expand=True, padx=5, pady=5)

    def load_course_datas(self):
        member_id = self.entry_member_id.get()
        self.x, self.y, self.sqnlens = courses_data.get_member_course(member_id)
        
        self.logger.debug("X : " + str(self.x))
        self.logger.debug("Y : " + str(self.y))
        self.logger.debug("SQNLENS : " + str(self.sqnlens))
        
#        list_courses_ = []
#        for course in self.x[0]:
#            list_courses_.append(str(self.cdict[course]))
#        print(list_courses_)
#        txt_x = np.asarray(list_courses_)
#        str_x = np.array2string(txt_x,separator='|')
#        self.text_input_datas.insert('insert', str_x)
        
        str_courses = ""
        for course in self.x[0]:
            str_courses += str(self.cdict[course])
            str_courses += ','
        str_courses = str_courses[:-1]
        self.text_input_datas.insert('insert', str_courses)

    def predict_course(self):
#        self.botton_clear()
        self.entry_predidcted_value.delete(first=0, last=100)
        self.entry_real.delete(first=0, last=100)
        text_course_ = self.text_input_datas.get('1.0','end')
        text_course_ = text_course_.replace("\n","")
        
#        Text Widget의 내용으로 예측진도를 다시 돌림. 나중을 위해
#        Text Widget의 String을 seperator를 기준으로 분리하여 numpy array에 적재
#        진도->진도ID로 변환하고 int형태로 변환 후 batch 형태의 2차원 array로 변환
        input_datas_ = np.array(text_course_.split(','))
        for i, data in enumerate(input_datas_):
            input_datas_[i] = int(self.reverse_cdict[data])
        input_datas_ = input_datas_.astype(int)
        input_datas_ = np.expand_dims(input_datas_, axis=0)
        sqnlens_ = [input_datas_[0].shape[0]]
        
#        print(input_datas_)
#        print(self.x)
#        print(sqnlens_)
#        print(self.sqnlens)
        
        preds = self.model.predict(self.x, self.sqnlens)
        preds = self.model.predict(input_datas_, sqnlens_)
        self.sv_predicted.set(self.cdict[preds.argmax()])
        self.sv_actual.set(self.cdict[self.y[0]])
        
    def clear(self):
        self.entry_member_id.delete(first=0, last=100)
        self.entry_predidcted_value.delete(first=0, last=100)
        self.entry_real.delete(first=0, last=100)
        self.sv_predicted.set('')
        self.sv_actual.set('')
        self.text_input_datas.delete('1.0', 'end')
        self.x, self.y, self.sqnlens = None, None, None
        
    def botton_clear(self):
        self.entry_predidcted_value.delete(first=0, last=100)
        self.entry_real.delete(first=0, last=100)
        self.sv_predicted.set('')
        self.sv_actual.set('')
        
    def destructor(self):
        """ Destroy the root object and release all resources """
        self.logger.info('Close the App.')
        self.root.destroy()

if __name__ == "__main__":
    
    pba = Application(None)
    pba.root.mainloop()