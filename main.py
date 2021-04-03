# Created on Apr 3, 2021
# Author: hmdliu

import os
import pytz
import pandas as pd
from csv_ical import Convert

TIME_ZONE = 'Asia/Shanghai'
INPUT_PATH = './ics/'
OUTPUT_PATH = './csv/'

class Parser:
    def __init__(self, input_path = './ics/', output_path = './csv/', time_zone = 'Asia/Shanghai'):
        self.tz = pytz.timezone(time_zone)
        self.input_path = input_path
        self.output_path = output_path
        self.make_path()

        self.ics_list = [file_name for file_name in os.listdir(input_path) if 'ics' in file_name]
        self.convert_raw()

        self.csv_df = pd.DataFrame(columns=['Event', 'ST', 'ET', 'Remark', 'Location', 'Category', 'Duration'])
        self.csv_list = [file_name for file_name in os.listdir(output_path + 'raw/') if 'csv' in file_name]
        self.combine_csv()

    def make_path(self):
        if not os.path.isdir(self.input_path):
            os.makedirs(self.input_path)
            print('[Warning]: No such an input directory.')
            exit(0)
        if not os.path.isdir(self.output_path + 'raw/'):
            os.makedirs(self.output_path + 'raw/')

    def convert_raw(self):
        print('[ics list]:', self.ics_list)
        if len(self.ics_list) == 0:
            print('[Warning]: No ics file in the given directory.')
            exit(0)
        for file_name in self.ics_list:
            convert = Convert()
            convert.read_ical(self.input_path + file_name)
            convert.make_csv()
            convert.save_csv(self.output_path + 'raw/' + file_name.split('.')[0] + '.csv')
            print('[raw convertion]: [%s] done' % (file_name.split('.')[0] + '.csv'))
        
    def combine_csv(self):
        print('[csv list]:', self.csv_list)
        for file_name in self.csv_list:
            df = pd.read_csv(self.output_path + 'raw/' + file_name, header = None)
            df.rename(columns = {0: 'Event', 1: 'ST', 2: 'ET', 3: 'Remark', 4: 'Location'}, inplace = True)

            df['ST'] = pd.to_datetime(df['ST'], utc = True)
            df['ET'] = pd.to_datetime(df['ET'], utc = True)
            # print(df.dtypes)
            df['ST'] = df['ST'].dt.tz_convert(self.tz)
            df['ET'] = df['ET'].dt.tz_convert(self.tz)   
            # print(df.head())
            
            category = file_name.split('.')[0]
            df['Category'] = [category for i in range(len(df))]
            df['Duration'] = (df['ET'] - df['ST']).astype('timedelta64[m]') / 60
            
            self.csv_df = pd.concat([self.csv_df, df], ignore_index=True)
            print('[combine]: category [%s] done' % category)
        
        self.csv_df.sort_values(by = ['ST'], inplace = True, ignore_index = True)
        print('[sort]: done')

        # print(self.csv_df.head())
        # print(self.csv_df.info())

    def output(self, output_file = 'output.csv'):
        with open(self.output_path + output_file, 'w') as f:
            f.write(self.csv_df.to_csv(index = False))
        print('[output]: done')

if __name__ == '__main__':
    p = Parser(INPUT_PATH, OUTPUT_PATH, TIME_ZONE)
    p.output()