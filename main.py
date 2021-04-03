# Created on Apr 3, 2021
# Author: hmdliu

import os
import pytz
import pandas as pd
from csv_ical import Convert

TIME_ZONE = 'Asia/Shanghai'
INPUT_PATH = './ics/'
OUTPUT_PATH = './csv/'

# Add your own tagging strategies here
TAG_DICT = {
    ('youtube', 'b站'): '#video',
    ('hearthstone'): '#game',
    ('buy'): '#shopping',
    ('coffee'): '#snacks'
}

class Parser:
    def __init__(self, input_path = './ics/', output_path = './csv/', time_zone = 'Asia/Shanghai', tag_dict = {}):
        self.tag_dict = tag_dict
        self.tz = pytz.timezone(time_zone)
        self.input_path = input_path
        self.output_path = output_path
        self.make_path()

        self.ics_list = [file_name for file_name in os.listdir(input_path) if 'ics' in file_name]
        self.convert_raw()

        self.csv_df = pd.DataFrame(columns=['Event', 'ST', 'ET', 'Remark', 'Location', 'Category', 'Duration'])
        self.category_list = [file_name.split('.')[0] for file_name in self.ics_list]
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
        print('[category list]:', self.category_list)
        for category in self.category_list:
            df = pd.read_csv(self.output_path + 'raw/' + category + '.csv', header = None)
            df.rename(columns = {0: 'Event', 1: 'ST', 2: 'ET', 3: 'Remark', 4: 'Location'}, inplace = True)

            df['ST'] = pd.to_datetime(df['ST'], utc = True)
            df['ET'] = pd.to_datetime(df['ET'], utc = True)
            # print(df.dtypes)
            df['ST'] = df['ST'].dt.tz_convert(self.tz)
            df['ET'] = df['ET'].dt.tz_convert(self.tz)   
            # print(df.head())
            
            # Comment these 3 lines if you don't need tags
            df['Event'] = df['Event'].astype(str).str.lower()
            df['Remark'] = df['Remark'].astype(str).str.lower()
            df['Location'] = df['Location'].astype(str).str.lower()
            
            df['Category'] = [category for i in range(len(df))]
            df['Duration'] = (df['ET'] - df['ST']).astype('timedelta64[m]') / 60
            
            self.csv_df = pd.concat([self.csv_df, df], ignore_index=True)
            print('[combine]: category [%s] done' % category)
        
        self.csv_df.sort_values(by = ['ST'], inplace = True, ignore_index = True)
        print('[sort]: done')

        # print(self.csv_df.head())
        # print(self.csv_df.info())

    def tagging(self):
        event_list = list(self.csv_df['Event'])
        tag_list = [[] for i in range(len(event_list))]
        # print(len(event_list), event_list[:5])

        for i in range(len(event_list)):
            for keywords, tag in self.tag_dict.items():
                if isinstance(keywords, str):
                    if event_list[i].find(keywords) != -1:
                        tag_list[i].append(tag)
                elif sum([(event_list[i].find(k) != -1) for k in keywords]):
                    tag_list[i].append(tag)

        tags = [' '.join(tag_list[i]) for i in range(len(event_list))]
        self.csv_df['Tag'] = tags
        print('[tagging]: done')

    def output(self, output_file = 'output.csv', start_date = '2000-01-01', end_date = '2100-01-01'):
        bound = pd.to_datetime(pd.Series([start_date, end_date])).dt.tz_localize(self.tz)
        output_df = self.csv_df[(self.csv_df['ST'] >= bound[0]) & (self.csv_df['ET'] < bound[1])]
        # print(output_df.head())
        with open(self.output_path + output_file, 'w') as f:
            f.write(output_df.to_csv(index = False))
        print('[output]: done')

if __name__ == '__main__':
    p = Parser(INPUT_PATH, OUTPUT_PATH, TIME_ZONE, TAG_DICT)
    p.tagging()
    p.output(start_date = '2020-12-01', end_date = '2021-05-01')