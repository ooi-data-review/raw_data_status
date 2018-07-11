#! /usr/local/bin/python

"""
Created on Mon Feb 06 2017
@author: leilabbb
"""

import pandas as pd
import glob
import os
import time
import numpy as np

start_time = time.time()

'''
This script combines all/or selected ingestion sheets found in GitHub repo:
https://github.com/ooi-integration/ingestion-csvs
# used for: (1) output file created on
#           (2) check raw data files created today
'''
# timestamp the output files
datein = pd.to_datetime('today')
created_on = datein.strftime("%d-%m-%Y")

# select a site
site_name = 'Pioneer'
# select a platform
ingest_key = 'CP01CNPM'   #CNSM CNPM PMCI PMCO'
numi = 8 #use 14, 8  or 3
# locate data file
main = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/file_count/' + site_name + '/'


# path to local copy of ingestion-csvs repo
rootdir = '/Users/leila/Documents/OOI_GitHub_repo/repos/ooi-integration/ingestion-csvs/'
# select the ingestion file example _D00003_ingest.csv or leave it as generic _ingest.csv
ingestion_file = '_ingest.csv' #'_ingest.csv'
# path to data file on the raw data repo
dav_mount = '/Volumes/dav/'
# path modification variables to match to system data file paths/
splitter = '/OMC/'
splitter_C = '/rsn_data/DVT_Data/'
splitter_CC = '/RSN/'

skip_list = ['CE02SHBP', 'CE04OSBP', 'CE04OSPD', 'CE04OSPS','CP05MOAS-AV00#']

mooring_header = ['parser', 'filename_mask', 'reference_designator', 'data_source', 'status', 'notes',
                  'Automated_status', 'number_files', 'file of today', 'file <= 1k', 'file > 1K', 'last file date']

status_check = ['Missing']
status_list_missing = ['Not Deployed', 'Not Expected', 'Expected','Missing']
status_list_available = ['Pending', 'Available']
# create output file
#outputfile = main + ingest_key + '/'

dirnames = os.listdir(rootdir)
rm_dirs = [dirnames.remove(i) for i in skip_list]

plt_list = filter(lambda x: x[0:numi] == ingest_key, dirnames)
path_list = [(rootdir + i) for i in plt_list if os.path.isdir(os.path.join(rootdir, i))]


# start script

for ii in range(len(plt_list)):
    platform = path_list[ii].split('/')[-1]
    filenames = os.listdir(path_list[ii])
    df = pd.DataFrame()
    for f in filenames:
        csv_file = os.path.join(path_list[ii], f)
        if csv_file.endswith(ingestion_file):
            filereader = pd.read_csv(csv_file)
            print(csv_file)
            if 'Unnamed: 4' in filereader.columns:
                filereader = filereader.rename(columns={'Unnamed: 4': 'status'})
            # remove rows with empty Reference Designators
            filereader.dropna(subset=['reference_designator'], inplace=True)

            # remove rows with empty cells
            filereader.dropna(how="all", inplace=True)

            # replace NAN by empty string
            filereader.fillna('', inplace=True)

            #  add to data frame --> add the file name as a column
            filereader['number_files'] = ''
            filereader['file <= 1k'] = ''
            filereader['file > 1K'] = ''
            filereader['file of today'] = ''
            filereader['last file date'] = ''
            filereader['Automated_status'] = ''

            file_num = []
            size1kless = []
            size1kplus = []
            todayfile = []
            filedate = []
            statusfile = []

            index_i = 0
            for row in filereader.itertuples():
                assigned_status = [row.status for i in status_check if i == row.status]
                #print(row.status,assigned_status, len(assigned_status))
                if len(assigned_status) is not 0:
                    if row.filename_mask is not '':
                        try:
                            try:
                                web_dir = os.path.join(dav_mount, row.filename_mask.split(splitter)[1])
                            except IndexError:
                                try:
                                    web_dir = os.path.join(dav_mount, row.platform, row.filename_mask.split(splitter_C)[1])
                                except IndexError:
                                    web_dir = os.path.join(dav_mount, row.filename_mask.split(splitter_CC)[1])

                            file_match = glob.glob(web_dir)

                            compare_2_date = datein.strftime("%d/%m/%Y")
                            file_1K_size = 0
                            file_1Kplus_size = 0
                            file_of_today = 0
                            kk = 0
                            file_modified = ''
                            for file in file_match:
                                if os.path.isfile(file):
                                    file_modified = pd.to_datetime(time.ctime(os.path.getmtime(file)))
                                    file_modified = file_modified.strftime("%d/%m/%Y")
                                    get_size = os.path.getsize(file)

                                    if get_size <= 1024:  # 513410:
                                        file_1K_size += 1

                                    if get_size > 1024:
                                        file_1Kplus_size += 1

                                    if file_modified == compare_2_date:
                                        file_of_today += 1
                                else:
                                    print(file)

                                kk += 1

                            size1kless.append(str(file_1K_size))
                            size1kplus.append(str(file_1Kplus_size))
                            todayfile.append(str(file_of_today))


                            num_files = len(file_match)

                            if num_files == 0:

                                automated_status_missing = [row.status for i in status_list_missing if i == row.status]
                                if len(automated_status_missing) == 0:
                                    statusfile.append('New')
                                else:
                                    statusfile.append(automated_status_missing[0])

                                filedate.append('')

                            else:

                                automated_status_available = [row.status for i in status_list_available if i == row.status]
                                if len(automated_status_available) == 0:
                                    statusfile.append('New')
                                else:
                                    statusfile.append(automated_status_available[0])

                                filedate.append(str(file_modified))

                            num_files_text = str(num_files)
                            file_num.append(num_files_text)

                        except AttributeError:
                            kk = 0
                            file_1K_size = 0
                            file_1Kplus_size = 0
                            file_of_today = 0
                            file_modified = ''

                            web_dir = row.filename_mask
                            num_files_text = 'file_mask w attribute error'
                            size1kless.append('file_mask w attribute error')
                            size1kplus.append('file_mask w attribute error')
                            todayfile.append('file_mask w attribute error')
                            file_num.append('file_mask w attribute error')
                            filedate.append('file_mask w attribute error')
                            statusfile.append('')

                            #print web_dir, '.....',  'file_mask w attribute error'
                    else:
                        kk = 0
                        file_1K_size = 0
                        file_1Kplus_size = 0
                        file_of_today = 0
                        file_modified = ''
                        web_dir = row.filename_mask
                        num_files_text = 'file_mask is empty'
                        size1kless.append('file_mask is empty')
                        size1kplus.append('file_mask is empty')
                        todayfile.append('file_mask is empty')
                        file_num.append('file_mask is empty')
                        filedate.append('file_mask is empty')
                        automated_status = [row.status for i in status_list_missing if i == row.status]
                        if len(automated_status) == 0:
                            statusfile.append('Missing')
                        else:
                            statusfile.append(automated_status[0])

                    # move back one space to go back to original code that check all status not only rowns with status == Missing


                    filereader['number_files'][row.Index] = file_num[index_i]
                    filereader['file <= 1k'][row.Index] = size1kless[index_i]
                    filereader['file > 1K'][row.Index] = size1kplus[index_i]
                    filereader['file of today'][row.Index] = todayfile[index_i]
                    filereader['last file date'][row.Index] = filedate[index_i]
                    filereader['Automated_status'][row.Index] = statusfile[index_i]


                    #print(row.Index, index_i, "--->", web_dir, ' : ')
                    #print('           number of files =', file_num[index_i], kk)
                    #print('           file <= 1k =', size1kless[index_i]) #file_1K_size
                    #print('           file > 1K =', size1kplus[index_i])#file_1Kplus_size
                   # print('           file of today =', todayfile[index_i])#file_of_today
                   # print('           file date =', filedate[index_i])  # last file's date

                    index_i += 1
                else:
                    filereader['number_files'][row.Index] = '-999'
                    filereader['file <= 1k'][row.Index] = '-999'
                    filereader['file > 1K'][row.Index] = '-999'
                    filereader['file of today'][row.Index] = '-999'
                    filereader['last file date'][row.Index] = '-999'
                    filereader['Automated_status'][row.Index] = '-999'


            # append all sheets in one file
            df = df.append(filereader)
            df = df[df.Automated_status.str.contains('-999') == False]

           #
    total_rows = len(df.axes[0])
    total_cols = len(df.axes[1])
    #print(total_rows, total_cols, df.index)

    if total_rows is not 0:
        print('NOTE: file should have been created')
        if not os.path.exists(main + platform ):
            os.makedirs(main + platform )

        outputfile = main + platform + '/' + platform +  created_on + '_rawfiles_query' + ingestion_file.split('_ingest.csv')[0] +'.csv'
        df.to_csv(outputfile, index=False, columns=mooring_header, na_rep='NaN', encoding='utf-8')

    print "time elapsed: {:.2f}s".format(time.time() - start_time)