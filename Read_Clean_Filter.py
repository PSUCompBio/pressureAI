#!/usr/bin/env python
# coding: utf-8

# In[ ]:

#import boto3
import s3fs #S3Fs is a Pythonic file interface to S3. It builds on top of botocore.
import matplotlib.pyplot as plt
import numpy as np
import math
import pandas as pd
import os
import h5py


# In[ ]:


#Define root path
fs = s3fs.S3FileSystem()
#client=boto3.client('s3')
root = "s3://blast-sim-data/"
#root = "s3://blast-sim-data/drdc"


# In[ ]:


# Define first set of data to read
dataset = "SCENARIO_TRAINER_THESIS/"
#dataset = "dataset2"
root_folder1 = os.path.join(root, dataset) #join the paths root and dataset
print('Dataset Location = ',root_folder1)
files = fs.ls(root_folder1); #ls - list the contents of the current directory ("ls" is short for "list") python
nfiles = len(files);

#files1 = files[1:nfiles];
files1 = files[1:10];
files1[:] 


# In[ ]:


# check for empty or errounous files
NonEmptyFileList =[]
for j in files1:
    #print(j)
    tmp =[]
    skip = 0;
    fullpathj = os.path.join('s3://', str(j))
    if j == 'blast-sim-data/drdc/dataset2/viper3d_th_overpressure_X13_100g_2m_Remap Files.txt' or         j == 'blast-sim-data/drdc/dataset2/viper3d_th_overpressure_X13_100g_2m_Viper Files.txt' : 
        skip = 1
    #tmp = pd.read_csv(fullpathj, delim_whitespace=True, header=None)  
    #print(len(tmp)
    if skip == 0:
        NonEmptyFileList.append(j)
        #print('Adding ',j)
    else:
        print('Skipping File: ',j)


# In[ ]:


# Important Variables 
# number of scenarios that will be processed. Each scenario should be 
# a different file from viper.
nscenarios = NonEmptyFileList
print('nscenarios = ',len(nscenarios))
# number of sensors per person (nspp) specified on face (does include the senssors worn)
nspp = 13 # for Jackson's simulation
#nspp = 5 # for drdc's simulations
#nspp = 4 # for Vikram's simulations
# Number of Sensors Worn (nsw) by person
nsw = 3 # chest, shoulder, helmet for B3
# Peak duration extraction (time for which pressure is above x% of max)
PeakDurationExtraction = 0.5
FilterPressureThreshold = 1e7
print('Filter Pressure Threshold = ',FilterPressureThreshold)


# In[ ]:


def create_df(scenarios,root_folder):
    all_max_values = []
    all_max_durations = []
    AllPeakInformation = []
    StartEndIndices= {}
    OverPressureDuration = []
    combinedNames = []
    NewColumnNames = []
    npeople = []
    OverPressureDurationAll = pd.DataFrame()
    MaxDurations = pd.DataFrame()
    MaxPressureValuesAll = pd.DataFrame()
    MaxDurationValuesAll = pd.DataFrame()
    combinedDf = pd.DataFrame()
    ScenarioCounter = 0
    SaveScenarioCounter = 0

    # each file represents a different scenario. each scenario could have 
    # multiple people (npeople) with with multiple tracer locations or
    # number of sensors per person (nspp)
    for scenario in scenarios:
        ScenarioFileName = os.path.basename(scenario)
        ScenarioFilePath = os.path.join(root_folder,ScenarioFileName)
        df = []
        df = pd.read_csv(ScenarioFilePath, delim_whitespace=True, header=None)                
        check_nan_in_df = df.isnull().values.any()
        print (check_nan_in_df)
        # remove time (the first column)
        df = df.drop(df.columns[[0]], axis=1)  # df.columns is zero-based pd.Index
        #print('df = ',df)
        # read only 1st column to store time trace 
        time = pd.read_csv(ScenarioFilePath,delim_whitespace=True,usecols=[0], header=None)
        #convert array to list
        #time = time.values.tolist()
        time = np.array(time)
        #print("time", time)
        row, col = df.shape
        #print("row, col : ",row, col)
        #print('nspp = ',nspp)
        #print(int(col/nspp))
        npeople.append(int(col/nspp))
        #print(npeople[ScenarioCounter])
    
    
        #print("df shape",df.shape)
        #print(df)
        # makes single array with chest thresholds for each person, each sensor
        # basically half value of maximum in each column in df
        thresholds=PeakDurationExtraction*df.max(axis=0)        
        # number of timepoints (number of rows in viper file)
        #print(time)
        ntimepoints=len(time)
        #print('ntimepoints = ',ntimepoints)
    
        # Initalize Arrays
        OverPressureDuration = []
        OverpressureDurationAll = []
        MaxPressureValues = []
        MaxDurationValues =[]
        frames = []
        SkipScenario = 0
        print('npeople in Scenario ',ScenarioCounter,' = ',npeople[ScenarioCounter])
        # FILTER: COMPUTE MAX PRESSURES to Filter by maximum pressure threshold
        for j in range(0,int(npeople[ScenarioCounter])):
            #print('person: ',j)
            for k in range (0,nspp):
                # Compute the max pressure values for this column,store only the pressures for this person
                CurrentMaxPressure = df.iloc[:,j*nspp+k].max()
                if CurrentMaxPressure >= FilterPressureThreshold:
                    SkipScenario = 1
                    PersonFilterAlert = j
                    #print('FILTER ALERT: Scenario ',ScenarioCounter,' Person ',j, ' Sensor ',k,' max pressure has exceeded the Filter Pressure Threshold!!!')
        
        if SkipScenario == 0:
            plt.plot(time,df.iloc[:,1])  
            # FIRST COMPUTE MAX PRESSURES
            for j in range(0,int(npeople[ScenarioCounter])):
                tmp = []
                for k in range (0,nspp):   
                    # Compute the max pressure values for this column,store only the pressures for this person
                    tmp.append(df.iloc[:,j*nspp+k].max())
                # Add pressures from this person to the total pressure list
                MaxPressureValues.append(tmp)

            ### NOW COMPUTE DURATIONS
            for j in range(0,int(npeople[ScenarioCounter])):
                tmp = []
                for k in range (0,nspp):   
                    start_index = -1
                    end_index   = -1
                    for i in range(0, ntimepoints):
                        if (df.iloc[i,j*nspp+k]>thresholds.iloc[j*nspp +k] and start_index == -1):
                            start_index=i  
                            #break
                    for i in range(start_index, ntimepoints):   
                        if df.iloc[i,j*nspp+k]<thresholds.iloc[j*nspp +k] and end_index == -1:
                            #print('min detected !','sensor ',k+1, ' i index = ',i)
                            end_index=i  
                            #break
                    #print('start_index = ', start_index)
                    #print('end_index = ', end_index)
                    StartEndIndices[j*nspp+k+0] = start_index
                    StartEndIndices[j*nspp+k+1] = end_index
                    tmp.append(time[end_index][0] - time[start_index][0])
                MaxDurationValues.append(tmp)
                #print('MaxDurationValues = ', MaxDurationValues)

            print('finished processing scenario ',ScenarioCounter,'...')
            # save pressures into single arrary, first convert to dataframe
            MaxPressureValuesDF = pd.DataFrame(MaxPressureValues)
            MaxPressureValuesAll = pd.concat([MaxPressureValuesAll,MaxPressureValuesDF],ignore_index=True)
            # save pressures into single arrary, first convert to dataframe
            MaxDurationValuesDF = pd.DataFrame(MaxDurationValues)
            MaxDurationValuesAll = pd.concat([MaxDurationValuesAll, MaxDurationValuesDF],ignore_index=True)
            #print('MaxPressureValuesDF = ', MaxPressureValuesDF)
            ScenarioCounter = ScenarioCounter + 1
            SaveScenarioCounter = SaveScenarioCounter + 1
            #print('MaxPressureValuesAll = ', MaxPressureValuesAll)
            #print('MaxDurationValuesAll = ', MaxDurationValuesAll)
        else: 
            print('FILTER ALERT for Person',PersonFilterAlert,'; Pressure Exceeded....skipping data save.')
            ScenarioCounter = ScenarioCounter + 1
    
    # ------------------------------------------------------------------------
    # At this point all the filtered data has been saved in the MaxPressure and MaxDuration dataframes
    # ------------------------------------------------------------------------
    
    #create column names for pressure df
    for k in range (0,nspp):
        # these numbers 0 1 2 may need to be modified based on nsw
        if k == 0:
            NewColumnNames.append('Chest-P')
        elif k == 1: 
            NewColumnNames.append('Shoulder-P')
        elif k == 2:
            NewColumnNames.append('Helmet-P')
        elif k > nsw-1:
            NewColumnNames.append('Sensor-'+str(k)+'-P')
    MaxPressureValuesAll.columns=NewColumnNames
    NewColumnNames = [] 

    for k in range (0,nspp):
        #print(str(col)+'P')
        # these numbers 0 1 2 may need to be modified based on nsw
        if k == 0:
            NewColumnNames.append('Chest-D')
        elif k == 1: 
            NewColumnNames.append('Shoulder-D')
        elif k == 2:
            NewColumnNames.append('Helmet-D')
        elif k > nsw-1:
            NewColumnNames.append('Sensor-'+str(k)+'-D')           
    MaxDurationValuesAll.columns=NewColumnNames
    
    AllPeakInformation=pd.concat([MaxPressureValuesAll, MaxDurationValuesAll], axis=1)
    #print(AllPeakInformation)
    l1 = MaxDurationValuesAll.columns
    l2 = MaxPressureValuesAll.columns
    #print('l1',l1)
    #print('l2',l2)
    colNames = zip(l2, l1)
    #print(colNames)
    combinedNames = [name for pair in colNames for name in pair]
    AllPeakInformation = AllPeakInformation[combinedNames]
    #print(AllPeakInformation)
    print('\n\n------------------------------\n------------------------------\n',SaveScenarioCounter,' out of ',ScenarioCounter,'scenarios were saved.\n------------------------------\n------------------------------\n')
    return AllPeakInformation, MaxPressureValuesAll, MaxDurationValuesAll


# In[ ]:


#df1, peak_df1, max_values_df1, max_durations_df1 = create_df (files1,root_folder1)
AllPeakInformation, MaxPressureValuesAll, MaxDurationValuesAll = create_df (files1,root_folder1)
#print(MaxPressureValuesAll)
#print("Duration")
#print(MaxDurationValuesAll)


hf = h5py.File('ProcessedData-DRDC-Dataset2-CleanFiltered.h5', 'w')
hf.create_dataset('AllPeakInformation', data=AllPeakInformation)
hf.create_dataset('MaxPressureValuesAll', data=MaxPressureValuesAll)
hf.create_dataset('MaxDurationValuesAll', data=MaxDurationValuesAll)
hf.close()

