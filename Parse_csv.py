import os, sys, csv
import pandas as pd
import numpy as np

files = "D:/修課/資料探勘/HW1/Dali_pm25.csv"

i = 0
data = pd.read_csv(files, sep=',', chunksize= 465318, header=None, encoding='utf-8')

for ct in data:
    #print(ct.head(10))
    #print("=================================")   
    ct = ct.replace(np.nan, 0)
    for i in range(3, len(ct.columns)):
        for j in ct.index:
            ct[i][j] = str(ct[i][j])
            if "-" or "*" or "#" or "x" in ct[i][j]:
                ct[i][j] = ct[i][j].replace("-", "")
                ct[i][j] = ct[i][j].replace("*", "")
                ct[i][j] = ct[i][j].replace("#", "")
                ct[i][j] = ct[i][j].replace("x", "")
            # elif ct[i][j] == "":
            #     ct[i][j] = ct[i][j].replace(np.nan, 0)
    
    print(ct.head(10))   
    ct.to_csv('D:/修課/資料探勘/HW1/Clean_pm25.csv', index=False,encoding='utf-8')    
    


