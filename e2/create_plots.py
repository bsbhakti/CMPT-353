import sys
import pandas as pd
import matplotlib.pyplot as plt

filename1 = sys.argv[1]
filename2 = sys.argv[2]

file1_data = pd.read_csv(f"./{filename1}",sep=" ",header=None,index_col=1,
        names=['lang', 'page', 'views', 'bytes'])
file2_data = pd.read_csv(f"./{filename2}",sep=" ",header=None,index_col=1,
        names=['lang', 'page', 'views', 'bytes'])

file1_data.sort_values(by='views',ascending=False,inplace=True)

combined_file = pd.DataFrame()
combined_file["page1"] = file1_data["views"]
combined_file["page2"] = file2_data["views"]
print(combined_file)


plt.figure(figsize=(10, 5)) # change the size to something sensible
plt.subplot(1, 2, 1) # subplots in 1 row, 2 columns, select the first
plt.plot(file1_data['views'].values) # build plot 1
plt.xlabel("Index")
plt.ylabel("Hour 1 views")
plt.subplot(1, 2, 2) # ... and then select the second
plt.plot(combined_file["page1"],combined_file["page2"],'b.')
plt.xscale('log')  
plt.yscale('log')  
plt.xlabel("Hour 1 views")
plt.ylabel("Hour 2 views")
plt.savefig('wikipedia.png')
del file1_data, file2_data






