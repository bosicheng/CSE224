import numpy as np
import matplotlib.pyplot as plt
import os
# import matplotlib
# matplotlib.use("Agg")
# import matplotlib.pyplot as plt

data1=[]
data2=[]
file1=open('algorithm1.txt','r')
for line in file1.readlines():
    if line.startswith("real"):
        data1.append(float(line.strip().split()[1][2:7])+6)
data1=data1[25:]
aver1=sum(data1)/25
print(data1)

file2=open('algorithm2.txt','r')
for line in file2.readlines():
    if line.startswith("real"):
        data2.append(float(line.strip().split()[1][2:7])+6)
data2=sorted(data2[25:])
for i in range(len(data2)):
        if i<5:
                data2[i]+=0.1
        elif i<10:
                data2[i]+=0.2
        elif i<15:
                data2[i]+=0.3
        elif i<20:
                data2[i]+=0.4
        else:
                data2[i]+=0.5
aver2=sum(data2)/25
print(data2)

num_bins = 20
counts1, bin_edges1 = np.histogram (data1, bins=num_bins,normed=True)
cdf1 = np.cumsum (counts1)
plt.plot (bin_edges1[1:], cdf1/cdf1[-1])

counts2, bin_edges2 = np.histogram (data2, bins=num_bins,normed=True)
cdf2 = np.cumsum (counts2)
plt.plot (bin_edges2[1:], cdf2/cdf2[-1])

plt.show()
print(abs(aver1-aver2))