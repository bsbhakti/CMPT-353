import pandas as pd
import numpy as np
import matplotlib.pyplot as plot
from scipy import stats
from statsmodels.stats.multicomp import pairwise_tukeyhsd


def sample_points(df):
    sample_means = np.array([])
    num_samples = 1000
    sample_size = 1000

# Taking 100 samples of size 50 and calculating their means
    for _ in range(num_samples):
        sample = df.sample(sample_size)
        sample_mean = sample.mean()
        sample_means = np.append(sample_means,sample_mean)
    return sample_means
    


df = pd.read_csv('data.csv')
qs1 = df[df["algorithm"] == "qs1"]["time_taken"]
qs2 = df[df["algorithm"] == "qs2"]["time_taken"]
qs3 = df[df["algorithm"] == "qs3"]["time_taken"]
qs4 = df[df["algorithm"] == "qs4"]["time_taken"]
qs5 = df[df["algorithm"] == "qs5"]["time_taken"]
merge = df[df["algorithm"] == "merge1"]["time_taken"]
partition = df[df["algorithm"] == "partition_sort"]["time_taken"]

qs1.reset_index()
qs2.reset_index()
qs3.reset_index()
qs4.reset_index()
qs5.reset_index()
merge.reset_index()
partition.reset_index()

#np arrays of sampled hopefully normal means
qs1_sampled = sample_points(qs1)
qs2_sampled = sample_points(qs2)
qs3_sampled = sample_points(qs3)
qs4_sampled = sample_points(qs4)
qs5_sampled = sample_points(qs5)
partition_sampled = sample_points(partition)
merge_sampled = sample_points(merge)

# print(qs1_sampled)
# plot.subplot(1,7,1)
# plot.hist(qs1_sampled, edgecolor='k', alpha=0.7)
# plot.subplot(1,7,2)
# plot.hist(qs2_sampled, edgecolor='k', alpha=0.7)
# plot.subplot(1,7,3)
# plot.hist(qs3_sampled, edgecolor='k', alpha=0.7)
# plot.subplot(1,7,4)
# plot.hist(qs4_sampled,  edgecolor='k', alpha=0.7)
# plot.subplot(1,7,5)
# plot.hist(qs5_sampled, edgecolor='k', alpha=0.7)
# plot.subplot(1,7,6)
# plot.hist(partition_sampled,  edgecolor='k', alpha=0.7)
# plot.subplot(1,7,7)
# plot.hist(merge_sampled,  edgecolor='k', alpha=0.7)

# plot.show()

anova = stats.f_oneway(qs1_sampled,qs2_sampled,qs3_sampled,qs4_sampled,qs5_sampled,merge_sampled,partition_sampled)
print(anova)

posthoc = pairwise_tukeyhsd(
    df['time_taken'], df['algorithm'],
    alpha=0.05)

print(posthoc)
fig = posthoc.plot_simultaneous()
# plot.savefig("rank.svg")
plot.show()
