import time
from implementations import all_implementations
import pandas as pd
import numpy as np
import matplotlib.pyplot as plot


df = pd.DataFrame(columns=['random_array', 'algorithm', 'time_taken'])
for i in range(2**10):
    size = 2**10

    random_array = np.random.rand(size)
    # print(random_array)
    for sort in all_implementations:
        t = np.array([])
        # for j in range(20):
        st = time.time()
        res = sort(random_array)
        en = time.time()
        diff = en - st
        # np.append(t,diff)
        df = df._append({
                'random_array': i,
                'algorithm': sort.__name__,
                'time_taken': diff
            }, ignore_index=True)
# print(df)
df.to_csv("data.csv")
