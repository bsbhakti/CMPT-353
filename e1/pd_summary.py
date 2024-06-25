import pandas as pd
def pd_summary():
    data = pd.read_csv('totals.csv').set_index(keys=['name'])
    counts = pd.read_csv('counts.csv').set_index(keys=['name'])

    sum1 = data.sum(axis=1)
    lowest = sum1.idxmin(axis=0)

    print("City with lowest total precipitation:\n",lowest)
    
    avgMonthRain = data.sum(axis=0)/counts.sum(axis=0)
    print("Average precipitation in each month:")
    print(avgMonthRain)
    
    avgCityRain = data.sum(axis=1)/counts.sum(axis=1)
    print("Average precipitation in each city:",)
    print(avgCityRain)

pd_summary()
