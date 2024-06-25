import numpy as np

def np_summary():
    data = np.load('monthdata.npz')
    totals = data["totals"]
    counts = data["counts"]
    sum1 = totals.sum(axis=1)
    lowest = sum1.argmin(axis=0)
    print(totals)
    print("Row with lowest total precipitation: ",lowest)
    
    
    avgMonthRain = totals.sum(axis=0)/counts.sum(axis=0)
    print("Average precipitation in each month:")
    print(avgMonthRain)
    
    avgCityRain = totals.sum(axis=1)/counts.sum(axis=1)
    print("Average precipitation in each city:")
    print(avgCityRain)
    
    reshapedTotals = np.reshape(totals,(4 * totals.shape[0],3))
    print(reshapedTotals)
    totalRainQuarter = reshapedTotals.sum(axis=1)
    totalRainQuarter = np.reshape(totalRainQuarter,(totalRainQuarter.shape[0]//4,4))
    print("Quarterly precipitation totals:")
    print(totalRainQuarter)

np_summary()