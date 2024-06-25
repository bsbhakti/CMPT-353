import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.nonparametric.smoothers_lowess import lowess
from pykalman import KalmanFilter

filename = sys.argv[1]


cpu_data = pd.read_csv(f"./{filename}",sep=",")
cpu_data['timestamp'] = pd.to_datetime(cpu_data['timestamp'])

# plt.figure(figsize=(12, 4))
# plt.plot(cpu_data['timestamp'], cpu_data['temperature'], 'b.', alpha=0.5)
# plt.show() # maybe easier for testing
# plt.savefig('1.svg') # for final submission

xval = pd.Series(range(1,2161))
loess_smoothed = lowess(cpu_data['temperature'], xval,frac=0.05)
print(loess_smoothed)
plt.plot(cpu_data['timestamp'], loess_smoothed[:, 1], 'r-')
# plt.show()

kalman_data = cpu_data[['temperature', 'cpu_percent', 'sys_load_1', 'fan_rpm']]

initial_state = kalman_data.iloc[0]

observation_covariance = np.diag([5,1, 0.6, 15]) ** 2 # TODO: shouldn't be zero
transition_covariance = np.diag([3,2, 2, 25]) ** 2  # TODO: shouldn't be zero
transition = [[0.94,0.5,0.2,-0.001], [0.1,0.4,2.1,0], [0,0,0.94,0], [0,0,0,1]] # TODO: shouldn't (all) be zero


kf = KalmanFilter(   initial_state_mean=initial_state,
                    initial_state_covariance=observation_covariance,
                    observation_covariance=observation_covariance,
                    transition_covariance=transition_covariance,
                        transition_matrices=transition )

kalman_smoothed, _ = kf.smooth(kalman_data)
kalman_data['timestamp'] = cpu_data['timestamp']
del cpu_data
plt.plot(kalman_data['timestamp'], kalman_data['temperature'], 'b.')
plt.plot(kalman_data['timestamp'], kalman_smoothed[:, 0], 'g-')
plt.xlabel('Timestamp')
plt.ylabel('Temperature')
plt.legend(["Observations","Lowess smoothened","Kalman filtered"])
plt.show()
plt.savefig('cpu.svg') # for final submission

