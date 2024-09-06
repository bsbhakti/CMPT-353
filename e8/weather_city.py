from operator import index
from sklearn.svm import SVC
import numpy as np
import pandas as pd
import sys
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler



labelled_data = pd.read_csv(sys.argv[1])
y, X = labelled_data[["city"]], labelled_data.drop(columns="city")
X_train, X_valid, y_train, y_valid = train_test_split(
    X, y, test_size=0.25, random_state=42)

unlabelled_data = pd.read_csv(sys.argv[2])
X_test =  unlabelled_data.drop(columns="city")

scaler = StandardScaler(copy=False)
scaler.fit(X_train)
X_train = pd.DataFrame(scaler.transform(X_train),columns=X_train.columns)
scaler.fit(X_test)
X_test = pd.DataFrame(scaler.transform(X_test),columns=X_test.columns)
scaler.fit(X_valid)
X_valid = pd.DataFrame(scaler.transform(X_valid),columns=X_valid.columns)

# scaler.transform(X_test)
# scaler.fit_transform(X_valid)

model = SVC(kernel='linear', C=0.4)
model.fit(X_train, y_train)
pred = model.predict(X_test)
pd.Series(pred).to_csv(sys.argv[3],index=False, header=False)
print(model.score(X_valid, y_valid))
# y_valid = y_valid.to_numpy().flatten()
# df = pd.DataFrame({'truth': y_valid, 'prediction': model.predict(X_valid)})
# print(df[df['truth'] != df['prediction']])