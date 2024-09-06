import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split



def get_pca(X):
    """
    Transform data to 2D points for plotting. Should return an array with shape (n, 2).
    """
    flatten_model = make_pipeline(
        MinMaxScaler(),PCA(2)
    )
    X2 = flatten_model.fit_transform(X)
    assert X2.shape == (X.shape[0], 2)
    return X2


def get_clusters(X):
    """
    Find clusters of the weather data.
    """
    model = make_pipeline(
        KMeans(n_clusters=9)
    )
    model.fit(X)
    # print(model.predict(X))
    return model.predict(X)


def main():
    data = pd.read_csv(sys.argv[1])
    y, X = data["city"], data.drop(columns="city")
    X, X_valid, y, y_valid = train_test_split(
    X, y, test_size=0.25, random_state=42)

    X2 = get_pca(X)
    clusters = get_clusters(X)
    plt.figure(figsize=(10, 6))
    plt.scatter(X2[:, 0], X2[:, 1], c=clusters, cmap='Set1', edgecolor='k', s=30)
    plt.savefig('clusters.png')

    df = pd.DataFrame({
        'cluster': clusters,
        'city': y,
    })
    counts = pd.crosstab(df['city'], df['cluster'])
    print(counts)


if __name__ == '__main__':
    main()
