import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
df = pd.read_csv('aggregated.csv')

# Select the features for clustering
features = df[['time_<lambda>','platform_first', 'country_code_first', 'game_server_nunique', 'fps_mean', 'fps_<lambda_0>', 'session_id_nunique', 'social_sum', 'movement_sum']]
features = pd.get_dummies(features, columns=['platform_first', 'country_code_first'], drop_first=True)
#print(features.isnull().any())
# Standardize the features (important for K-Means)
scaler = StandardScaler()
scaled_features = scaler.fit_transform(features)


explained_variances = []

# Define a range of cluster numbers to test
cluster_range = range(2, 8)  # You can adjust the range as needed

# Calculate explained variance for each cluster number
for n_clusters in cluster_range:
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    kmeans.fit(scaled_features)
    explained_variances.append(kmeans.inertia_)

# Plot the explained variances
plt.figure(figsize=(8, 5))
plt.plot(cluster_range, explained_variances, marker='o', linestyle='-')
plt.title('Elbow Method for Optimal Cluster Number')
plt.xlabel('Number of Clusters')
plt.ylabel('Explained Variance')
plt.xticks(cluster_range)
plt.grid(True)
plt.show()


silhouette_scores = []

# Calculate Silhouette Score for each cluster number
for n_clusters in cluster_range:
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    cluster_labels = kmeans.fit_predict(scaled_features)
    silhouette_scores.append(silhouette_score(scaled_features, cluster_labels))

# Plot the Silhouette Scores
plt.figure(figsize=(8, 5))
plt.plot(cluster_range, silhouette_scores, marker='o', linestyle='-')
plt.title('Silhouette Score for Optimal Cluster Number')
plt.xlabel('Number of Clusters')
plt.ylabel('Silhouette Score')
plt.xticks(cluster_range)
plt.grid(True)
plt.show()

# Perform K-Means clustering
kmeans = KMeans(n_clusters=4, random_state=42)  # You can choose the number of clusters
df['cluster'] = kmeans.fit_predict(scaled_features)
cluster_centers = scaler.inverse_transform(kmeans.cluster_centers_)  # Inverse transform if you scaled your features
cluster_centers_df = pd.DataFrame(cluster_centers, columns=features.columns)
print(cluster_centers_df)
sns.countplot(data=df, x='platform_first', hue='cluster')
plt.show()
# Principal Component Analysis (PCA) for dimensionality reduction (for visualization)
pca = PCA(n_components=2)
principal_components = pca.fit_transform(scaled_features)
df['PCA1'] = principal_components[:, 0]
df['PCA2'] = principal_components[:, 1]

# Plot the clusters using PCA for visualization
plt.figure(figsize=(10, 6))
sns.scatterplot(data=df, x='PCA1', y='PCA2', hue='cluster', palette='viridis', s=100)
plt.title('K-Means Clustering Visualization')
plt.xlabel('Principal Component 1')
plt.ylabel('Principal Component 2')
plt.legend(title='Cluster')
plt.show()
# Create subplots for social interactions
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Bar plot for social interactions by cluster
sns.barplot(data=df, x='cluster', y='social_sum', ax=axes[0])
axes[0].set_title('Social Interactions by Cluster')
axes[0].set_xlabel('Cluster')
axes[0].set_ylabel('Social Interaction Sum')

# Countplot for social interactions by cluster
sns.countplot(data=df, x='social_sum', hue='cluster', ax=axes[1])
axes[1].set_title('Count of Social Interactions by Cluster')
axes[1].set_xlabel('Social Interaction Sum')
axes[1].set_ylabel('Count')

plt.tight_layout()
plt.show()
# Create subplots for movement

# Create subplots for the remaining features
fig, axes = plt.subplots(3, 3, figsize=(18, 12))

# Features to visualize
remaining_features = ['time_<lambda>', 'game_server_nunique', 'fps_mean', 'fps_<lambda_0>', 'session_id_nunique']

for i, feature in enumerate(remaining_features):
    row, col = divmod(i, 3)
    
    # Bar plot for the feature by cluster
    sns.barplot(data=df, x='cluster', y=feature, ax=axes[row, col])
    axes[row, col].set_title(f'{feature} by Cluster')
    axes[row, col].set_xlabel('Cluster')
    axes[row, col].set_ylabel(feature)

plt.tight_layout()
plt.show()

# Create subplots for social interactions and movement

fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Box plot for social interactions by cluster
sns.boxplot(data=df, x='cluster', y='social_sum', ax=axes[0])
axes[0].set_title('Social Interactions by Cluster')
axes[0].set_xlabel('Cluster')
axes[0].set_ylabel('Social Interaction Sum')

# Box plot for movement by cluster
sns.boxplot(data=df, x='cluster', y='movement_sum', ax=axes[1])
axes[1].set_title('Movement by Cluster')
axes[1].set_xlabel('Cluster')
axes[1].set_ylabel('Movement Sum')

plt.tight_layout()
plt.show()
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
# Box plot for movement by cluster
sns.boxplot(data=df, x='cluster', y='movement_sum', ax=axes[0])
axes[0].set_title('Movement by Cluster')
axes[0].set_xlabel('Cluster')
axes[0].set_ylabel('Movement Sum')

# Violin plot for movement by cluster
sns.violinplot(data=df, x='cluster', y='movement_sum', ax=axes[1])
axes[1].set_title('Distribution of Movement by Cluster')
axes[1].set_xlabel('Cluster')
axes[1].set_ylabel('Movement Sum')

plt.tight_layout()
plt.show()
