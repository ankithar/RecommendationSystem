# RecommendationSystem
Locality Sensitive Hashing(LSH) to find similar movies and Model and User based recommendation system to predict rating of movies

1. Locality sensitive hashing to find similar movies
LSH is run on ml-latest-small.zip found here- https://grouplens.org/datasets/movielens/
Movies are considered similar if their Jaccard similarity is >=0.5. The result is compared with the groud truth file, SimilarMovies.GroundTruth.05 present under Data folder. Precision and recall are calculated to assess efficiency.

2. Model and user based recommendation system
The problem statement is to predict the ratings of movies whose IDs are mentioned in testing_small.csv and testing_20m.csv in Data folder.
- Model based CF recommendation system implemeted using Spak MLib.
- User based CF recommendation system implemented using Pearson correlation.

