AmazonReviews
Analyzing amazon review dataset using Apache Hadoop, Pig, HBase 

TopReviwers consists of two MR jobs. 

Job 1 - TopReviewersAnalyzeMR

This job calculates the reviewerAvgRating, reviewerNumberOfReviews, reviewerScore for a reviewer in each category.

Job 2 - TopFive

This job groups the reviewers by category and sorts them descendingly based on the reviewerScore.
# AmazonReviews
