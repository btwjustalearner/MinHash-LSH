# MinHash-LSH

In this task, I implement the Min-Hash and Locality Sensitive Hashing algorithms with Jaccard similarity to find similar business pairs in the train_review.json file. We focus on the 0 or 1 ratings rather than the actual ratings/stars in the reviews. Specifically, if a user has rated a business, the user’s contribution in the characteristic matrix is 1. If the user hasn’t rated the business, the contribution is 0. Table 1 shows an example. Your task is to identify business pairs whose Jaccard similarity is >= 0.05.

You can define any collection of hash functions that you think would result in a consistent permutation of the row entries of the characteristic matrix. Some potential hash functions are: 

𝑓(𝑥)=(𝑎𝑥+𝑏) % 𝑚 𝑓(𝑥)=((𝑎𝑥+𝑏) % 𝑝) % 𝑚

where 𝑝 is any prime number; 𝑚 is the number of bins. You can define any combination for the parameters (𝑎, 𝑏, 𝑝, or 𝑚) in your implementation.

After you have defined all the hash functions, you will build the signature matrix using Min-Hash. Then you will divide the matrix into 𝒃 bands with 𝒓 rows each, where 𝒃×𝒓=𝒏 (𝒏 is the number of hash functions). You need to set 𝒃 and 𝒓 properly to balance the number of candidates and the computational cost. Two businesses become a candidate pair if their signatures are identical in at least one band.

Lastly, you need to verify the candidate pairs using their original Jaccard similarity. Table 1 shows an example of calculating the Jaccard similarity between two businesses. Your final outputs will be the business pairs whose Jaccard similarity is >= 0.05.
