# KitNET in 3 simple ideas

1. **Learning to “reconstruct” normal behavior (autoencoder)**

An autoencoder is a small neural network that learns to reproduce its input: you give it a vector of numbers, and it tries to output the same thing.

- If the input looks like what it has already seen, which is normal, it reconstructs it well -> small error.
- If the input is strange, which is abnormal, it reconstructs it poorly -> large error.

In KitNET, the final score is based on this reconstruction error, measured as a distance. The larger the score, the more suspicious the point is.

KitNET learns online during its two training grace periods. Afterward, this
implementation scores with the trained weights and does not continually retrain
the autoencoders.

2. **Do not build “one huge brain”, but several small ones (ensemble)**

KitNET splits the vector into small groups of features, trains **one small autoencoder per group**, then uses **a final autoencoder that combines the errors**. This is lighter and faster.

3. **Learning the groups automatically (Feature Mapper)**

KitNET starts with a learning phase where it observes the first data points and estimates which variables or features “move together”, in other words their correlations.

It then performs grouping, through hierarchical clustering, to decide which small autoencoders it will create. That is the **Feature Mapper** phase.

## Two phases before normal KitNET operation

FM: Feature Mapper  
AD: Anomaly Detector

**FM grace period**: it observes the data in order to learn the feature grouping, the Feature Mapper phase.

**AD grace period**: it trains its autoencoders on normal behavior.
