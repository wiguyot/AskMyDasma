# Isolation Forest for Beginners

## The idea in one sentence

A point is **suspicious** if it can be **isolated very quickly** from the others.

## A forest of random partitioning trees

iForest uses a **forest of decision trees**, but not to predict a grade or a class.
Here, the trees are mainly used to **separate** the data.

## How do we “isolate” a point?

We build many trees. Each tree is a small set of “questions”.

To isolate a point in a tree:

1. Pick a variable at random, for example temperature.
2. Pick a value at random, for example `18`.
3. Ask: “Is the value <= 18?” In scikit-learn, yes goes left and no goes right.
4. Repeat with another question until almost only that point remains.

The more questions are needed to isolate it, the more it looks like the other points.

## Why does it detect anomalies?

- A **normal** point is often in the middle of a cloud, so it takes **many splits** to isolate it.
- An **abnormal** point is often more alone or in a rare area, so it is isolated with **few splits**.

So iForest looks at the **depth**, the number of steps, needed to isolate a point:

- small depth -> **high anomaly score**
- large depth -> **low anomaly score**

## Why is it fast and practical?

- No need for examples of attacks or errors: it is **unsupervised**.
- It works well in practice with numerical data.
- It can use only a **subset** of the data to build each tree, which makes it faster.

In this project, each tree uses a fixed sample of **256 points** by default. A tree also has a height limit—about
8 splits for 256 samples—because very long branches are not needed to identify the isolated points. The path lengths
are normalized into a score between 0 and 1: a score around **0.5** is the paper's reference point, while higher
scores mean more anomalous.

The streaming window and periodic retraining used by this service are local implementation choices. See the
[implementation guide and scientific limits](../../../docs/models/iforest/paper-conformity.md) for the exact parameters and caveats.

## Choosing meaningful features

If your variables do not make sense, for example identifiers that change all the time, iForest can make mistakes.
In general, it works better with variables describing a **behavior** such as measurements, statistics, or ratios.

## Mini example

You measure students' heights in centimeters.

- Most are between `150` and `190`.
- You have one value at `240`.

That value is very easy to isolate with a few splits, so iForest considers it **suspicious**.

---

**Keywords to remember:** isolate, random split, depth, forest, anomaly.
