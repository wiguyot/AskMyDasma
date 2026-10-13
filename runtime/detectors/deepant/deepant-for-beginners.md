# DeepAnT for Beginners

## The Idea in One Sentence

DeepAnT learns to **predict the next value** in a curve, or time series. If the
real value is **very different** from the prediction, the point is suspicious.

## What Is a Time Series?

A time series is a sequence of measurements over time: temperature every minute,
network traffic every second, and so on.

## How DeepAnT Works

1. It looks at a **small window** of the past, for example the last 30 measurements.
2. A neural network learns to predict the next value.
3. The prediction is compared with the real value.
4. The larger the gap, the larger the **anomaly score**.

## Why This Detects Anomalies

- If everything is normal, the curve follows its usual pattern and the prediction is usually close.
- If something changes, such as a spike, a break, or unusual behavior, the prediction misses and the gap grows.

## Simple Example

You measure the power consumption of a machine.

- Usually, it stays around 100.
- One day, it jumps to 250.

The model did not expect this jump, so the gap produces a high anomaly score.
The service reports an anomaly if that score exceeds its active threshold.

## Important Points

- DeepAnT is a **deep learning** method, so it is often heavier to train than iForest or HSTree.
- It works better when there is enough normal data.
- On CPU, it can be slow when the data rate is high.

**Keywords:** time series, window, prediction, gap, anomaly.
