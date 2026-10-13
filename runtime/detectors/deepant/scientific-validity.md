# DeepAnT Scientific Validity and Limits

## Algorithm reference

DeepAnT is a published method: Munir et al., *IEEE Access*, 2019,
[DOI: 10.1109/ACCESS.2018.2886457](https://doi.org/10.1109/ACCESS.2018.2886457).
It uses a convolutional neural network to predict the next measurement and
assigns an anomaly score from the prediction error. The published evaluation
covers 433 time series from 10 benchmarks.

## Limits of this integration

The published algorithm does not by itself validate results from the local
`deepant-analysis` service.

- The integration adapts the model to streaming data with periodic refits. The
  paper uses a training/testing protocol and thresholds tuned through validation.
- Unlabeled CSV files do not provide anomaly ground truth. Columns such as
  `Label`, `State` or `HL_Activity` must be excluded when they represent labels
  or reference annotations.
- Detection quality requires evaluation of precision, recall, F1, false
  positives, detection delay and stability by group. Comparisons with other
  detectors must use a common evaluation protocol.
- Technical tests establish implementation behavior; they do not establish
  scientific detection quality for a new dataset.

## Interpretation

Treat the output as a technically implemented anomaly score. Claims of validated
detection quality require an evaluation campaign with suitable labeled data or
controlled injected anomalies.
