# Summary of the SHAP Paper — Focus on the Number of Samples for Explainability

## Paper Reference

**Title:** *A Unified Approach to Interpreting Model Predictions*
**Authors:** Scott M. Lundberg, Su-In Lee
**Conference:** NIPS 2017
**Topic:** SHAP — *SHapley Additive exPlanations*

[Original publication](https://papers.neurips.cc/paper/7062-a-unified-approach-to-interpreting-model-predictions).

---

## 1. General Idea of the Paper

The paper proposes a unified framework for several machine learning model explanation methods: **SHAP**.

The central idea is that many local explanation methods can be understood as **additive feature attribution methods**.

A local explanation aims to explain a given prediction `f(x)` by assigning a contribution to each feature:

```text
f(x) ≈ φ0 + φ1 + φ2 + ... + φM
```

where:

* `φ0` represents the model’s base value;
* `φi` represents the contribution of feature `i`;
* `M` is the number of features used in the explanation.

The paper shows that Shapley values are the unique solution satisfying three desirable properties:

1. **Local accuracy**: the sum of the contributions must reconstruct the local prediction.
2. **Missingness**: a missing feature must receive no contribution.
3. **Consistency**: if a feature contributes more in a model, its attribution should not decrease.

---

## 2. Why the Number of Samples Matters

The exact computation of Shapley values is expensive.

For `M` features, one would theoretically need to consider all possible coalitions of features, up to:

```text
2^M
```

coalitions.

This quickly becomes infeasible as the number of features increases.

Examples:

| Number of features `M` |       Number of coalitions `2^M` |
| ---------------------: | -------------------------------: |
|                     10 |                            1,024 |
|                     20 |                        1,048,576 |
|                     30 |                    1,073,741,824 |
|                    100 | infeasible for exact computation |

Samples are therefore used to approximate Shapley values without exhaustively exploring all possible combinations.

In KernelSHAP, `nsamples` budgets coalition masks: each mask selects which
features are kept from the explained observation. Applying one mask to a
background of N rows can require N model-row evaluations, which may be batched.
Therefore, `nsamples` is not the total number of model calls or evaluated rows.

---

## 3. What the Paper Actually Says About the Number of Samples

The paper does not provide a universal rule such as:

```text
You must always use exactly X samples.
```

Instead, it shows that the required number of samples strongly depends on several factors:

* the number of simplified features `M`;
* the type of model;
* whether the model is dense or sparse;
* the expected level of precision;
* the method used: Shapley sampling, Kernel SHAP, LIME, etc.

The paper mainly compares the relative efficiency of different methods.

---

## 4. Main Result: Kernel SHAP Is More Sample-Efficient

The paper compares, in particular:

* **Kernel SHAP**;
* **Shapley sampling values**;
* **LIME**.

The conclusion is that **Kernel SHAP obtains better estimates with fewer model evaluations** than classical Shapley sampling.

The main reason is methodological:

* Shapley sampling estimates each feature contribution separately;
* Kernel SHAP estimates all contributions jointly using weighted linear regression.

This regression uses a specific kernel, called the **Shapley kernel**, designed to recover Shapley values in the additive explanation framework.

---

## 5. Figure 3 Experiment: 200 to 1000 Evaluations

In Figure 3 of the paper, the authors compare the stability of the estimates as the number of model evaluations increases.

They use budgets approximately between:

```text
200 and 1000 model evaluations
```

Two cases are studied.

### Case A — Dense Model

A decision tree uses all 10 input features.

Observed result:

* Kernel SHAP converges better toward the true Shapley value;
* Shapley sampling is more variable;
* LIME can remain significantly different from the true value.

### Case B — Sparse Model

A decision tree uses only 3 features out of 100.

Observed result:

* Kernel SHAP remains more efficient;
* regularization becomes important;
* LIME can produce values far from the Shapley values.

Important point:
the 200 to 1000 range is not a general recommendation, but an experimental protocol used to compare methods.

---

## 6. The MNIST Case: 50,000 Samples

In the MNIST experiment, the authors state that SHAP and LIME were run with:

```text
50,000 samples
```

However, this number must be interpreted carefully.

This is an image-based case, with a much larger input space than a simple tabular problem. Even when pixels are segmented or simplified, explaining images remains computationally expensive.

This number should therefore not be generalized to all use cases.

In particular:

* 50,000 samples may be relevant for an experimental comparison on images;
* it is not a reasonable default value for standard tabular data;
* it is not a universal recommendation from the paper.

---

## 7. Practical Interpretation for Choosing `nsamples`

In practice, the number of samples should be chosen as a trade-off between:

* computational cost;
* stability of the explanations;
* expected precision;
* number of features;
* number of predictions to explain.

For Kernel SHAP, a commonly used operational rule is to start with:

```text
nsamples = 2 * M + 2048
```

where `M` is the number of varying feature groups for the explained observation.
The implementation caps the budget at the number of available nontrivial
coalitions when that number is small.

Examples:

| Number of varying groups `M` | Uncapped budget `2 * M + 2048` |
| ---------------------: | ---------------------: |
|                     10 |                  2,068 |
|                     50 |                  2,148 |
|                    100 |                  2,248 |
|                    500 |                  3,048 |
|                   1000 |                  4,048 |

For example, with 10 varying groups the actual budget is capped at
`2^10 - 2 = 1022` nontrivial coalitions, rather than 2068. The values in the
table show the formula before that cap and other special-case shortcuts.

This rule does not come directly from the NIPS 2017 paper as a universal theoretical recommendation, but it is consistent with the idea that Kernel SHAP can provide a useful approximation without exploring all coalitions.

---

## 8. Pay Attention to the Real Cost: Samples × Background

The actual cost of Kernel SHAP does not depend only on `nsamples`.

It also depends on the number of reference samples used as the **background dataset**.

The approximate cost is closer to:

```text
number of predictions explained
× nsamples
× background size
```

Thus, even a moderate `nsamples` value can become very expensive if the background dataset is too large.

Example:

```text
100 predictions explained
× 2,248 samples
× 1,000 background rows
= 224,800,000 potential elementary evaluations
```

It is therefore often more efficient to reduce the background dataset intelligently than to massively increase `nsamples`.

---

## 9. Practical Recommendation

For serious use of SHAP, it is better to reason in terms of stability rather than using a fixed arbitrary value.

A reasonable approach is the following.

### Step 1 — Reduce the Number of Features When Possible

Before explaining, avoid using an unnecessarily large feature space.

Examples:

* remove constant features;
* remove almost-null features;
* group some variables;
* use meaningful domain features;
* avoid directly explaining thousands of variables without reduction.

### Step 2 — Choose a Representative but Compact Background

Avoid using the entire training dataset if it is large.

For example, use:

* a stratified random sample;
* a clustering-based summary;
* a few dozen or a few hundred representative examples.

### Step 3 — Start with a Reasonable Value

For Kernel SHAP:

```text
nsamples = auto
```

or explicitly:

```text
nsamples = 2 * M + 2048
```

### Step 4 — Test Stability

Compare explanations using several budgets:

```text
nsamples = auto
nsamples = 2 × auto
nsamples = 4 × auto
```

Then check whether:

* the important features remain the same;
* their ranking remains stable;
* the signs of the contributions remain stable;
* the magnitudes change significantly or not.

### Step 5 — Stop When the Explanations Are Stable

There is no need to increase `nsamples` if the conclusions no longer change.

The goal is not to obtain a numerically perfect value, but a reliable and stable explanation.

---

## 10. Recommendations by Use Case

| Use case                                            | Recommendation                                                |
| --------------------------------------------------- | ------------------------------------------------------------- |
| Small number of features, simple model              | Exact computation or low `nsamples` may be possible           |
| Standard tabular data, tens to hundreds of features | Start with `nsamples=auto`                                    |
| Model that is expensive to evaluate                 | Strongly reduce the background dataset                        |
| Very large number of features                       | Reduce or group features before using SHAP                    |
| Images or text                                      | Use simplified representations: superpixels, tokens, segments |
| Deep learning                                       | Prefer specialized explainers when available                  |
| Need for strong robustness                          | Run a stability study with several `nsamples` values          |

---

## 11. What Should Not Be Concluded from the Paper

The paper does not justify saying:

```text
You should always use 1000 samples.
```

nor:

```text
You should always use 50,000 samples.
```

nor:

```text
The recommended number of samples is independent of the model.
```

These interpretations would be misleading.

The paper shows instead that:

* exact computation is exponential;
* approximation becomes necessary as `M` increases;
* Kernel SHAP is more efficient than classical Shapley sampling;
* the right number of samples depends on the problem;
* empirical stability should guide the final choice.

---

## 12. Conclusion

The key point of the paper is that **Kernel SHAP improves sampling efficiency** when approximating Shapley values.

In the paper’s tabular experiments, the authors illustrate this efficiency using budgets around **200 to 1000 model evaluations**.

In the MNIST experiment, they use **50,000 samples**, but this number corresponds to a specific image-based experimental context.

In practice, one should therefore avoid looking for a magic number of samples. A robust strategy is to:

```text
1. reduce the background dataset;
2. start with nsamples=auto;
3. increase progressively;
4. measure attribution stability;
5. keep the smallest budget that produces stable explanations.
```

The right question is not:

```text
How many samples should I use?
```

but rather:

```text
From how many samples onward do my explanations become stable?
```
