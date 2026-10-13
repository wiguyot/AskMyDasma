# anomaly-light image

Runtime image for Python anomaly services that share the PySAD-based dependency
set and do not require SHAP or PyTorch.

Current consumers:

- `hstree-analysis`
- `kitnet-analysis`

The application code is still mounted from the repository by Docker Compose.
This image only moves the pinned Python dependency installation from container
startup to Docker build time.

`pysad` currently pulls several scientific dependencies transitively, including
`pyod`, `scikit-learn`, `scipy`, `numba`, and `matplotlib`. Those packages are
therefore present in the built image even though this profile does not add SHAP
or PyTorch.
