# Contributing to AskMyDasma

Thank you for your interest in AskMyDasma. Contributions can include bug
reports, feature ideas, documentation improvements, and code changes.

## Before you start

- Check the existing documentation and issue tracker for a similar report or
  proposal.
- For a large change, open an issue first so the scope can be discussed.
- Keep changes focused. Update the documentation when behavior or setup steps
  change.
- Do not include passwords, access tokens, private data, or confidential logs
  in an issue or pull request. Use a small synthetic example when possible.

## Report a bug

Open a bug report with the issue form in the repository's **Issues** area. Add
enough detail for another person to reproduce the problem:

- the AskMyDasma version or Git commit;
- your operating system, Docker version, and Docker Compose version;
- the steps or commands that caused the problem;
- what you expected and what happened;
- relevant logs with secrets and private data removed.

If the project is hosted somewhere other than GitHub, use that host's issue
tracker. The public repository page will be the source of truth for the current
reporting location.

## Suggest a feature

Use the feature request form in the repository's **Issues** area. Describe the
problem you want to solve, who needs the feature, and what result would help.
You may also describe other approaches you considered.

## Submit a change

For local Python development, use Python 3.12 or later and create an environment:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[web]' pytest
```

Install the relevant optional extras (`iforest`, `deepant`, `mdragpoint`,
`explainability`) for work on those model components. The pinned Docker images
remain the runtime reference; local dependency resolution can differ.

1. Create a focused branch for your change.
2. Make the change and add or update relevant documentation.
3. Run the checks related to your change. For Python changes, run the test
   suite with `PYTHONPATH=src:. .venv/bin/python -m pytest -q` when the project
   environment is available.
4. Run `git diff --check` and review your changes before opening a pull request.
5. Explain the problem, the change, and the checks you ran in the pull request.

Please do not include datasets or copied third-party code in a contribution
unless their source and permission to redistribute them are clear.

## Maintainer contact

For project or research inquiries, contact William Guyot-Lénat at
[william.guyot@limos.fr](mailto:william.guyot@limos.fr).
Use GitHub Issues for bug reports and feature requests so discussions remain
accessible to contributors.
