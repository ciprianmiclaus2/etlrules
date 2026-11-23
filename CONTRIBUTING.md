# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

## Types of Contributions

### Report Bugs

Report bugs at https://github.com/ciprianmiclaus2/etlrules/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Python version, pandas version
* Any other details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug (a test case is ideal).

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

### Implement Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

### Write Documentation

ETLrules could always use more documentation, whether as part of the
official ETLrules docs, in docstrings, or even on the web in blog posts,
articles, and such.

### Submit Feedback

The best way to send feedback is to file an issue at https://github.com/ciprianmiclaus2/etlrules/issues.

If you are proposing a feature:

* Start with the problem statement (the pain point, what problem are you trying to solve)
* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

## Get Started!

Ready to contribute? Here's how to set up `etlrules` for local development.

1. Fork the `etlrules` repo on GitHub.
2. Clone your fork locally

```
    $ git clone git@github.com:your_name_here/etlrules.git
```

3. Create a virtualenv and install the package in editable mode with all the backends and test dependencies:

```
    $ cd etlrules/
    $ python -m venv .venv
    $ source .venv/bin/activate
    $ pip install -e ".[polars,dask,test]"
```

4. Check that the tests pass before you start:

```
    $ pytest tests/
```

5. Create a branch for local development:

```
    $ git checkout -b name-of-your-bugfix-or-feature
```

   Now you can make your changes locally.

6. When you're done making changes, check that your changes pass the
   tests, including testing other Python versions, with tox:

```
    $ tox
```

7. Commit your changes and push your branch to GitHub:

```
    $ git add .
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature
```

8. Submit a pull request through the GitHub website.

## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.md.
3. The pull request should work for Python 3.11, 3.12, 3.13 and 3.14. Check
   https://github.com/ciprianmiclaus2/etlrules/actions
   and make sure that the tests pass for all supported Python versions.

## Tips

To run the tests:

```
    $ pytest tests/
```

To run a subset of the tests:

```
    $ pytest tests/rules/test_basic.py
```

## Deploying

A reminder for the maintainers on how to deploy.

1. Make sure all your changes are merged into `main`, including:
    * the new version in `etlrules/__init__.py` (`__version__`, the single source of the package version)
    * an entry for the new version in HISTORY.md
2. Check the "Python package" workflow passed for the latest commit on `main`:
   https://github.com/ciprianmiclaus2/etlrules/actions
3. On GitHub, go to Releases -> "Draft a new release", create a new tag named after the version
   (e.g. `0.3.4`) targeting `main`, add the release notes (e.g. the HISTORY.md entry)
   and click "Publish release".

Publishing the release triggers the "Upload Python Package" workflow, which builds the package
and uploads it to PyPI. Pushing a tag on its own, or saving the release as a draft, does not trigger it.
The upload workflow does not run the tests, which is why step 2 matters.

The upload workflow can also be started manually from the Actions tab ("Upload Python Package" ->
"Run workflow"). Choose the release tag in "Use workflow from" so that the right version is built.
PyPI rejects uploading a version that already exists, so bump the version before re-publishing.

The upload uses the `PYPI_API_TOKEN` repository secret. If the upload step fails with a 403,
create a new API token on PyPI and update the secret in the repository settings.
