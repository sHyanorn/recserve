# Baseline bundle

Builds the baseline bundle of docs/SPEC.md §8 from the files of `make data`: item vectors from implicit-feedback ALS (64 factors) on the train split, a co-occurrence table and a popularity list. Dependencies are locked in `uv.lock`; the seed and all parameters are constants at the top of `build.py`. The same inputs, Python version and CPU type give the same version; the version is a hash of every file in the bundle.

```bash
uv run python build.py DATA OUT        # prints the version; writes OUT/<version>/
uv run python verify.py OUT/<version>  # checksums, shapes, golden cases
```
