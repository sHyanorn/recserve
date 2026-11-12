# Baseline bundle

Builds the baseline bundle of docs/SPEC.md §8 from the files of `make data`: item vectors from implicit-feedback ALS (64 factors) on the train split, a co-occurrence table and a popularity list. Dependencies are locked in `uv.lock`; the seed and all parameters are constants at the top of `build.py`. The same inputs, Python version and CPU type give the same version; the version is a hash of every file in the bundle.

```bash
uv run python build.py DATA OUT        # prints the version; writes OUT/<version>/
uv run python build.py DATA OUT --encoder onnx  # the same, with user_encoder.onnx (D026)
uv run python build.py DATA OUT --encoder onnx --ranker  # also a logistic-regression ranker.onnx trained on the valid split (D027)
uv run python verify.py OUT/<version>  # checksums, shapes, the ranker check, golden cases
uv run pytest                          # verify.py on hand-written bundles: checksum, train_cutoff_ts, history_raw, top 10, unlisted file
uv run python histories.py DATA ZERO_CORE  # test events' histories in recserve's data and in seqrec-lab's; ZERO_CORE is the 0-core Video_Games.csv.gz
uv run python testdata.py ../../internal/onnx/onnxtest/testdata  # the Go tests' ONNX models and SHA256SUMS
```
