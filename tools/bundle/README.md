# Baseline bundle

Builds the baseline bundle of docs/SPEC.md §8 from the files of `make data`: item vectors from implicit-feedback ALS on the train split, with the settings recorded for the category once chosen on its valid split (D058), and b7ff0634dd7b's for a category with none recorded or with `--baseline`; a co-occurrence table and a popularity list. Dependencies are locked in `uv.lock`; the seed and all parameters are constants at the top of `build.py`. The same inputs, Python version and CPU type give the same version; the version is a hash of every file in the bundle.

```bash
uv run python build.py DATA OUT        # prints the version; writes OUT/<version>/
uv run python build.py DATA OUT --encoder onnx  # with user_encoder.onnx, the ALS fold-in of the history (D026, D058)
uv run python build.py DATA OUT --encoder onnx --ranker  # also a logistic-regression ranker.onnx trained on the valid split (D027)
uv run python build.py DATA OUT --category Musical_Instruments  # another category's split; --baseline for b7ff0634dd7b's ALS settings
uv run python build.py DATA OUT --encoder onnx --cutoff T --ranker-from B  # from every event before T, with the ranker of the bundle B
uv run python als_grid.py DATA ITEMS_IDS OUT  # the ALS grid on the valid split (docs/registration/m8-freshness.md, 6.1)
uv run python fresh.py DATA ITEMS_IDS OUT  # the popularity reference on the valid split for each window (6.2)
uv run python verify.py OUT/<version>  # checksums, shapes, the ranker check, golden cases
uv run pytest                          # verify.py on hand-written bundles; build.py, the grid and the stream state on small data
uv run python histories.py DATA ZERO_CORE  # test events' histories in recserve's data and in seqrec-lab's; ZERO_CORE is the 0-core Video_Games.csv.gz
uv run python testdata.py ../../internal/onnx/onnxtest/testdata  # the Go tests' ONNX models and SHA256SUMS
```
