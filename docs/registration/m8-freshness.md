# Freshness: registration of the M8 evaluation

This file is committed before any run it governs and does not change after its commit. Amendments are appended, dated, to `docs/registration/m8-freshness-amendments.md`, each committed before the run it governs. The decisions behind it are D058 to D061.

## 1. Question

A bundle is trained once and served unchanged, while the service receives every event through its event stream. How much recommendation quality does the service gain by keeping state from that stream on top of a frozen bundle: popularity over a sliding window, co-occurrence counted as events arrive, and two recency features for the ranker? How does that compare with rebuilding the bundle every 7 or 30 days? The cost of the state in serving is measured under docs/BENCHMARKS.md, M8, whose protocol is committed before those runs; it is not part of this registration.

## 2. Data

- Video_Games and Musical_Instruments from Amazon Reviews 2023, 5-core, timestamp split, downloaded from https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/benchmark/5core/timestamp/, the address `scripts/data.sh` uses, and checked against `scripts/data.sha256`. The split's cut points are 1628643414042 and 1658002729837 (ms since the Unix epoch) for both categories.
- Video_Games: train 736,827 events, the latest at 1628643144373; valid 34,510 events, from 1628644724721 to 1658002237581; test 43,249 events.
- Musical_Instruments: when this file is committed, its train and valid files have been downloaded and their SHA-256 listed in `scripts/data.sha256`, and none of its files has been opened. Its test file is downloaded after this commit, and its SHA-256 is added in a later commit, before any Musical_Instruments run.
- Terms: the source states no data license and asks that BLaIR (Hou et al., 2024, arXiv:2403.03952) be cited. The data are used for research and downloaded by script, and nothing derived from rows is committed (D011).
- Building a bundle reads the three split files: the item IDs of all three, its catalog (SPEC §8, amended 2026-11-12), and the test file's user IDs, among which `build.py` draws the golden cases (their train histories only). Building a rebuilt bundle (section 4) reads the events before its time, which are also its golden cases' histories. The real-data test on a valid split reads the test file only for its earliest event time, against which a bundle's `train_cutoff_ts` is checked; the other real-data tests send test events as requests and are not run on Musical_Instruments before its test run. None of these involves a choice. No Musical_Instruments test event is a request, or a label for a ranker or a choice, before its test run; Video_Games test events have been requests before, in M7 and in the analysis of section 11.

## 3. Hypotheses

- H1, primary, confirmatory on Musical_Instruments: recall@10 under S3, with state from the event stream for retrieval and ranking, is higher than under F0, the frozen bundle (section 7).
- H2, descriptive: the part of that difference that comes from each component (sliding popularity, streaming co-occurrence, a ranker retrained on stream features, the two recency features), and S3 against bundles rebuilt every 7 and every 30 days.
- On Video_Games every result is exploratory (section 11).

## 4. Bundles

Built by `tools/bundle/build.py` for each category.

- B1: implicit-feedback ALS on the train split only, with the settings chosen on valid (section 6.1); a user encoder `user_encoder.onnx` whose user vector is u = (YᵀY + λI)⁻¹ Σ y_j over the history's last 50 items, Y being the item vectors, y_j those of the history's items and λ the ALS regularization (D058); the co-occurrence table, the popularity list and the logistic-regression ranker over the six features of SPEC §8 (D027), the ranker trained on the bundle's own user vectors, for B1 the fold-in's.
- B0: the settings of bundle b7ff0634dd7b: 64 factors, α 1, λ 0.05, 15 iterations, the user vector the mean of the last 50 items' vectors; the same co-occurrence table and popularity list, and a ranker trained as B1's is, on B0's own vectors and user vectors. On Video_Games, B0 is b7ff0634dd7b; on Musical_Instruments, `build.py` builds it with those settings and `--encoder mean`.
- B1s6 and B1s8: B1 with its ranker retrained on the valid split on features from the stream state (section 5), as D027 point 7 trains it: a request at the first valid event of each valid user with train events, whose history is the user's events before it; candidates and features from B1's vectors and the stream state at that time; label 1 for the items of the user's valid events; users none of whose valid items are candidates left out, 20% of the rest held out; C = 1. B1s6 has the six features of SPEC §8; B1s8 those six followed by `window_count` and `item_age` (D060).
- Rebuilt bundles R_P,k for P = 7 and 30 days: at T_k = 1658002729837 + k × P days, for every k ≥ 0 with T_k not after the category's latest test event, `build.py` with B1's settings on every event of the three split files before T_k, carrying B1's ranker unchanged. For S3+R30, bundles rebuilt the same way carry B1s8's ranker. A rebuilt bundle records T_k in its manifest. From k = 1 on, its events reach into the test period, so rebuilt bundles are loaded without the check against the first test event (SPEC §8); in its place, every request checks that the bundle answering it has T_k not after ts and only events before ts.

## 5. Stream state

The events of a run are those of the splits it loads: train and valid in a run on valid, all three in a run on test. For a request at time ts, every quantity below uses only events before ts; events at one time are ordered by event ID (`history.EventID`) (D059).

- Window count c_i: the number of item i's events in [ts − w, ts), w chosen on valid (section 6.2).
- Popular list: the 100 items with the highest c_i above 0, ties to the lower internal ID.
- Co-occurrence: an event of user u on item i at time t pairs i with the item of each of u's at most 5 latest events before t, except those on i, counted in both items' rows at time t; events at the same time are not paired. Item i's row at ts holds the 50 items with the most pairs with i before ts, ties to the lower internal ID.
- `window_count`: ln(1 + c_i). `item_age`: ln(1 + d_i), d_i the days from item i's first event to ts; 0 if it has none before ts.

The popular list and the rows take the places of `popular.npy` and `cooc_ids.npy`: the sources' scores and the six features of SPEC §8 are computed from them as from those files.

## 6. Choices made on valid

Made per category, on its valid split only, and committed before that category's first test read.

6.1. ALS for B1: factors {64, 128, 256} × α {1, 10, 40, 100} × λ {0.01, 0.1, 1}, 15 iterations, seed 1, on the train split. Each setting is scored by its vector reference on valid: one request per valid event; the history the user's train and valid events before it, its last 50 items; exact dot product over all items, the history's items left out, ties to the lower internal ID; recall@10. The highest wins. Among settings equal to four decimals: the higher NDCG@10, then fewer factors, then the smaller α, then the larger λ. For the chosen setting only, recall@10 on valid with the exact per-user solve of implicit ALS on the same last 50 items (A = YᵀY + λI + Σ (c_j − 1) y_j y_jᵀ, b = Σ c_j y_j, c_j being α times the item's number of events among them) is reported; it changes no choice.

6.2. Window w ∈ {1, 3, 7, 14, 30} days, by the recall@10 of the popularity reference (section 7) on valid. Among windows equal to four decimals: the higher NDCG@10, then the shorter window.

6.3. Fixed in advance, not chosen: co-occurrence as in section 5; the two recency features; the rankers' training (section 4, C = 1); both rebuild periods, each reported.

## 7. Conditions

Each condition sends one request per event of the evaluated split through the pipeline in process, as TestRealDataQuality does (docs/BENCHMARKS.md, Recommendation quality): k = 20, the user's history before ts from the loaded splits, every stage budget off, MMR included.

- F0: B1.
- S1: B1, its popularity source the popular list of section 5; co-occurrence from B1.
- S2: S1 with co-occurrence from the rows of section 5.
- S3a: B1s6 with the sources of S2.
- S3: B1s8 with the sources of S2.
- R7, R30: for a request at ts, R_P,k with the largest T_k not after ts, with its own sources.
- S3+R30: R_30,k as in R30, with B1s8's ranker and the sources of S2.
- Reference rows: B1's vector reference (TestRealDataQuality's reference); the popularity reference, every item ranked by c_i, ties to the lower internal ID, the history's items left out.

## 8. Criteria

- Metrics: recall@10, recall@20 and NDCG@10 of each request's item over the whole catalog, the history's items left out (docs/BENCHMARKS.md, Recommendation quality).
- Primary: on Musical_Instruments, the mean over its test events of S3's recall@10 minus F0's, with the 95% percentile interval of a paired bootstrap over users. The test events' users are sorted by raw ID, compared as byte strings; each of 1,000 resamples draws as many users with replacement, by a PCG seeded with 1 as `pairedBootstrap` seeds it; a resample's value is the sum of its users' differences over all their events divided by its number of events; the interval's bounds are the 25th and the 975th of the 1,000 values in ascending order (nearest rank). Resampling users leaves the stream state as it is; every event weighs the same, so a user with more test events weighs more.
- Reading of H1: confirmed if the interval's lower bound is above 0; contradicted if its upper bound is below 0; otherwise, a bound at exactly 0 included, not confirmed.
- Beside it, not used for H1: the interval of the paired bootstrap over events (M7's method: 1,000 resamples of the events by the same PCG, the same bounds).
- Descriptive, each with both intervals of its recall@10 difference: S1 − F0, S2 − S1, S3a − S2, S3 − S3a, S3 − R7, S3 − R30, R7 − F0, R30 − F0, S3+R30 − S3, S3+R30 − R30. As levels without intervals: recall@20 and NDCG@10 of every row, and every row's recall@10 on the events whose item has at least one train event and on the others. Read 1's table of TestRealDataQuality's conditions keeps its M7 form: each metric's difference from healthy, with its interval over events.
- Video_Games: the same table and intervals, all exploratory.
- The README reports the Musical_Instruments reading of H1, whichever it is.

## 9. Test reads

- Video_Games: two. Read 1: B1 under TestRealDataQuality's conditions (reference, healthy, each degradation forced, HNSW) and B0's reference and healthy rows, with the strata of section 8. Read 2: the conditions of section 7.
- Musical_Instruments: one, with read 1's rows and the conditions of section 7.
- A run that fails before it writes or prints any metric does not count; it may run once more, after an amendment that records it.
- The code that computes quality on a test split, before it opens the test file, checks that this file's SHA-256 equals a constant committed with it; appends a line (UTC time, commit, category, a hash of the conditions) to `bench-results/m8/test-reads.log` and stops if the log already holds the category's reads; and refuses an output path that exists. The log is committed with the results. Per-event ranks are not committed (D011); their SHA-256 are given with the results.

## 10. Validity

A run is valid if: no response of a condition is degraded, and in TestRealDataQuality's forced conditions every response reports its reason exactly when forced (docs/BENCHMARKS.md, Recommendation quality); every test event is at or after 1658002729837; for every request, the window count of its item equals the number of that item's events in [ts − w, ts) found by binary search in its sorted event times; and in Video_Games read 2, the per-event ranks of F0 and of the vector reference equal read 1's. Before a category's first test read, on its valid split: the popularity reference's per-event ranks equal those of `tools/bundle/fresh.py` for every window of 6.2, and B1's vector reference equals that of the script that ran 6.1.

## 11. Disclosure

- Before this registration, an exploratory analysis ran 18 configurations on the Video_Games test split, in this order: ref_mean (bundle vectors, mean of last 50); ref_foldin (bundle vectors, ALS fold-in); pop_global (train+valid counts); pop_bundle (popular.npy, top 100); pop_window_1d; pop_window_7d; pop_window_30d; itemknn_test_top100_last1; itemknn_test_top100_last3; itemknn_test_top100_last10; itemknn_test_top100_lastNone; ease_test_lambda100 (fit train+valid); als_test_f128_a40_r0.1 (fit train+valid); itemknn_last3_refit_every_7d; itemknn_last3_refit_every_30d; als_f128_a40_refit_every_30d; ease_l100_refit_every_30d; pop_window_14d. On the valid split it ran four item-kNN settings, three EASE settings, eight ALS settings and popularity windows of 1, 3, 7, 14 and 30 days. On the test split it also reran TestRealDataQuality twice, matching bench-results/m7/quality.md, the second time writing every condition's per-event ranks. From these and its configurations' own ranks it computed the recall@10 differences of 13 configurations from the healthy pipeline and 5 differences between configurations, each with a paired interval over events; figures per stratum of the test events; and the recall of the union of two rows' hits, an upper bound for combining them. Its results, test figures included, were known when this file was written, so the Video_Games results are exploratory. On valid, windows of 14 and 30 days gave equal recall@10 (0.0581), and 14 days the higher NDCG@10.
- Musical_Instruments was chosen on 2026-11-14 by a rule fixed before any of its files was read: among the Amazon Reviews 2023 categories with a 5-core timestamp split from the same source, the one closest to Video_Games by √(ln²(items / 25.6K) + ln²(ratings / 814.6K)) on the 5-core statistics at https://amazon-reviews-2023.github.io/data_processing/5core.html, whose test file is at least half as large as Video_Games' by HTTP Content-Length. That statistics page had been seen before the rule was written, at about 18:11 UTC with no exact record of the time, when no file of any candidate category had been opened. The figures below were read again from that page and from HTTP response headers, and from nothing else, on 2026-11-14 at 18:35 UTC, Software's test file size from its header at 18:52 UTC: Musical_Instruments, 57.4K users, 24.6K items, 511.8K ratings, distance 0.47; next Baby_Products 0.52; Software 0.60, whose test file, 406,243 bytes, is under half of Video_Games', so the rule excludes it; Industrial_and_Scientific 0.68. Test files: 872,810 bytes for Musical_Instruments, 887,435 for Video_Games.
