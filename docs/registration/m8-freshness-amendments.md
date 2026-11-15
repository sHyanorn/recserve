# Freshness: amendments to the registration

Amendments to docs/registration/m8-freshness.md, in date order. Each is committed before the run it governs.

## 2026-11-15: Video_Games read 2 stopped before any metric

Read 2 of Video_Games logged this line in bench-results/m8/test-reads.log:

    2026-11-14T22:51:31Z 654f0f812c31828f6831f542bffdd0729ea6f4f1 Video_Games 04b5dcba8e82381ba1648be798869c22b11caa916b49a138db994a5475890530

The kernel then stopped the test process for lack of memory, with 13.3 GiB of anonymous memory resident, after 50.8 s of go test: TestRealDataFreshness held the read's 90 bundles at once and built a pipeline over each before the first request. The process wrote and printed no metric and no per-event rank. Under section 9 the run does not count, and read 2 may run once more.

Two things in the code change before that run. First, when TestRealDataFreshness loads rebuilt bundles (3552ec6), one at a time: before the read is logged, it checks each and closes it; after, it checks the golden stream inputs of each bundle rebuilt with B1s8's ranker and closes it, and loads a rebuilt bundle for the requests of its period and closes it after them. Each condition builds its pipelines when it runs. The conditions, the bundles, the checks and the tables stay as they are, with one check added: a rebuilt bundle opened for a period must have the rebuild time it had when it was checked. The rerun's conditions hash is the one in the line above. Second, openTestRead (3552ec6 and the commit of this amendment): it does not count the line above as a read, it checks this file's SHA-256 as it checks the registration's, and it refuses a later Video_Games read whose conditions hash differs from that line's.

So read 2 is allowed once more, and only on a commit that holds this amendment. If that run also stops after it logs its line and before any metric, another amendment is needed.

Between read 1 and the stopped read 2, besides the bundle builds that sections 2 and 4 describe, the runs below read the Video_Games test file, and none of them computed a metric. TestRealDataStreamGolden ran once on B1s6 and once on B1s8. Through loadRealData it read the test file only for its earliest event time. It built the stream state from the train and valid splits. A script read the test file's event times to find its latest event, at 1693678468630, which sets the number of rebuild times. A temporary test, not in the repository, checked the golden stream inputs of B1s6, B1s8 and the 14 bundles rebuilt every 30 days with B1s8's ranker, as read 2 checks them. It built the stream state from every event of the three splits, the test split's 43,249 included. It printed only, for each bundle, its version, its rebuild time where it has one, the number of test events before that time, and whether the check passed. Each of its checks depends only on the events before the bundle's golden cases' times. For B1s6 and B1s8, those are train and valid events. Of the 14 bundles, the 13 rebuilt after the test split's start also depend on test events, at most 43,197; the one rebuilt at its start depends on none. From the stopped read to this amendment, no run read the test file.
