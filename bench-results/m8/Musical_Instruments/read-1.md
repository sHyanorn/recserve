# Musical_Instruments, test read 1

- conditions: 5e58a46868ba08e850bf4c078bc910ca94971c682feb8658b6709cb90875c42b
- B1: 3be5f6667aaa
- B0: 2dda7923d636
- B1s6: f79fea0f6a5a
- B1s8: 244c913c1cc8
- R7: 0b55bdca69f7 0c852d6ebe6f 16c7fa8ab1c7 1b8e782ddaa1 209c007fb248 2290e3a5b967 252419b2b5cf 257eacc6501a 2b223d29fad5 2f6276993e90 313e079cb952 31e5cec9a030 33ab8210c302 3556cc78529a 3f7852fdfb2f 41e149fa7730 44ed87611f0a 461661ce69e9 47505176fbbf 490b20a930aa 4d9beb587803 4dc3f1204bf0 552221a30793 5a1284ecf209 5cbc27caf58b 631ac7711dda 75ce3a7a575d 77455473d9c5 7e0a1deaa9a8 80451cdc46a7 8ed385f9d39e 9142837f1e9d 935620edd4a8 93febb9d43af a44e5a3ab5f9 a4cb4591cb0f a79d01cacc70 a9e2c790f1e1 abaf9806a2e5 af4a59794acb afdcd2424338 b0ad143fef35 b804a54f6d9f bfd3e6540dc3 c234c563c2d1 c25feb3fae58 c56c7cd925fd cec07871438a d6f48b2916f8 db7c6f92a3a6 dd7826c8ffa0 e2d99b4c969e e4d3d4ae121c ea5dd6bedbed edb0d1d49eab f833a3ec2452 fa1415a554fe fc3f119a9e85 fc552361f24f fc65423128a5
- R30: 2290e3a5b967 27c88f07e955 3bb0701be0f1 623b1fe47412 6c72e92c3ad1 80aa175191c1 8f9cc970723d 94842ee6e307 9e1331461b18 a44e5a3ab5f9 c31dab259e2f d4b4116aba7a dfa8bd791fa9 fbee08124dff
- R30S8: 03d1384ae6bb 0447abf1a042 05009e5f3f2b 06b637d587bb 0c73e14dc9cc 1f51538ffb23 5576d8064134 6edeea1cf41d 6fb6a1457802 7a138ae049cb a2464f35ef44 a63087e673ec d0d5febf9549 fce00b3c2ca3

## Read 1

B0's requests whose history gives no user vector: 4231

- bundle: 3be5f6667aaa
- test events: 42595, one request each
- requests whose history gives no user vector: 4231

| Condition | Recall@10 | Recall@20 | NDCG@10 | Δ Recall@10 [95% CI] | Δ Recall@20 [95% CI] | Δ NDCG@10 [95% CI] |
|---|---:|---:|---:|---:|---:|---:|
| B1 reference | 0.0180 | 0.0279 | 0.0100 | -0.0020 [-0.0033, -0.0007] | -0.0014 [-0.0030, +0.0002] | -0.0005 [-0.0013, +0.0002] |
| B1 healthy | 0.0200 | 0.0293 | 0.0105 | | | |
| B1 encoder fails | 0.0187 | 0.0270 | 0.0095 | -0.0014 [-0.0022, -0.0005] | -0.0023 [-0.0032, -0.0014] | -0.0010 [-0.0014, -0.0006] |
| B1 co-occurrence fails | 0.0178 | 0.0274 | 0.0087 | -0.0022 [-0.0030, -0.0016] | -0.0018 [-0.0026, -0.0011] | -0.0017 [-0.0022, -0.0014] |
| B1 ranker fails | 0.0181 | 0.0268 | 0.0099 | -0.0019 [-0.0031, -0.0007] | -0.0025 [-0.0039, -0.0010] | -0.0006 [-0.0013, +0.0001] |
| B1 shard 1 of 2 fails | 0.0202 | 0.0294 | 0.0105 | +0.0001 [-0.0001, +0.0003] | +0.0002 [-0.0001, +0.0005] | +0.0000 [-0.0000, +0.0001] |
| B1 HNSW, ef 400 | 0.0200 | 0.0292 | 0.0105 | -0.0000 [-0.0001, +0.0000] | -0.0000 [-0.0001, +0.0000] | -0.0000 [-0.0000, +0.0000] |
| B0 reference | 0.0123 | 0.0179 | 0.0066 | -0.0078 [-0.0090, -0.0066] | -0.0114 [-0.0129, -0.0101] | -0.0039 [-0.0046, -0.0033] |
| B0 healthy | 0.0187 | 0.0258 | 0.0094 | -0.0014 [-0.0023, -0.0005] | -0.0035 [-0.0044, -0.0025] | -0.0011 [-0.0015, -0.0007] |

| Row | Recall@10 | Recall@20 | NDCG@10 | Recall@10, item with train events (28609) | Recall@10, others (13986) |
|---|---:|---:|---:|---:|---:|
| B1 reference | 0.0180 | 0.0279 | 0.0100 | 0.0268 | 0.0000 |
| B1 healthy | 0.0200 | 0.0293 | 0.0105 | 0.0299 | 0.0000 |
| B1 encoder fails | 0.0187 | 0.0270 | 0.0095 | 0.0278 | 0.0000 |
| B1 co-occurrence fails | 0.0178 | 0.0274 | 0.0087 | 0.0265 | 0.0000 |
| B1 ranker fails | 0.0181 | 0.0268 | 0.0099 | 0.0270 | 0.0000 |
| B1 shard 1 of 2 fails | 0.0202 | 0.0294 | 0.0105 | 0.0300 | 0.0000 |
| B1 HNSW, ef 400 | 0.0200 | 0.0292 | 0.0105 | 0.0298 | 0.0000 |
| B0 reference | 0.0123 | 0.0179 | 0.0066 | 0.0183 | 0.0000 |
| B0 healthy | 0.0187 | 0.0258 | 0.0094 | 0.0278 | 0.0000 |

## The conditions of section 7

- window: 30 days

| Row | Recall@10 | Recall@20 | NDCG@10 | Recall@10, item with train events (28609) | Recall@10, others (13986) |
|---|---:|---:|---:|---:|---:|
| F0 | 0.0200 | 0.0293 | 0.0105 | 0.0299 | 0.0000 |
| S1 | 0.0186 | 0.0284 | 0.0105 | 0.0278 | 0.0000 |
| S2 | 0.0197 | 0.0292 | 0.0109 | 0.0294 | 0.0000 |
| S3a | 0.0237 | 0.0336 | 0.0126 | 0.0307 | 0.0092 |
| S3 | 0.0317 | 0.0522 | 0.0159 | 0.0284 | 0.0386 |
| R7 | 0.0242 | 0.0362 | 0.0136 | 0.0317 | 0.0089 |
| R30 | 0.0231 | 0.0347 | 0.0130 | 0.0311 | 0.0066 |
| S3+R30 | 0.0327 | 0.0539 | 0.0167 | 0.0304 | 0.0374 |
| vector reference | 0.0180 | 0.0279 | 0.0100 | 0.0268 | 0.0000 |
| popularity reference | 0.0155 | 0.0302 | 0.0073 | 0.0077 | 0.0315 |

| Δ Recall@10 | Mean | 95% CI, users | 95% CI, events |
|---|---:|---:|---:|
| S3 − F0 | +0.0117 | [+0.0096, +0.0137] | [+0.0102, +0.0131] |
| S1 − F0 | -0.0014 | [-0.0020, -0.0008] | [-0.0020, -0.0008] |
| S2 − S1 | +0.0011 | [+0.0006, +0.0016] | [+0.0006, +0.0016] |
| S3a − S2 | +0.0039 | [+0.0031, +0.0048] | [+0.0031, +0.0047] |
| S3 − S3a | +0.0081 | [+0.0062, +0.0099] | [+0.0067, +0.0094] |
| S3 − R7 | +0.0075 | [+0.0056, +0.0094] | [+0.0060, +0.0091] |
| S3 − R30 | +0.0086 | [+0.0067, +0.0105] | [+0.0071, +0.0102] |
| R7 − F0 | +0.0042 | [+0.0029, +0.0054] | [+0.0030, +0.0053] |
| R30 − F0 | +0.0031 | [+0.0019, +0.0042] | [+0.0019, +0.0042] |
| S3+R30 − S3 | +0.0010 | [-0.0003, +0.0022] | [-0.0003, +0.0022] |
| S3+R30 − R30 | +0.0096 | [+0.0079, +0.0113] | [+0.0081, +0.0110] |

H1 (S3 − F0, the interval over users): confirmed

## Per-event ranks, not committed

| Row | SHA-256 of its file |
|---|---|
| B1 reference | bf858cadfb67026cb32a2c819481e1c5c3c3499c88b690001cbf5f037e1e562b |
| B1 healthy | 49588aab22c8ad3f094ae69325a8209bac3c198d6bb31034e40eb093ee72910d |
| B1 encoder fails | 92947f4599df0d29df94d7cb63358f77f518387a08e2113e45c6cfd38873ab81 |
| B1 co-occurrence fails | 477dd5bef1016bc12f81998cdf53b8971ca443096595c49c15a80acf5533d315 |
| B1 ranker fails | 9c44e7b152253cfaeaa47722732be26d6dc0e5abd8ab3665e7a5d0a838625f97 |
| B1 shard 1 of 2 fails | f7705dd36ad9b9caf15c77e2da3eadbcf1bbc280b6e601b27b7adb59b7af9c0c |
| B1 HNSW, ef 400 | ed1340c1e6f6382237b3019b1c493a9e94831625eb18a96a943388a115b0869d |
| B0 reference | f245818b791e545f89328c62042fb1098274c9e8a995ba29cea54165605215cc |
| B0 healthy | 075780e5659f4a004e35c3bce8117f367a22a28129a132312a192ec1fed75bdc |
| F0 | 49588aab22c8ad3f094ae69325a8209bac3c198d6bb31034e40eb093ee72910d |
| S1 | fc15472c1b2472e5996e65477f39408b9ab9adf6f0ee9fd07993ad7b47917f1d |
| S2 | c8c0b0cf1b03719e3b202f904ce1712af8de5c20b7a81cae0f6f1fa447323368 |
| S3a | 02b9344fad6827398ef52c86ba406196494f24775fc7a10f95ef7db6a97ef997 |
| S3 | 7cf93b94779ad5a76b93e0a389d0d2d4f6b1ca8871ad218557d6f05457d47c04 |
| R7 | 5deef87bfab9c7ec12cf6d108605b4ecd7fe9a227354e6e4561f55a1878cd9ac |
| R30 | 68e9117d203401118d516ce4874b184b5836df3a6e1086368ae8b24bff30d175 |
| S3+R30 | 69d01d49e2ba7befd019decff57af3c0e1c4150aca1ded377329471b5644af24 |
| vector reference | bf858cadfb67026cb32a2c819481e1c5c3c3499c88b690001cbf5f037e1e562b |
| popularity reference | 010ba7b8f3aba9eed73abfc13185aef8d95d573decf4e53f1b5cc207d585ffba |
