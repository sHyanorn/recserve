# Video_Games, test read 1

- conditions: 50fa10e73f074f23876c90d8f536d131e7533971ea312e65b270eb776897b870
- B1: a8d9c526fb03
- B0: b7ff0634dd7b

## Read 1

B0's requests whose history gives no user vector: 8378

- bundle: a8d9c526fb03
- test events: 43249, one request each
- requests whose history gives no user vector: 8378

| Condition | Recall@10 | Recall@20 | NDCG@10 | Δ Recall@10 [95% CI] | Δ Recall@20 [95% CI] | Δ NDCG@10 [95% CI] |
|---|---:|---:|---:|---:|---:|---:|
| B1 reference | 0.0166 | 0.0239 | 0.0089 | -0.0031 [-0.0043, -0.0019] | -0.0034 [-0.0047, -0.0019] | -0.0005 [-0.0012, +0.0001] |
| B1 healthy | 0.0197 | 0.0272 | 0.0094 | | | |
| B1 encoder fails | 0.0188 | 0.0255 | 0.0087 | -0.0009 [-0.0016, -0.0003] | -0.0018 [-0.0025, -0.0010] | -0.0007 [-0.0009, -0.0005] |
| B1 co-occurrence fails | 0.0163 | 0.0227 | 0.0074 | -0.0034 [-0.0043, -0.0026] | -0.0046 [-0.0055, -0.0037] | -0.0020 [-0.0024, -0.0016] |
| B1 ranker fails | 0.0163 | 0.0238 | 0.0088 | -0.0034 [-0.0046, -0.0023] | -0.0034 [-0.0048, -0.0021] | -0.0006 [-0.0013, -0.0000] |
| B1 shard 1 of 2 fails | 0.0197 | 0.0274 | 0.0094 | +0.0000 [-0.0001, +0.0001] | +0.0001 [-0.0001, +0.0003] | +0.0000 [-0.0000, +0.0001] |
| B1 HNSW, ef 400 | 0.0197 | 0.0272 | 0.0094 | -0.0000 [-0.0001, +0.0000] | -0.0000 [-0.0001, +0.0000] | -0.0000 [-0.0000, +0.0000] |
| B0 reference | 0.0043 | 0.0065 | 0.0025 | -0.0154 [-0.0168, -0.0142] | -0.0208 [-0.0222, -0.0192] | -0.0068 [-0.0075, -0.0062] |
| B0 healthy | 0.0181 | 0.0246 | 0.0087 | -0.0016 [-0.0024, -0.0008] | -0.0026 [-0.0037, -0.0015] | -0.0007 [-0.0010, -0.0004] |

| Row | Recall@10 | Recall@20 | NDCG@10 | Recall@10, item with train events (17091) | Recall@10, others (26158) |
|---|---:|---:|---:|---:|---:|
| B1 reference | 0.0166 | 0.0239 | 0.0089 | 0.0421 | 0.0000 |
| B1 healthy | 0.0197 | 0.0272 | 0.0094 | 0.0499 | 0.0000 |
| B1 encoder fails | 0.0188 | 0.0255 | 0.0087 | 0.0475 | 0.0000 |
| B1 co-occurrence fails | 0.0163 | 0.0227 | 0.0074 | 0.0411 | 0.0000 |
| B1 ranker fails | 0.0163 | 0.0238 | 0.0088 | 0.0411 | 0.0000 |
| B1 shard 1 of 2 fails | 0.0197 | 0.0274 | 0.0094 | 0.0499 | 0.0000 |
| B1 HNSW, ef 400 | 0.0197 | 0.0272 | 0.0094 | 0.0498 | 0.0000 |
| B0 reference | 0.0043 | 0.0065 | 0.0025 | 0.0108 | 0.0000 |
| B0 healthy | 0.0181 | 0.0246 | 0.0087 | 0.0458 | 0.0000 |

## Per-event ranks, not committed

| Row | SHA-256 of its file |
|---|---|
| B1 reference | e9e38e562f64e999971ff5376ad466ec59e225da243623866c38d706a10ac858 |
| B1 healthy | 00c9c939fe20728261cbab48e9844f5ce66d6ba13cdffc74f10a15a33b72a4aa |
| B1 encoder fails | 2cb8c08d39115d7d7100884a802511bc769b22c3c313876f2ba4b309711f05df |
| B1 co-occurrence fails | d7838b38c81c69d3767a29013ca19efb32108baf68d2b00351d54d14d09822df |
| B1 ranker fails | db41d153af6b7a3ce8624d635c8b2870fe00fd6625a4bf87830dc8138abb0623 |
| B1 shard 1 of 2 fails | 30b566a21cf86cd0b689f0391af4bb400a24749e636ab04ca5216e259de318be |
| B1 HNSW, ef 400 | cfd0d970785b7241fa3c77d144815bc14b23c93f0d67fd92959f0e7947d84765 |
| B0 reference | e4382c6fde6846f9e5f233c67f6c573853e809d314979898eb05513be5f9c847 |
| B0 healthy | 0abfb2f4d3672802e722f8709b03e0464af9c5855a41fbcff90b1251ec7d0807 |
