"""fresh.py's state on hand-written events: event IDs as Go computes them, the window's bounds, the pairing rule
and ties."""

import numpy as np

from fresh import State, event_id, popularity_rank, select_window

IX = {f"i{k}": k for k in range(1, 5)}


def test_event_id_is_go_s():
    # history.EventID in Go for the same fields.
    assert event_id("AGKH", "B081TJ8YS3", 1588615855070) == 8778309058214435204
    assert event_id("u", "i", -3) == 8666530849759146750


def test_window_bounds():
    s = State([("u", "i1", 89), ("u", "i1", 90), ("v", "i1", 99), ("w", "i1", 100), ("w", "i2", 100)], IX)
    assert s.count(1, 90, 100) == 2
    assert s.popular(100, 10) == [1]
    assert s.popular(101, 10) == [1, 2]
    assert s.first(2, 100) is None and s.first(1, 100) == 89


def test_pairs_with_the_latest_events_before_and_not_at_the_time():
    # u's events at 1 to 7 on i1, x (unknown), i2, i3, i4, i1, i2; and one on i3 at 7, the same time as i2.
    rows = [("u", r, t) for t, r in enumerate(["i1", "x", "i2", "i3", "i4", "i1", "i2"], start=1)] + [("u", "i3", 7)]
    s = State(rows, IX)
    # i2 at 7 pairs with the 5 latest before 7, x among them, so not with i1 at 1: i3, i4, i1, not itself
    # or i3 at 7; with the pairs i3 at 7 makes, i2's row before 8 holds i1 3 times, i3 3, i4 2.
    assert s.row(2, 8) == [1, 3, 4]
    assert s.row(2, 7) == [1, 3, 4]  # i1 at 3 and 6, i3 at 4, i4 at 5
    assert s.row(3, 7) == [1, 2, 4]
    assert s.row(1, 3) == []  # the pair of i1 and i2 is made at 3


def test_ties_to_the_lower_id_and_popularity_rank():
    s = State([("a", "i3", 1), ("b", "i2", 2), ("c", "i4", 3), ("d", "i4", 4)], IX)
    assert s.popular(10, 10) == [4, 2, 3]
    assert popularity_rank(s, 3, 10, 10, set()) == 3
    assert popularity_rank(s, 3, 10, 10, {4}) == 2
    assert popularity_rank(s, 1, 10, 10, set()) == 4  # no events: after every item with some, then by ID
    assert popularity_rank(s, 4, 10, 10, {4}) == 0
    assert np.array_equal(s.counts(10, 10), [0, 0, 1, 1, 2])


def test_select_window():
    # 0.05812 and 0.05808 are equal to four decimals: the higher NDCG@10, then the shorter window.
    assert select_window([(1, 0.04, 0.03), (14, 0.05812, 0.0322), (30, 0.05808, 0.0309)]) == 14
    assert select_window([(7, 0.05, 0.02), (14, 0.05, 0.02)]) == 7
    assert select_window([(7, 0.0499, 0.09), (14, 0.0501, 0.02)]) == 14
