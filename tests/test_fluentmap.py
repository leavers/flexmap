import random
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple, Type, TypeVar, Union

import pytest

from fluentmap import Arguments
from fluentmap import map as fmap


T = TypeVar("T")


def map_func(
    v: T,
    *vs: T,
    wait_time: Optional[float] = None,
    exc_type: Optional[Type[Exception]] = None,
) -> Union[T, Tuple[T, ...]]:
    if exc_type is not None:
        raise exc_type("mock")
    if wait_time is not None:
        time.sleep(wait_time)
    if vs:
        return (v, *vs)
    return v


def test_simple_map():
    # should accept zero-arg iterables
    assert list(fmap(map_func, ())) == list(map(map_func, ()))
    # most common case
    assert list(fmap(map_func, (0, 1, 2))) == list(map(map_func, (0, 1, 2)))
    # should accept set
    assert list(fmap(map_func, {0, 1, 2})) == list(map(map_func, {0, 1, 2}))
    # should accept str
    assert list(fmap(map_func, "012")) == list(map(map_func, "012"))
    # should accpet range
    assert list(fmap(map_func, range(6), batch_size=0)) == list(range(6))
    # should accept iterables of iterables
    assert list(fmap(map_func, ((0, 0), (1, 1)))) == list(
        map(map_func, ((0, 0), (1, 1)))
    )
    # should have same behavior with builtin map on receiving multiple iterables of
    # different length
    assert list(fmap(map_func, (0, 1, 2), (3, 4))) == list(
        map(map_func, (0, 1, 2), (3, 4))
    )
    # should accept multiple iterables of iterables of different length
    assert list(fmap(map_func, ((0, 0), (1, 1), (2, 2)), ((3, 3), (4, 4)))) == list(
        map(
            map_func,
            ((0, 0), (1, 1), (2, 2)),
            ((3, 3), (4, 4)),
        )
    )
    # test of batch_size
    assert list(fmap(map_func, range(6), batch_size=3)) == [
        [0, 1, 2],
        [3, 4, 5],
    ]
    # test of batches that cannot be rounded
    assert list(fmap(map_func, range(7), batch_size=3)) == [
        [0, 1, 2],
        [3, 4, 5],
        [6],
    ]
    # test of batches of iterables that cannot be rounded
    assert list(fmap(map_func, [(i, i) for i in range(7)], batch_size=3)) == [
        [(0, 0), (1, 1), (2, 2)],
        [(3, 3), (4, 4), (5, 5)],
        [(6, 6)],
    ]
    # test of on_return
    assert list(fmap(map_func, (0, 1, 2), on_return=lambda x: x + 1)) == [1, 2, 3]
    # test of combination of batch_size and on_return
    assert list(
        fmap(
            map_func,
            range(7),
            batch_size=3,
            on_return=lambda row: [x + 1 for x in row],
        )
    ) == [
        [1, 2, 3],
        [4, 5, 6],
        [7],
    ]
    # test of combination of Arguments and on_return
    assert list(
        fmap(
            map_func,
            [Arguments(i, i) for i in range(2)],
            on_return=lambda row: [x + 1 for x in row],
        )
    ) == [[1, 1], [2, 2]]

    # test num_prepare works
    assert list(fmap(map_func, (0, 1, 2), num_prepare=1)) == [0, 1, 2]
    assert list(fmap(map_func, (0, 1, 2), num_prepare=2)) == [0, 1, 2]

    track: List[int]

    def yield_data(n: int):
        for i in range(n):
            track.append(i)
            yield i

    def func1(res: int):
        if res == 0:
            time.sleep(0.15)
        track.append(-1)
        return res

    track = []
    assert list(fmap(func1, yield_data(2), num_prepare=2)) == [0, 1]
    assert track == [0, 1, -1, -1]

    def func2(res: int):
        if res < 2:
            time.sleep(0.15)
        track.append(-1)
        return res

    track = []
    assert list(fmap(func2, yield_data(4), num_prepare=2)) == [0, 1, 2, 3]
    assert track == [0, 1, 2, -1, 3, -1, -1, -1]

    def func3(res: List[int]):
        if res == [0, 1] or res == [2, 3]:
            time.sleep(0.15)
        track.append(-1)
        return res

    track = []
    assert list(fmap(func3, yield_data(9), batch_size=2, num_prepare=3)) == [
        [0, 1],
        [2, 3],
        [4, 5],
        [6, 7],
        [8],
    ]
    assert track == [0, 1, 2, 3, 4, 5, 6, 7, -1, 8, -1, -1, -1, -1]

    # test imap stop asap
    def process_exc(res: List[int]):
        time.sleep(0.15)
        track.append(-1)
        raise RuntimeError("mock")

    track = []
    with pytest.raises(RuntimeError):
        _ = list(fmap(process_exc, yield_data(9), batch_size=2, num_prepare=2))
    assert len(track) in (7, 8)
    assert track[:7] == [0, 1, 2, 3, 4, 5, -1]
    if len(track) == 8:
        assert track[7] == 6


def test_concurrent_map():
    with ThreadPoolExecutor(max_workers=256) as executor:
        # should accept zero-arg iterables
        assert list(fmap(map_func, (), executor=executor)) == list(map(map_func, ()))
        # most common case
        assert list(fmap(map_func, (0, 1, 2), executor=executor)) == list(
            map(map_func, (0, 1, 2))
        )
        # should accept set
        assert list(fmap(map_func, {0, 1, 2}, executor=executor)) == list(
            map(map_func, {0, 1, 2})
        )
        # should accept str
        assert list(fmap(map_func, "012", executor=executor)) == list(
            map(map_func, "012")
        )
        # should accpet range
        assert list(
            fmap(
                map_func,
                range(6),
                batch_size=0,
                executor=executor,
            )
        ) == list(range(6))
        # should accept iterables of iterables
        assert list(fmap(map_func, ((0, 0), (1, 1)), executor=executor)) == list(
            map(map_func, ((0, 0), (1, 1)))
        )
        # should have same behavior with builtin map on receiving multiple iterables of
        # different length
        assert list(fmap(map_func, (0, 1, 2), (3, 4), executor=executor)) == list(
            map(map_func, (0, 1, 2), (3, 4))
        )
        # should accept multiple iterables of iterables of different length
        assert list(
            fmap(
                map_func,
                ((0, 0), (1, 1), (2, 2)),
                ((3, 3), (4, 4)),
                executor=executor,
            )
        ) == list(
            map(
                map_func,
                ((0, 0), (1, 1), (2, 2)),
                ((3, 3), (4, 4)),
            )
        )
        # test of batch_size
        assert list(fmap(map_func, range(6), batch_size=3, executor=executor)) == [
            [0, 1, 2],
            [3, 4, 5],
        ]
        # test of batches that cannot be rounded
        assert list(fmap(map_func, range(7), batch_size=3, executor=executor)) == [
            [0, 1, 2],
            [3, 4, 5],
            [6],
        ]
        # test of batches of iterables that cannot be rounded
        assert list(
            fmap(
                map_func,
                [(i, i) for i in range(7)],
                batch_size=3,
                executor=executor,
            )
        ) == [
            [(0, 0), (1, 1), (2, 2)],
            [(3, 3), (4, 4), (5, 5)],
            [(6, 6)],
        ]
        # test of on_return
        assert list(
            fmap(map_func, (0, 1, 2), executor=executor, on_return=lambda x: x + 1)
        ) == [1, 2, 3]
        # test of combination of batch_size and on_return
        assert list(
            fmap(
                map_func,
                range(7),
                batch_size=3,
                executor=executor,
                on_return=lambda row: [x + 1 for x in row],
            )
        ) == [
            [1, 2, 3],
            [4, 5, 6],
            [7],
        ]
        # test of combination of Arguments
        assert list(
            fmap(
                map_func,
                [Arguments(i, wait_time=0.4 - 0.2 * i) for i in range(3)],
                executor=executor,
            )
        ) == [0, 1, 2]
        # result should be inversed when sort_by_completion=True
        assert list(
            fmap(
                map_func,
                [Arguments(i, wait_time=0.4 - 0.2 * i) for i in range(3)],
                executor=executor,
                sort_by_completion=True,
            )
        ) == [2, 1, 0]
        # test of combination of sort_by_completion=True and on_return
        assert list(
            fmap(
                map_func,
                [Arguments(i, wait_time=0.4 - 0.2 * i) for i in range(3)],
                executor=executor,
                sort_by_completion=True,
                on_return=lambda x: x + 1,
            )
        ) == [3, 2, 1]
        # test integrity of high-concurrency situation
        assert list(
            fmap(
                map_func,
                [Arguments(i, wait_time=random.random()) for i in range(1024)],
                executor=executor,
                on_return=lambda x: x + 1,
            )
        ) == list(range(1, 1025))
