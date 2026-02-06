from icestream.kafkaserver.internal_topics import murmur2, partition_for_key


def test_murmur2_matches_kafka_vectors():
    cases = {
        b"21": -973932308,
        b"foobar": -790332482,
        b"a-little-bit-long-string": -985981536,
        b"a-little-bit-longer-string": -1486304829,
        b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8": -58897971,
        b"abc": 479470107,
    }

    for data, expected in cases.items():
        assert murmur2(data) == expected


def test_partition_for_key_uses_positive_hash():
    assert partition_for_key(b"foobar", 50) == 16
