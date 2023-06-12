from cow_bff.heatmap import calculate_time_overlap, compute_heatmap
import pytest


def test_calculate_time_overlap():
    assert calculate_time_overlap(0, 10, 20, 30) == 0
    assert calculate_time_overlap(0, 20, 10, 30) == 10
    assert calculate_time_overlap(0, 20, 5, 15) == 10