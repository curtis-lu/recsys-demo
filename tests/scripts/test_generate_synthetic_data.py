import numpy as np
from scripts.generate_synthetic_data import (
    generate_feature_table,
    generate_inference_population,
)


def test_inference_population_grain_unique():
    rng = np.random.default_rng(0)
    ft = generate_feature_table(rng)
    pop = generate_inference_population(ft)
    # 一 (snap_date, cust_id) 一列
    assert not pop.duplicated(subset=["snap_date", "cust_id"]).any()
    # 母體 ⊆ feature_table 的 (snap_date, cust_id)
    ft_keys = set(map(tuple, ft[["snap_date", "cust_id"]].drop_duplicates().values))
    pop_keys = set(map(tuple, pop[["snap_date", "cust_id"]].values))
    assert pop_keys <= ft_keys
