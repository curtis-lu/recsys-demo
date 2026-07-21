def test_per_item_ap_available_from_common():
    from recsys_tfb.diagnosis.metric._common import per_item_ap
    assert callable(per_item_ap)


def test_item_ability_reuses_the_shared_per_item_ap():
    """釘住「同一個函式物件」而不是「兩邊算出來一樣」。

    後者對一份被複製貼上的副本照樣成立——而複製品會漂移，這正是本 task
    要消滅的東西。
    """
    import recsys_tfb.diagnosis.metric._common as common
    import recsys_tfb.diagnosis.metric.item_ability._compute as ia
    assert ia.per_item_ap is common.per_item_ap
