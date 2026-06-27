import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from punctuation_audit import audit, load_punctuation, _DEFAULT_PUNCT_FILE

PUNCT = {"—", " ", "，", "：", "。", "（", "）", "；", "…"}


def test_punctuation_only_true():
    diff = "-先看懂它怎麼跑——再調快。\n+先看懂它怎麼跑，再調快。\n"
    r = audit(diff, PUNCT)
    assert r["punctuation_only"] and r["symmetric"]


def test_word_change_detected():
    diff = "-先看懂它怎麼跑——再調快。\n+先看懂它如何跑，再調快。\n"
    assert not audit(diff, PUNCT)["punctuation_only"]


def test_ignores_diff_headers():
    diff = "--- a/x.md\n+++ b/x.md\n@@ -1 +1 @@\n-甲——乙\n+甲，乙\n"
    r = audit(diff, PUNCT)
    assert r["punctuation_only"]
    assert r["removed"] == ["甲——乙"]


def test_punctuation_set_file_loads():
    punct = load_punctuation(_DEFAULT_PUNCT_FILE)
    assert {"，", "—", " "} <= punct
