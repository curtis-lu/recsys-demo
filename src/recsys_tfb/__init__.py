import os

# Pin OpenMP thread count before any numpy / LightGBM import. Company hosts
# expose os.cpu_count()=240, which makes LightGBM spawn 240 OMP threads per
# boost iteration and thrashes cache / memory bandwidth (~500x per-iter
# slowdown observed during HPO). The in-params `num_threads: 4` is not
# enough — by the time LightGBM calls omp_set_num_threads, other libs that
# already initialized the OMP pool keep the larger thread count.
# `setdefault` so shell / wrapper can still override.
os.environ.setdefault("OMP_NUM_THREADS", "4")
