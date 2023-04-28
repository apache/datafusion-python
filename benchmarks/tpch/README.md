# DataFusion Python Benchmarks Derived from TPC-H

## Create Release Build

From repo root:

```bash
maturin develop --release
```

Note that release builds take a really long time, so you may want to temporarily comment out this section of the 
root Cargo.toml when frequently building.

```toml
[profile.release]
lto = true
codegen-units = 1
```

## Generate Data

```bash
./tpch-gen.sh 1
```

## Run Benchmarks

```bash
python tpch.py ./data ./queries
```

A summary of the benchmark timings will be written to `results.csv`. For example:

```csv
setup,1.4
q1,2978.6
q2,679.7
q3,2943.7
q4,2894.9
q5,3592.3
q6,1691.4
q7,3003.9
q8,3818.7
q9,4237.9
q10,2344.7
q11,526.1
q12,2284.6
q13,1009.2
q14,1738.4
q15,1942.1
q16,499.8
q17,5178.9
q18,4127.7
q19,2056.6
q20,2162.5
q21,8046.5
q22,754.9
total,58513.2
```