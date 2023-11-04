# Running Experiments

## `drop_caches`
Before, running any experimental code, we must first create a supplemental script for dropping Linux's file system cache.
Go into the `drop_caches` directory, run make, update owner and set the sticky bit:
```
cd drop_caches
make
sudo chown root drop_caches
sudo chmod u+s drop_caches
```
Then, install the binary somewhere and add to `PATH` to ensure the experiment scripts can use it.

## Generating Data
### Dependencies
We use a set of python scripts to generate and import data into a compatible format.
These scripts require the following python packages:
- numpy
- pandas
- scikit-learn
- click

### Forest Data
First, download the original csv from [here](http://kdd.ics.uci.edu/databases/covertype/covertype.html).
Then, use `scripts/gen_forest_data.py` to increase the dataset size, import the data into a compatible format, and generate the set of all possible forest predicates.
```
python scripts/gen_forest_data.py <input-csv> <output-db-path> <output-preds-path>
```

### Importing TPC-H and CH-benchmark Data
Use the scripts `scripts/tbl2db.py` and `scripts/tpc-h-tbl2db.py` to import `.tbl` data from the CH-benchmark and TCP-H respectively:
```
python scripts/tbl2db.py <input-ch-data-dir> <output-db-path>
python scripts/tpc-h-tbl2db.py <input-tpc-h-data-dir> <output-db-path>
```
Both scripts expect a single `.tbl` file containing all the data.

## Running Experiments
### Running Forest Experiments
Run the forest experiments with:
```
cargo build --release
./target/release/run-forest-exp --preds <forest-preds-path> <db-path>
```
The number of randomly generated queries and the number of trials per query can be controlled with `-n` and `-t` respectively.

### Running TPC-H and CH-benchmark Experiments
Run these experiments:
```
cargo build --release
./target/release/tpc-h-exp --db-path <db-path>
./target/release/ch-exp --db-path <db-path>
```
The number of trials can be controlled with `-t`.


## Running Tests
To run tests, simply run:
```
cargo test
```
