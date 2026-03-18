# ZTF-queries

Collect ZTF alert packets (and optionally image cutouts) for the TDABench sample
from the ZTF internal database (Kowalski).

## Usage

We note here that these scripts are not intended to be used by TDABench users, rather
these show how the TDABench data was obtained.

### 1) Prerequisites

- **Kowalski access**: these scripts query `kowalski.caltech.edu` via `penquins`.
- **Credentials**: set environment variables:

```bash
export KOWALSKI_USER="..."
export KOWALSKI_PASS="..."
```

### 2) Prepare an input CSV

`query_train_data.py` expects a CSV with a column named **`ZTFID`**, e.g.:

```csv
ZTFID
ZTF22abwqedu
ZTF21aaabxyz
```

### 3) Run query + processing

This downloads raw alerts into `--raw-dir`, then builds the processed outputs in
`--output-dir`:

```bash
python query_train_data.py \
  --stage both \
  --input bts.csv \
  --raw-dir raw_alerts/ \
  --output-dir downloads/ \
  --include-cutouts \
  --cutout-size 63
```

## `query_train_data.py` (main entrypoint)

`query_train_data.py` is a two-stage pipeline:

- **Stage `query`**: query Kowalski for each ZTF objectId and save raw alert
  packets to disk as `.npy` (one file per object per programid).
- **Stage `process`**: load those raw `.npy` files, optionally build image
  triplets from cutouts, compute/augment candidate features, then write:
  - `candidates.csv` (tabular per-alert metadata/features)
  - `triplets*.npy` (stacked \(N, H, W, 3\) array) if `--include-cutouts`

### What it queries

The entire ZTF alert packet (including image cutouts) for all alerts from all sources in the TDABench dataset.

### Command-line arguments

- **`--stage {query,process,both}`**: required. Controls which pipeline stage(s)
  run.
- **`--input` / `-i`**: input CSV path (default `bts.csv`). Must include `ZTFID`.
- **`--raw-dir`**: required for all stages. Directory where raw `.npy` alert
  files are saved/read.
- **`--output-dir` / `-o`**: processed outputs directory (default `downloads/`).
- **`--include-cutouts`**: if set, include cutouts in the query and generate
  triplets in processing (default is **off**).
- **`--no-normalize-cutouts`**: disable L2/Frobenius normalization of cutouts
  (normalization is **on** by default).
- **`--cutout-size`**: output cutout size (default `63`). If not 63, triplets
  are center-cropped and renormalized.

### Example workflows

#### Download raw alerts only

```bash
python query_train_data.py \
  --stage query \
  --input bts.csv \
  --raw-dir raw_alerts/
```

Outputs (per ZTFID, per programid):
- **Without cutouts**: `raw_alerts/<ZTFID>_prog{1,2}.npy`
- **With cutouts** (`--include-cutouts`): `raw_alerts/<ZTFID>_prog{1,2}_co.npy`

#### Process previously-downloaded raw alerts

```bash
python query_train_data.py \
  --stage process \
  --input bts.csv \
  --raw-dir raw_alerts/ \
  --output-dir downloads/ \
  --include-cutouts \
  --cutout-size 63
```

Outputs in `downloads/`:
- **`candidates.csv`**: per-alert rows with candidate fields + classifications
  plus custom features computed in `prep_alerts()`.
- **`triplets.npy`** (or `triplets<cutout-size>.npy`): stacked triplets array if
  `--include-cutouts` is enabled.

## `ZTF_alert_utils.py` (supporting utilities)

`ZTF_alert_utils.py` provides the helper functions used by
`query_train_data.py`’s processing stage (and some extra utilities):

- **`make_triplet(alert, normalize=True)`**: unzip FITS cutouts from an alert
  packet and assemble a \(63,63,3\) science/template/difference triplet,
  optionally L2-normalized; flags corrupted/invalid cutouts.
- **`crop_triplets(triplets, crop_to_size)`**: center-crop triplets to a smaller
  square size and renormalize.
- **`prep_alerts(alerts, label)`**: convert a list of alert packets into a
  `pandas.DataFrame`, merge `candidate` + `classifications`, and compute extra
  per-object/per-alert features (e.g. peak mag, age, days since peak). It also
  attempts to query auxiliary non-detection history via Kowalski when possible.
- **`plot_triplet(trip)`**: quick visualization helper.

