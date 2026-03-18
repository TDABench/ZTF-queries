import pandas as pd
import numpy as np
import argparse
import os

from ZTF_alert_utils import (make_triplet, prep_alerts, crop_triplets)

KOWALSKI_USER = os.environ.get('KOWALSKI_USER')
KOWALSKI_PASS = os.environ.get('KOWALSKI_PASS')

if KOWALSKI_USER is not None and KOWALSKI_PASS is not None:
    from penquins import Kowalski

    k = Kowalski(instances={
        'kowalski': {
            'protocol': 'https',
            'port': 443,
            'host': 'kowalski.caltech.edu',
            'username': KOWALSKI_USER,
            'password': KOWALSKI_PASS
        }
    })
else:
    print("Kowalski credentials were not found. \
           They must be set as environment variables KOWALSKI_USER and \
           KOWALSKI_PASS. \nQuerying Kowalski will not be possible.")
    exit()


def query_kowalski(ZTFID, kowalski, programid, include_cutouts: bool = True,
                   raw_dir: str = None):
    """
    Query Kowalski for alerts for a (list of) ZTFID(s) and persist raw results.

    Parameters
    ----------
    ZTFID: string or list
        Object IDs to query for (e.g. ZTF22abwqedu)

    kowalski:
        a kowalski api object created with the kowalski library

    include_cutouts (optional): bool
        Easy flag for including/excluding cutouts from query projection.
        Cutouts are not processed here; they are saved as part of the raw data.

    programid:
        which program to pull alerts from (1=public, 2=collab, 3=caltech mode)

    raw_dir: str
        directory where all query results will be individually saved to disk
        before any processing is done. This must be provided.

    Notes
    -----
    This function is responsible only for querying and persisting raw alerts.
    It does not collate alerts across objects or return them to the caller.

    Adapted from: https://github.com/nabeelre/BTSbot/ and earlier from
        https://github.com/growth-astro/ztfrest/
    See here for ZTF alert packet feature definitions:
        https://zwickytransientfacility.github.io/ztf-avro-alert/schema.html

    This can also be done by querying from Fritz instead of Kowalski.
    """

    # Deal with input being a single ZTF object (string) and multiple (list)
    if isinstance(ZTFID, str):
        list_ZTFID = [ZTFID]
    elif isinstance(ZTFID, list):
        list_ZTFID = ZTFID
    else:
        raise ValueError(f"ZTFID must be a list or a string, not {type(ZTFID)}")

    if not isinstance(raw_dir, str) or raw_dir == "":
        raise ValueError(
            "A valid directory path must be provided for `raw_dir` in "
            "`query_kowalski`."
        )

    if include_cutouts:
        cutout_query_dict = {
            "cutoutScience": 1,
            "cutoutTemplate": 1,
            "cutoutDifference": 1
        }
    else:
        cutout_query_dict = {}

    # For each object requested ...
    for ZTFID in list_ZTFID:
        suffix = "_co" if include_cutouts else ""
        filename = f"{ZTFID}_prog{programid}{suffix}.npy"
        raw_path = os.path.join(raw_dir, filename)

        # If the appropriate raw file already exists, skip querying
        if os.path.exists(raw_path):
            print(f"  Found existing raw data for {ZTFID} (programid={programid}, "
                  f"include_cutouts={include_cutouts}); skipping query")
            continue

        # Set up query
        query = {
            "query_type": "find",
            "query": {
                "catalog": "ZTF_alerts",
                "filter": {
                    # take only alerts for specified object
                    'objectId': ZTFID,
                    # take only alerts with specified programid
                    "candidate.programid": programid,
                },
                # what quantities to recieve
                "projection": {
                    "_id": 0,
                    "objectId": 1,

                    "candidate.candid": 1,
                    "candidate.programid": 1,
                    "candidate.programpi": 1,
                    "candidate.fid": 1,
                    "candidate.pid": 1,
                    "candidate.isdiffpos": 1,
                    "candidate.ndethist": 1,
                    "candidate.ncovhist": 1,
                    "candidate.sky": 1,
                    "candidate.pdiffimfilename": 1,
                    "candidate.fwhm": 1,
                    "candidate.seeratio": 1,
                    "candidate.mindtoedge": 1,
                    "candidate.nneg": 1,
                    "candidate.nbad": 1,
                    "candidate.scorr": 1,
                    "candidate.dsnrms": 1,
                    "candidate.ssnrms": 1,
                    "candidate.exptime": 1,
                    "candidate.tblid": 1,
                    "candidate.nid": 1,
                    "candidate.rcid": 1,
                    "candidate.xpos": 1,
                    "candidate.ypos": 1,

                    "candidate.field": 1,
                    "candidate.jd": 1,
                    "candidate.ra": 1,
                    "candidate.dec": 1,

                    "candidate.magpsf": 1,
                    "candidate.sigmapsf": 1,
                    "candidate.diffmaglim": 1,
                    "candidate.magap": 1,
                    "candidate.sigmagap": 1,
                    "candidate.magapbig": 1,
                    "candidate.sigmagapbig": 1,
                    "candidate.magdiff": 1,
                    "candidate.magzpsci": 1,
                    "candidate.magzpsciunc": 1,
                    "candidate.magzpscirms": 1,

                    "candidate.distnr": 1,
                    "candidate.magnr": 1,
                    "candidate.sigmanr": 1,
                    "candidate.chinr": 1,
                    "candidate.sharpnr": 1,
                    "candidate.ranr": 1,
                    "candidate.decnr": 1,
                    "candidate.magfromlim": 1,
                    "candidate.aimage": 1,
                    "candidate.bimage": 1,
                    "candidate.aimagerat": 1,
                    "candidate.bimagerat": 1,
                    "candidate.elong": 1,

                    "candidate.neargaia": 1,
                    "candidate.neargaiabright": 1,
                    "candidate.maggaia": 1,
                    "candidate.maggaiabright": 1,

                    "candidate.drb": 1,
                    "candidate.drbversion": 1,
                    "candidate.classtar": 1,
                    "candidate.sgscore1": 1,
                    "candidate.distpsnr1": 1,
                    "candidate.sgscore2": 1,
                    "candidate.distpsnr2": 1,
                    "candidate.sgscore3": 1,
                    "candidate.distpsnr3": 1,
                    "candidate.rb": 1,
                    "candidate.rbversion": 1,
                    "candidate.ssdistnr": 1,
                    "candidate.ssmagnr": 1,
                    "candidate.ssnamenr": 1,
                    "candidate.sumrat": 1,

                    "candidate.jdstarthist": 1,
                    "candidate.jdendhist": 1,
                    "candidate.jdstartref": 1,
                    "candidate.jdendref": 1,
                    "candidate.nframesref": 1,

                    "candidate.sgmag1": 1,
                    "candidate.srmag1": 1,
                    "candidate.simag1": 1,
                    "candidate.szmag1": 1,

                    "candidate.sgmag2": 1,
                    "candidate.srmag2": 1,
                    "candidate.simag2": 1,
                    "candidate.szmag2": 1,

                    "candidate.sgmag3": 1,
                    "candidate.srmag3": 1,
                    "candidate.simag3": 1,
                    "candidate.szmag3": 1,

                    "candidate.nmtchps": 1,
                    "candidate.clrcoeff": 1,
                    "candidate.clrcounc": 1,
                    "candidate.chipsf": 1,
                    "candidate.nmatches": 1,
                    "candidate.zpclrcov": 1,
                    "candidate.zpmed": 1,
                    "candidate.clrmed": 1,
                    "candidate.clrrms": 1,
                    "candidate.tooflag": 1,
                    "candidate.objectidps1": 1,
                    "candidate.objectidps2": 1,
                    "candidate.objectidps3": 1,
                    "candidate.rfid": 1,
                    "candidate.dsdiff": 1,

                    "classifications.acai_h": 1,
                    "classifications.acai_v": 1,
                    "classifications.acai_o": 1,
                    "classifications.acai_n": 1,
                    "classifications.acai_b": 1,
                    "classifications.bts": 1,
                } | cutout_query_dict
            }
        }

        # Execute query
        r = kowalski.query(query)

        if r['kowalski']['data'] == []:
            # No alerts recieved - possibly due to connection or permissions
            print(f"  No programid={programid} data for", ZTFID)
            continue
        else:
            # returned data is list of dicts, each dict is an alert packet
            object_alerts = r['kowalski']['data']

        # Save raw data
        if not os.path.exists(raw_dir):
            os.makedirs(raw_dir, exist_ok=True)
        np.save(raw_path, object_alerts)
        print(f"  Finished querying {ZTFID}, saved to {filename}")

    print(f"Finished all programid={programid} queries\n\n\n")


def process_raw_alerts(query_df,
                       programids: list[int] = [1, 2],
                       include_cutouts: bool = True,
                       normalize_cutouts: bool = True,
                       cutout_size: int = 63,
                       raw_dir: str = None,
                       output_dir: str = "download/"):
    """
    Build training data from previously downloaded raw alert files.

    Parameters
    ----------
    query_df: DataFrame
        dataframe with column "ZTFID"

    programids: list[int]
        list of programids to process (default [1, 2])

    include_cutouts (optional): bool
        whether to construct image triplets from cutout data

    normalize_cutouts (optional): bool
        normalize cutouts by the Frobenius norm (L2)

    cutout_size (optional): int
        size of cutout images (default 63)

    raw_dir: str
        directory containing raw `.npy` alert files written by `query_kowalski`

    output_dir (optional): str
        directory where processed triplets and candidate metadata will be saved
    """

    if not isinstance(raw_dir, str) or raw_dir == "":
        raise ValueError(
            "A valid directory path must be provided for `raw_dir` when "
            "processing raw alerts."
        )

    print(f"Processing raw alerts for {len(query_df)} objects")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    all_alerts = []
    all_triplets = [] if include_cutouts else None

    for ztfid in query_df['ZTFID']:
        for programid in programids:
            suffix = "_co" if include_cutouts else ""
            path = os.path.join(raw_dir, f"{ztfid}_prog{programid}{suffix}.npy")
            if not os.path.exists(path):
                continue

            object_alerts = np.load(path, allow_pickle=True)

            if include_cutouts:
                triplets = np.empty((len(object_alerts), 63, 63, 3))
                to_drop = np.array((), dtype=int)

                for i, alert in enumerate(object_alerts):
                    triplets[i], drop = make_triplet(
                        alert, normalize=normalize_cutouts
                    )
                    if drop:
                        to_drop = np.append(to_drop, int(i))

                if len(to_drop) > 0:
                    triplets = np.delete(triplets, list(to_drop), axis=0)
                    object_alerts = np.delete(
                        object_alerts, list(to_drop), axis=0
                    )

                for alert, triplet in zip(object_alerts, triplets):
                    alert['triplet'] = triplet

                if triplets.size > 0:
                    all_triplets.append(triplets)

            all_alerts.extend(list(object_alerts))

    if include_cutouts and all_triplets:
        triplets = np.concatenate(all_triplets, axis=0)

        if cutout_size != 63:
            triplets = crop_triplets(triplets, cutout_size)

        triplet_filename = "triplets" + \
            f"{cutout_size if cutout_size != 63 else ''}.npy"
        np.save(os.path.join(output_dir, triplet_filename), triplets)
        del triplets
        print("Saved and purged triplets\n")

    # augment alerts with custom features (labels not assigned here)
    cand_data = prep_alerts(all_alerts, label=None)

    cand_data.to_csv(os.path.join(output_dir, "candidates.csv"), index=False)
    del cand_data
    print("Saved and purged candidate data")


def main():
    parser = argparse.ArgumentParser(
        description="Query Kowalski for ZTF alerts and build training data."
    )

    parser.add_argument(
        "--stage",
        choices=["query", "process", "both"],
        required=True,
        help=(
            "Stage to run: 'query' to download raw alerts, 'process' to build "
            "training data from previously downloaded raw alerts, or 'both' "
            "to run query followed by process."
        ),
    )
    parser.add_argument(
        "-i", "--input",
        default="bts.csv",
        help="Path to input CSV containing ZTFIDs (default: bts.csv)."
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="downloads/",
        help="Directory where output files will be written (default: downloads)."
    )
    parser.add_argument(
        "--include-cutouts",
        action="store_true",
        default=False,
        help="Include cutouts when querying Kowalski (default: disabled)."
    )
    parser.add_argument(
        "--no-normalize-cutouts",
        dest="normalize_cutouts",
        action="store_false",
        help="Disable normalization of cutouts (enabled by default)."
    )
    parser.set_defaults(normalize_cutouts=True)
    parser.add_argument(
        "--cutout-size",
        type=int,
        default=63,
        help="Size of cutout images (default: 63)."
    )
    parser.add_argument(
        "--raw-dir",
        default=None,
        help=(
            "Directory for raw alert .npy files used by query/process/both "
            "stages. Required for all stages."
        ),
    )
    args = parser.parse_args()

    query_df = pd.read_csv(args.input, index_col=None)

    if args.stage in ("query", "both"):
        if args.raw_dir is None:
            parser.error("--raw-dir is required when --stage=query or --stage=both")

        print(f"Querying Kowalski for {len(query_df)} objects")

        if k.ping('kowalski'):
            print("Connected to Kowalski")
        else:
            print("Unable to connect to Kowalski")
            exit()

        ztfids = query_df['ZTFID'].to_list()

        query_kowalski(
            ztfids,
            k,
            programid=1,
            include_cutouts=args.include_cutouts,
            raw_dir=args.raw_dir,
        )
        query_kowalski(
            ztfids,
            k,
            programid=2,
            include_cutouts=args.include_cutouts,
            raw_dir=args.raw_dir,
        )

    if args.stage in ("process", "both"):
        if args.raw_dir is None:
            parser.error("--raw-dir is required when --stage=process or --stage=both")

        process_raw_alerts(
            query_df=query_df,
            programids=[1, 2],
            raw_dir=args.raw_dir,
            output_dir=args.output_dir,
            include_cutouts=args.include_cutouts,
            normalize_cutouts=args.normalize_cutouts,
            cutout_size=args.cutout_size,
        )


if __name__ == "__main__":
    """
    Example usages:

    # 1) Download raw alerts only (no processing yet)
    python query_train_data.py \\
        --stage query \\
        --input bts.csv \\
        --raw-dir raw_alerts/

    # 2) Process previously downloaded raw alerts only
    python query_train_data.py \\
        --stage process \\
        --input bts.csv \\
        --raw-dir raw_alerts/ \\
        --output-dir downloads/ \\
        --include-cutouts \\
        --cutout-size 63

    # 3) Run both stages in one go (download then process)
    python query_train_data.py \\
        --stage both \\
        --input bts.csv \\
        --raw-dir raw_alerts/ \\
        --output-dir downloads/ \\
        --include-cutouts \\
        --cutout-size 63
    """
    main()
