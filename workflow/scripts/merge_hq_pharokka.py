from pathlib import Path
import argparse
import pandas as pd

snakemake = globals().get("snakemake")


def merge_hq_pharokka(hq_path: str, mash_path: str, out_path: str) -> tuple[int, int, int]:
    hq = pd.read_csv(hq_path, sep="\t")
    mash = pd.read_csv(mash_path, sep="\t")

    if "contig" in mash.columns and "contig_id" not in mash.columns:
        mash = mash.rename(columns={"contig": "contig_id"})

    merged = hq.merge(mash, on="contig_id", how="left")

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, sep="\t", index=False)
    return len(hq), len(mash), len(merged)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Merge HQ and Pharokka MASH TSVs on contig id")
    p.add_argument("--hq", required=True)
    p.add_argument("--mash", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--log", required=False, default=None)
    return p.parse_args()


def run_with_snakemake() -> None:
    mash_path = Path(str(snakemake.input.pharokka_dir)) / f"{snakemake.wildcards.accession}_top_hits_mash_inphared.tsv"
    if not mash_path.exists():
        raise FileNotFoundError(f"Expected Pharokka MASH file not found: {mash_path}")

    hq_n, mash_n, merged_n = merge_hq_pharokka(
        str(snakemake.input.hq),
        str(mash_path),
        str(snakemake.output.tsv),
    )

    if snakemake.log:
        Path(str(snakemake.log[0])).parent.mkdir(parents=True, exist_ok=True)
        with open(str(snakemake.log[0]), "w") as log:
            print(f"Merged {hq_n} HQ contigs & {mash_n} MASH hits - {merged_n} rows", file=log)


def run_with_cli() -> None:
    args = parse_args()
    hq_n, mash_n, merged_n = merge_hq_pharokka(args.hq, args.mash, args.out)

    msg = f"Merged {hq_n} HQ contigs & {mash_n} MASH hits - {merged_n} rows"
    if args.log:
        Path(args.log).parent.mkdir(parents=True, exist_ok=True)
        with open(args.log, "w") as log:
            print(msg, file=log)
    else:
        print(msg)


if __name__ == "__main__":
    if snakemake is not None:
        run_with_snakemake()
    else:
        run_with_cli()
