from pathlib import Path

import numpy as np
import pandas as pd

import click

import build_col

SCHEMAS = {
    "lineitem": [
        ("l_orderkey", "int"),
        ("l_partkey", "int"),
        ("l_suppkey", "int"),
        ("l_linenumber", "int"),
        ("l_quantity", "double"),
        ("l_extendedprice", "double"),
        ("l_discount", "double"),
        ("l_tax", "double"),
        ("l_returnflag", "string"),
        ("l_linestatus", "string"),
        ("l_shipdate", "string"),
        ("l_commitdate", "string"),
        ("l_receiptdate", "string"),
        ("l_shipinstruct", "string"),
        ("l_shipmode", "string"),
        ("l_comment", "string"),
        (
            "extra_field",
            "string",
        ),  # dbgen ends each row with a |, which confuses parser
    ],
    "part": [
        ("p_partkey", "int"),
        ("p_name", "string"),
        ("p_mfgr", "string"),
        ("p_brand", "string"),
        ("p_type", "string"),
        ("p_size", "int"),
        ("p_container", "string"),
        ("p_retailprice", "double"),
        ("p_comment", "string"),
        (
            "extra_field",
            "string",
        ),  # dbgen ends each row with a |, which confuses parser
    ],
}


def to_pd_dtype(col_type: str):
    return {"int": np.int32, "double": np.float64, "string": str}[col_type]


def convert_table(table_path: Path, outdir: Path) -> None:
    table = table_path.stem.lower()
    assert table in SCHEMAS
    cols = SCHEMAS[table]

    (outdir / table).mkdir(exist_ok=True)

    print(f"Reading {table_path} ... ", end="", flush=True)
    df = pd.read_table(
        table_path,
        sep="|",
        names=[c[0] for c in cols],
        dtype={c: to_pd_dtype(ctype) for c, ctype in cols},
    )
    print("Done!")

    print(df.columns)
    print(df.dtypes)

    for col, col_type in cols:
        if col_type == "string":
            df[col].fillna("", inplace=True)

        build_col.build_col(outdir / table / col, col_type, vals=df[col])

    with open(outdir / table / "__schema__", "w") as f:
        print(",".join([c[0] for c in cols]), file=f)
        print(",".join([c[1] for c in cols]), file=f)
        print(",".join([""] * len(cols)), file=f)


@click.command()
@click.option("-t", "--tables", help="Comma-separated list of tables to convert")
@click.argument("indir", type=Path)
@click.argument("outdir", type=Path)
def cli(tables: str | None, indir: Path, outdir: Path) -> None:
    outdir.mkdir(exist_ok=True)

    if tables:
        tables = tables.split(",")

    for table_path in indir.iterdir():
        table = table_path.stem.lower()

        if not tables or table in tables:
            convert_table(table_path, outdir)


if __name__ == "__main__":
    cli()
