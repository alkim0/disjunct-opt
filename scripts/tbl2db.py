from pathlib import Path

import pandas as pd

import click

import build_col

SCHEMAS = {
    "item": [
        ("i_id", "int"),
        ("i_im_id", "int"),
        ("i_name", "string"),
        ("i_price", "double"),
        ("i_data", "string"),
    ],
    "orderline": [
        ("ol_o_id", "int"),
        ("ol_d_id", "int"),
        ("ol_w_id", "int"),
        ("ol_number", "int"),
        ("ol_i_id", "int"),
        ("ol_supply_w_id", "int"),
        ("ol_delivery_d", "string"),
        ("ol_quantity", "int"),
        ("ol_amount", "double"),
        ("ol_dist_info", "string"),
    ],
}


def convert_table(table_path: Path, outdir: Path) -> None:
    table = table_path.stem.lower()
    assert table in SCHEMAS
    cols = SCHEMAS[table]

    (outdir / table).mkdir(exist_ok=True)

    print(f"Reading {table_path} ... ", end="", flush=True)
    df = pd.read_table(table_path, sep="|", names=[c[0] for c in cols])
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
