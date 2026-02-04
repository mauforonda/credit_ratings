#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
from upload import upload_dataset
import argparse

url = "https://bcrdgdcprod.blob.core.windows.net/documents/entorno-internacional/documents/Serie_Historica_Spread_del_EMBI.xlsx"


def load_and_clean(url):
    df = pd.read_excel(url, skiprows=1)
    # Ignore annotation fields outside the main table
    df = df[[c for c in df.columns if "Unnamed" not in c]].copy()
    df.Fecha = pd.to_datetime(df.Fecha)
    df = df.set_index("Fecha").melt(ignore_index=False).reset_index()
    df.columns = ["fecha", "region", "valor"]
    return df


def do_upload():
    parser = argparse.ArgumentParser(
        description="Descarga y guarda valores del Emerging Markets Bond index."
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Sube los datos a Supabase (por defecto solo guarda CSV).",
    )
    args = parser.parse_args()
    return args.upload


df = load_and_clean(url)
root_dir = Path(__file__).resolve().parents[1]
data_dir = root_dir / "data"
data_dir.mkdir(parents=True, exist_ok=True)
df.sort_values(["fecha", "region"]).to_csv(data_dir / "embi.csv", index=False)

upload = do_upload()
if upload:
    upload_dataset("creditratings_embi", df, ["fecha", "region"])
