#!/usr/bin/env python3

import json
import pandas as pd
from pathlib import Path

# con documentos JSON como https://www.fitchratings.com/entity/bolivia-80962881

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
FITCH_DIR = Path(__file__).resolve().parent / "fitch"

fitch = {
    "Bolivia": "bolivia",  # Un par de Nombre de país : nombre de documento JSON
    "Perú": "peru",
    "Argentina": "argentina",
    "Brasil": "brasil",
    "Chile": "chile",
    "Colombia": "colombia",
    "Ecuador": "ecuador",
    "Paraguay": "paraguay",
}

# Qué tipos de ratings nos interesan

rating_terms = {
    "Long Term Issuer Default Rating": "long",
    "Short Term Issuer Default Rating": "short",
}


def procesar_ratings(fn, pais, rating_terms):
    """
    Procesa un JSON con ratings para un país y retorna
    una serie de tiempos con ratings en los tipos `rating_types`
    y una columna con el nombre de país
    """
    with open(fn, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data["data"]["getEntity"]["ratingHistory"])
    df = df[
        [
            "ratingActionDescription",
            "ratingCode",
            "ratingTypeDescription",
            "ratingEffectiveDate",
        ]
    ].copy()
    df.columns = ["cambio", "rating", "term", "fecha"]
    df = df[df.term.isin(rating_terms.keys())]
    df.term = df.term.map(rating_terms)
    df.fecha = pd.to_datetime(df.fecha).dt.date
    df["pais"] = pais
    return df[["pais", "fecha", "term", "rating", "cambio"]]


DATA_DIR.mkdir(parents=True, exist_ok=True)
df = pd.concat(
    [
        procesar_ratings(FITCH_DIR / f"{fitch[pais]}.json", pais, rating_terms)
        for pais in fitch.keys()
    ]
)
df.to_csv(DATA_DIR / "fitch.csv", index=False)
