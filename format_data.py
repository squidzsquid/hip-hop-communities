from datetime import datetime
from itertools import combinations
import numpy as np
import pandas as pd


def released_to_timestamp(year: str) -> int:
    return int(datetime(int(year), 1, 1).timestamp())


def clean_data(data: pd.DataFrame, out_path: str = None) -> pd.DataFrame:
    # Exclude all cases with missing year/artist or with only one artist listed (i.e. collaborations only)
    d = data.copy()
    d = d[(~d[["artists", "released", "styles"]].isna().any(1)) & (d["released"] != "0000") & (d["artists"].str.contains(","))]

    d["year"] = d["released"].apply(lambda x: x.split("-")[0])
    ts = {y: released_to_timestamp(y) for y in d["year"].unique()}
    d["timestamp"] = d["year"].apply(lambda x: ts[x])
    d.drop(columns="released", inplace=True)

    d = d[d["year"].astype(int) < 2011]

    d = remove_duplicates(d)
    d.to_csv(out_path, sep="\t", index=False)

    return d


def remove_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    # Remove duplicate versions of the same release -- just take the latest one with the most artists featured
    data["n_artists"] = data["artists"].apply(lambda x: len(x.split(", ")))
    sorted_data = data.sort_values(["master_id", "n_artists", "year"], ascending=[True, False, False])
    sorted_data.drop(columns="n_artists", inplace=True)

    return sorted_data[(sorted_data["master_id"].isna()) | (~sorted_data["master_id"].duplicated(keep="first"))]


def write_edge_list(data: pd.DataFrame, out_path: str = None) -> None:
    data = data[data["timestamp"] > -1]
    out = []

    for timestamp, artists in zip(data["timestamp"].values, data["artists"].values):
        if not isinstance(artists, str) and np.isnan(artists):
            continue

        artists_list = artists.split(", ")
        if len(artists_list) > 1:
            for collab in combinations(artists_list, 2):
                out.append({"artist_1": collab[0], "artist_2": collab[1], "timestamp": timestamp})

    pd.DataFrame(out).sort_values(by="timestamp").to_csv(out_path, sep="\t", index=False, header=False)


if __name__ == "__main__":
    df = pd.read_csv("releases_raw.tsv", sep="\t")
    clean_df = clean_data(df, "releases.tsv")
    write_edge_list(clean_df, "edges.tsv")
