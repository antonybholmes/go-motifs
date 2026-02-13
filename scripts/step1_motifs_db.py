import collections
import json
import os
import re
import sqlite3
from datetime import datetime

import pandas as pd
import uuid_utils as uuid
from nanoid import generate

files = [
    "JASPAR2022_CORE_redundant_v2.meme",
    "jolma2013.meme",
    "SwissRegulon_human_and_mouse.meme",
]

dir = "../../data/modules/motifs"

db = collections.defaultdict(lambda: collections.defaultdict(set))

data = []

datasets = {}

dataset_index = 1
sample_index = 1

with open("meme/JASPAR2022_CORE_redundant_v2.meme", "r") as f:
    datasets["JASPAR2022_CORE_redundant_v2"] = {
        "id": str(uuid.uuid7()),
        "index": len(datasets) + 1,
    }
    for line in f:
        line = line.strip()

        print(line)

        if line.startswith("MOTIF"):
            tokens = line.split(" ")
            id = tokens[1]
            name = tokens[2]
            genes = set([gene.split("_")[0] for gene in name.split("::")])

            db[id]["JASPAR2022_CORE_redundant_v2"].update(genes)
            db[name]["JASPAR2022_CORE_redundant_v2"].update(genes)

            row = {
                "index": sample_index,
                "dataset": "JASPAR2022_CORE_redundant_v2",
                "id": id,
                "name": name,
                "genes": list(sorted(genes)),
                "weights": [],
            }

            sample_index += 1

        if line.startswith("letter-probability"):

            matcher = re.search(r"w= (\d+)", line)

            weights = []
            if matcher:
                w = int(matcher.group(1))

                print(w)

                for i in range(0, w):
                    l = next(f).strip()
                    print(l)
                    l = re.sub(" +", " ", l)
                    pvalues = [float(x.strip()) for x in l.split(" ")]

                    weights.append(pvalues)

                row["weights"] = weights

                print(json.dumps(weights))

                data.append(row)


with open("meme/SwissRegulon_human_and_mouse.meme", "r") as f:
    datasets["SwissRegulon_human_and_mouse"] = {
        "id": str(uuid.uuid7()),
        "index": len(datasets) + 1,
    }

    for line in f:
        line = line.strip()

        if line.startswith("MOTIF"):
            tokens = line.split(" ")
            id = tokens[1]
            genes = set()

            for gene in id.split(".p")[0].split("_"):
                # replace range in gene e.g. E2F1..5 becomes E2F1, E2F2, etc.

                not_found = True

                matcher = re.search(r"(.+)(\d+)\.\.(\d+)", gene)

                if not_found and matcher:
                    symbol = matcher.group(1)
                    start = int(matcher.group(2))
                    end = int(matcher.group(3))

                    r = ",".join([f"{symbol}{s}" for s in range(start, end + 1)])

                    print(gene)
                    gene = r  # re.sub(r"(\d+)\.\.(\d+)", r, gene)
                    print(gene)
                    not_found = False

                if not_found and "{" not in gene:
                    matcher = re.search(r"^(.+?)(\d+(?:,\d+)+)", gene)

                    if matcher:
                        prefix = matcher.group(1)
                        suffixes = [s.strip() for s in matcher.group(2).split(",")]

                        for s in suffixes:
                            g = f"{prefix}{s}"
                            genes.add(g)

                matcher = re.search(r"^(.+)\{(.+)\}", gene)

                if not_found and matcher:
                    prefix = matcher.group(1)
                    suffixes = [s.strip() for s in matcher.group(2).split(",")]

                    print(gene, suffixes)

                    for s in suffixes:
                        g = f"{prefix}{s}"
                        genes.add(g)

                if not_found:
                    # use gene as is
                    genes.add(gene)

            row = {
                "index": sample_index,
                "dataset": "SwissRegulon_human_and_mouse",
                "id": id,
                "name": id,
                "genes": list(sorted(genes)),
                "weights": [],
            }

            sample_index += 1

        if line.startswith("letter-probability"):
            matcher = re.search(r"w= (\d+)", line)

            weights = []
            if matcher:
                w = int(matcher.group(1))

                print(w)

                for i in range(0, w):
                    l = next(f).strip()
                    print(l)
                    l = re.sub(" +", " ", l)
                    pvalues = [float(x.strip()) for x in l.split(" ")]
                    print(pvalues)
                    weights.append(pvalues)

            print(json.dumps(weights))

            row["weights"] = weights

            data.append(row)


with open("meme/jolma2013.meme", "r") as f:
    datasets["jolma2013"] = {
        "id": str(uuid.uuid7()),
        "index": len(datasets) + 1,
    }

    for line in f:
        line = line.strip()

        if line.startswith("MOTIF"):
            tokens = line.split(" ")
            id = tokens[1]
            genes = set()

            for gene in re.sub(r"_DBD.*", "", id.split("_full")[0]).split("_"):
                # replace range in gene
                db[id]["jolma2013"].add(gene)
                genes.add(gene)

            row = {
                "index": sample_index,
                "dataset": "jolma2013",
                "id": id,
                "name": id,
                "genes": list(sorted(genes)),
                "weights": [],
            }

            sample_index += 1

        if line.startswith("letter-probability"):
            matcher = re.search(r"w= (\d+)", line)

            weights = []
            if matcher:
                w = int(matcher.group(1))

                print(w)

                for i in range(0, w):
                    l = next(f).strip()
                    print(l)
                    l = re.sub(" +", " ", l)
                    pvalues = [float(x.strip()) for x in l.split(" ")]
                    print(pvalues)
                    weights.append(pvalues)

            print(json.dumps(weights))

            row["weights"] = weights

            data.append(row)


with open("meme/H12CORE_meme_format.meme", "r") as f:
    datasets["H12CORE"] = {
        "id": str(uuid.uuid7()),
        "index": len(datasets) + 1,
    }

    for line in f:
        line = line.strip()
        print(line)

        if line.startswith("MOTIF"):
            tokens = line.split(" ")
            id = tokens[1]
            genes = set()

            for gene in re.sub(r"\.H12CORE.*", "", id.split("_full")[0]).split("_"):
                # replace range in gene
                db[id]["H12CORE"].add(gene)
                genes.add(gene)

            row = {
                "index": sample_index,
                "dataset": "H12CORE",
                "id": id,
                "name": id,
                "genes": list(sorted(genes)),
                "weights": [],
            }

            sample_index += 1

        if line.startswith("letter-probability"):
            matcher = re.search(r"w= (\d+)", line)

            weights = []
            if matcher:
                w = int(matcher.group(1))

                print(w)

                for i in range(0, w):
                    l = next(f).strip()
                    print(l)
                    l = re.sub("[ \t]+", " ", l)
                    pvalues = [float(x.strip()) for x in l.split(" ")]
                    print(pvalues)
                    weights.append(pvalues)

            print(json.dumps(weights))

            row["weights"] = weights

            data.append(row)

with open("meme/H13CORE_meme_format.meme", "r") as f:
    datasets["H13CORE"] = {
        "id": str(uuid.uuid7()),
        "index": len(datasets) + 1,
    }

    for line in f:
        line = line.strip()
        print(line)

        if line.startswith("MOTIF"):
            tokens = line.split(" ")
            id = tokens[1]
            genes = set()

            for gene in re.sub(r"\.H13CORE.*", "", id.split("_full")[0]).split("_"):
                # replace range in gene
                db[id]["H13CORE"].add(gene)
                genes.add(gene)

            row = {
                "index": sample_index,
                "dataset": "H13CORE",
                "id": id,
                "name": id,
                "genes": list(sorted(genes)),
                "weights": [],
            }

            sample_index += 1

        if line.startswith("letter-probability"):
            matcher = re.search(r"w= (\d+)", line)

            weights = []
            if matcher:
                w = int(matcher.group(1))

                print(w)

                for i in range(0, w):
                    l = next(f).strip()
                    print(l)
                    l = re.sub("[ \t]+", " ", l)
                    pvalues = [float(x.strip()) for x in l.split(" ")]
                    print(pvalues)
                    weights.append(pvalues)

            print(json.dumps(weights))

            row["weights"] = weights

            data.append(row)

# data = []
# jdata = collections.defaultdict(lambda: collections.defaultdict(list))

# for motif in sorted(db):
#     dbs = "|".join(sorted(db[motif]))
#     genes = set()
#     for d in sorted(db[motif]):
#         genes.update(db[motif][d])

#     data.append([motif, dbs, "|".join(sorted(genes))])

#     jdata[motif]["db"] = list(sorted(db[motif]))
#     jdata[motif]["genes"] = list(sorted(sorted(genes)))

# df_out = pd.DataFrame(data, columns=["motif", "database", "gene_symbol"])
# df_out.to_csv("motif_to_gene.tsv", sep="\t", header=True, index=False)

# with open("motif_to_gene.json", "w") as f:
#    json.dump(jdata, f, indent=2)

# get date as y-mm-dd
date = datetime.now().strftime("%Y-%m-%d")

db = os.path.join(dir, f"motifs-{date}.db")

if os.path.exists(db):
    os.remove(db)

conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()


cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA foreign_keys = ON;")

cursor.execute("BEGIN TRANSACTION;")

cursor.execute("DROP TABLE IF EXISTS datasets;")
cursor.execute(
    """
    CREATE TABLE datasets (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL,
        name TEXT NOT NULL);
"""
)

cursor.execute("CREATE INDEX idx_datasets_name ON datasets (LOWER(name));")


cursor.execute("DROP TABLE IF EXISTS datasets_fts;")
cursor.execute(
    """
    CREATE VIRTUAL TABLE datasets_fts USING fts5(
        name,
        content='datasets',
        content_rowid='rowid'
    );
"""
)

cursor.execute("DROP TABLE IF EXISTS motifs;")
cursor.execute(
    """
    CREATE TABLE motifs (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL,
        dataset_id INTEGER NOT NULL,
        motif_id TEXT NOT NULL, 
        motif_name TEXT NOT NULL, 
        genes TEXT NOT NULL,
        length INTEGER NOT NULL,
        UNIQUE (dataset_id, motif_id),
        FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE);
"""
)

cursor.execute("CREATE INDEX idx_motifs_motif_id ON motifs (LOWER(motif_id));")
cursor.execute("CREATE INDEX idx_motifs_name ON motifs (LOWER(motif_name));")
cursor.execute("CREATE INDEX idx_motifs_dataset_id ON motifs (dataset_id);")


cursor.execute("DROP TABLE IF EXISTS motifs_fts;")
cursor.execute(
    """
    CREATE VIRTUAL TABLE motifs_fts USING fts5(
        motif_id,
        motif_name,
        content='motifs',
        content_rowid='rowid'
    );
"""
)

cursor.execute("DROP TABLE IF EXISTS weights;")
cursor.execute(
    """
    CREATE TABLE weights (
        id INTEGER PRIMARY KEY,
        motif_id INTEGER NOT NULL,
        position INTEGER NOT NULL,
        a REAL NOT NULL,
        c REAL NOT NULL,
        g REAL NOT NULL,
        t REAL NOT NULL,
        FOREIGN KEY (motif_id) REFERENCES motifs(id) ON DELETE CASCADE);
"""
)
cursor.execute("CREATE INDEX idx_weights_motif_id ON weights (motif_id);")

for name in sorted(datasets):

    cursor.execute(
        "INSERT INTO datasets (id, public_id, name) VALUES (?, ?, ?);",
        (
            datasets[name]["index"],
            datasets[name]["id"],
            name,
        ),
    )


for row in data:

    cursor.execute(
        "INSERT INTO motifs (id, public_id, dataset_id, motif_id, motif_name, genes, length) VALUES (?, ?, ?, ?, ?, ?, ?);",
        (
            row["index"],
            str(uuid.uuid7()),
            datasets[row["dataset"]]["index"],
            row["id"],
            row["name"],
            "|".join(row["genes"]),
            len(row["weights"]),
        ),
    )

    for i, weight in enumerate(row["weights"]):
        cursor.execute(
            "INSERT INTO weights (motif_id, position, a, c, g, t) VALUES (?, ?, ?, ?, ?, ?);",
            (
                row["index"],
                i + 1,
                weight[0],
                weight[1],
                weight[2],
                weight[3],
            ),
        )


cursor.execute("INSERT INTO datasets_fts(datasets_fts) VALUES ('rebuild');")
cursor.execute("INSERT INTO motifs_fts(motifs_fts) VALUES ('rebuild');")

cursor.execute("COMMIT;")

conn.close()

print("Done.")
