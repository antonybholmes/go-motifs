import collections
import json
import os
import re
import sqlite3
from nanoid import generate
import uuid_utils as uuid
import pandas as pd


files = [
    "JASPAR2022_CORE_redundant_v2.meme",
    "jolma2013.meme",
    "SwissRegulon_human_and_mouse.meme",
]

dir = "../../data/modules/motifs"

db = collections.defaultdict(lambda: collections.defaultdict(set))

data = []

with open("../meme/JASPAR2022_CORE_redundant_v2.meme", "r") as f:
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
                "dataset": "JASPAR2022_CORE_redundant_v2",
                "id": id,
                "name": name,
                "genes": list(sorted(genes)),
                "weights": [],
            }

        if line.startswith("letter-probability"):
            print("asdasdasd", row)
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


with open("../meme/SwissRegulon_human_and_mouse.meme", "r") as f:
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
                "dataset": "SwissRegulon_human_and_mouse",
                "id": id,
                "name": id,
                "genes": list(sorted(genes)),
                "weights": [],
            }

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


with open("../meme/jolma2013.meme", "r") as f:
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
                "dataset": "jolma2013",
                "id": id,
                "name": id,
                "genes": list(sorted(genes)),
                "weights": [],
            }

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


with open("../meme/H12CORE_meme_format.meme", "r") as f:
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
                "dataset": "H12CORE",
                "id": id,
                "name": id,
                "genes": list(sorted(genes)),
                "weights": [],
            }

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

with open("../meme/H13CORE_meme_format.meme", "r") as f:
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
                "dataset": "H13CORE",
                "id": id,
                "name": id,
                "genes": list(sorted(genes)),
                "weights": [],
            }

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


db = os.path.join(dir, "motifs.db")

if os.path.exists(db):
    os.remove(db)

conn = sqlite3.connect(db)
cursor = conn.cursor()


cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA foreign_keys = ON;")

cursor.execute("BEGIN TRANSACTION;")

cursor.execute("DROP TABLE IF EXISTS motifs;")
cursor.execute(
    """
    CREATE TABLE motifs (
        id TEXT PRIMARY KEY ASC,
        dataset TEXT NOT NULL,
        motif_id TEXT NOT NULL, 
        motif_name TEXT NOT NULL, 
        genes TEXT NOT NULL);
"""
)

cursor.execute("CREATE INDEX motifs_motif_id_idx ON motifs (motif_id);")
cursor.execute("CREATE INDEX motifs_motif_name_idx ON motifs (motif_name);")

cursor.execute("DROP TABLE IF EXISTS weights;")
cursor.execute(
    """
    CREATE TABLE weights (
        id TEXT PRIMARY KEY ASC,
        motif_id TEXT NOT NULL, 
        position INTEGER NOT NULL,
        a REAL NOT NULL,
        c REAL NOT NULL,
        g REAL NOT NULL,
        t REAL NOT NULL,
        FOREIGN KEY (motif_id) REFERENCES motifs(id) ON DELETE CASCADE);
"""
)


cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")
for row in data:
    id = str(uuid.uuid7())

    cursor.execute(
        "INSERT INTO motifs (id, dataset, motif_id, motif_name, genes) VALUES (?, ?, ?, ?, ?);",
        (
            id,
            row["dataset"],
            row["id"],
            row["name"],
            ",".join(row["genes"]),
        ),
    )

    for i, weight in enumerate(row["weights"]):
        cursor.execute(
            "INSERT INTO weights (id, motif_id, position, a, c, g, t) VALUES (?, ?, ?, ?, ?, ?, ?);",
            (
                str(uuid.uuid7()),
                id,
                i + 1,
                weight[0],
                weight[1],
                weight[2],
                weight[3],
            ),
        )

cursor.execute("COMMIT;")
conn.close()

print("Done.")
