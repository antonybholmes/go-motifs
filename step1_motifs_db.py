import collections
import json
import os
import re
from nanoid import generate
import pandas as pd


files = [
    "JASPAR2022_CORE_redundant_v2.meme",
    "jolma2013.meme",
    "SwissRegulon_human_and_mouse.meme",
]

dir = "../data/modules/motifs"

db = collections.defaultdict(lambda: collections.defaultdict(set))

data = []

with open("meme/JASPAR2022_CORE_redundant_v2.meme", "r") as f:
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
            
            row = ["JASPAR2022_CORE_redundant_v2", id, name, list(sorted(genes)), 0, []]

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

                row[-2] = w
                row[-1] = weights

                print(json.dumps(weights))

                data.append(row)


with open("meme/SwissRegulon_human_and_mouse.meme", "r") as f:
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

            row = ["SwissRegulon_human_and_mouse", id, id, list(sorted(genes)), 0, []]

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

            row[-2] = w
            row[-1] = weights

            data.append(row)


with open("meme/jolma2013.meme", "r") as f:
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

            row = ["jolma2013", id, id, list(sorted(genes)), 0, []]

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

            row[-2] = w
            row[-1] = weights

            data.append(row)


with open("meme/H12CORE_meme_format.meme", "r") as f:
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

            row = ["H12CORE", id, id, list(sorted(genes)), 0, []]

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

            row[-2] = w
            row[-1] = weights

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


with open(os.path.join(dir, "motifs.sql"), "w") as f:
    for row in data:
        publicId = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
        print(
            f"INSERT INTO motifs (public_id, dataset, motif_id, motif_name, genes, size, weights) VALUES ('{publicId}', '{row[0]}','{row[1]}', '{row[2]}', '{','.join(row[3])}', {row[4]}, '{json.dumps(row[5])}');",
            file=f,
        )
