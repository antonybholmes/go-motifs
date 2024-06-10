import collections
import json
import re
import pandas as pd


files = [
    "JASPAR2022_CORE_redundant_v2.meme",
    "jolma2013.meme",
    "SwissRegulon_human_and_mouse.meme",
]

db = collections.defaultdict(lambda: collections.defaultdict(set))

with open("JASPAR2022_CORE_redundant_v2.meme", "r") as f:
    for line in f:
        line = line.strip()

        if line.startswith("MOTIF"):
            tokens = line.split(" ")
            id = tokens[1]
            name = tokens[2]
            genes = [gene.split("_")[0] for gene in name.split("::")]

            db[id]["JASPAR2022_CORE_redundant_v2"].update(genes)
            db[name]["JASPAR2022_CORE_redundant_v2"].update(genes)


with open("SwissRegulon_human_and_mouse.meme", "r") as f:
    for line in f:
        line = line.strip()

        if line.startswith("MOTIF"):
            tokens = line.split(" ")
            id = tokens[1]
            genes = set()

            for gene in id.split(".p")[0].split("_"):
                # replace range in gene

                if gene == "ADNP_IRX_SIX_ZHX":
                    print("aha", gene)
                    exit(0)

                matcher = re.search(r"(\d+)\.\.(\d+)", gene)

                if matcher:
                    start = int(matcher.group(1))
                    end = int(matcher.group(2))

                    r = ",".join([str(s) for s in range(start, end + 1)])

                    gene = re.sub(r"(\d+)\.\.(\d+)", r, gene)

                matcher = re.search(r"^(.+)\{(.+)\}", gene)

                if matcher:
                    prefix = matcher.group(1)
                    suffixes = [s.strip() for s in matcher.group(2).split(",")]
                    for s in suffixes:
                        motif = f"{prefix}{s}"
                        genes.add(motif)
                        # print(g)
                else:
                    matcher = re.search(r"^(.+?)(.,.+)", gene)

                    if matcher:

                        prefix = matcher.group(1)
                        suffixes = [s.strip() for s in matcher.group(2).split(",")]
                        for s in suffixes:
                            motif = f"{prefix}{s}"
                            genes.add(motif)
                            print(
                                "fff", gene, motif, matcher.group(1), matcher.group(2)
                            )
                    else:
                        db[id]["SwissRegulon_human_and_mouse"].add(gene)


with open("jolma2013.meme", "r") as f:
    for line in f:
        line = line.strip()

        if line.startswith("MOTIF"):
            tokens = line.split(" ")
            id = tokens[1]
            genes = set()

            for gene in re.sub(r"_DBD.*", "", id.split("_full")[0]).split("_"):
                # replace range in gene

                db[id]["jolma2013"].add(gene)


df = pd.read_csv("hocomoco/H12CORE_annotation_v12.tsv", sep="\t", header=0)

for motif, gene in zip(df["tf"].values, df["gene_symbol"].values):
    db[motif]["H12CORE"].add(gene)


data = []
jdata = collections.defaultdict(lambda: collections.defaultdict(list))

for motif in sorted(db):
    dbs = "|".join(sorted(db[motif]))
    genes = set()
    for d in sorted(db[motif]):
        genes.update(db[motif][d])

    data.append([motif, dbs, "|".join(sorted(genes))])

    jdata[motif]["db"] = list(sorted(db[motif]))
    jdata[motif]["genes"] = list(sorted(sorted(genes)))

df_out = pd.DataFrame(data, columns=["motif", "database", "gene_symbol"])
df_out.to_csv("motif_to_gene.tsv", sep="\t", header=True, index=False)

with open("motif_to_gene.json", "w") as f:
    json.dump(jdata, f, indent=2)


with open("motiftogene.sql", "w") as f:
    for motif in sorted(jdata):
        print(f"INSERT INTO motifs (motif, sources, genes) VALUES ('{motif}', '{'|'.join(jdata[motif]['db'])}','{'|'.join(jdata[motif]['genes'])}');",file=f)