rm ../../data/modules/motifs/motifs.db

 

cat tables.sql | sqlite3 ../../data/modules/motifs/motifs.db
cat ../../data/modules/motifs/motifs.sql | sqlite3 ../../data/modules/motifs/motifs.db
