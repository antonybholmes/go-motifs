rm data/modules/motiftogene/motiftogene.db

 

cat tables.sql | sqlite3 data/modules/motiftogene/motiftogene.db
cat motiftogene.sql | sqlite3 data/modules/motiftogene/motiftogene.db
