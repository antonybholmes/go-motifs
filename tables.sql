PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS motifs;
CREATE TABLE motifs (
    id INTEGER PRIMARY KEY ASC, 
    motif TEXT NOT NULL, 
    sources TEXT NOT NULL,
    genes TEXT NOT NULL);
CREATE INDEX motifs_motif_idx ON motifs (motif);
