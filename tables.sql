PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS motifs;
CREATE TABLE motifs (
    id INTEGER PRIMARY KEY ASC,
    public_id TEXT NOT NULL,
    dataset TEXT NOT NULL,
    motif_id TEXT NOT NULL, 
    motif_name TEXT NOT NULL, 
    genes TEXT NOT NULL,
    size INTEGER NOT NULL,
    weights TEXT NOT NULL);
CREATE INDEX motifs_motif_id_idx ON motifs (motif_id);
CREATE INDEX motifs_motif_name_idx ON motifs (motif_name);
