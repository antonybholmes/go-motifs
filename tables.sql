PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS conversion;
CREATE TABLE conversion (
    id INTEGER PRIMARY KEY ASC, 
    human_gene_id TEXT NOT NULL, 
    mouse_gene_id TEXT NOT NULL,
    human_gene_symbol TEXT NOT NULL, 
    mouse_gene_symbol TEXT NOT NULL,
    human_entrez INTEGER NOT NULL DEFAULT -1, 
    mouse_entrez INTEGER NOT NULL DEFAULT -1,
    UNIQUE(human_gene_id, mouse_gene_id));
CREATE INDEX conversion_human_gene_id_idx ON conversion (human_gene_id);
CREATE INDEX conversion_mouse_gene_id_idx ON conversion (mouse_gene_id);

DROP TABLE IF EXISTS human_terms;
CREATE TABLE human_terms (
    id INTEGER PRIMARY KEY ASC, 
    gene_id TEXT NOT NULL,
    term TEXT NOT NULL);
CREATE INDEX human_terms_term_idx ON human_terms (term);
 
DROP TABLE IF EXISTS human;
CREATE TABLE human (
    id INTEGER PRIMARY KEY ASC, 
    gene_id TEXT NOT NULL,
    gene_symbol TEXT NOT NULL, 
    aliases TEXT NOT NULL,
    entrez INTEGER NOT NULL DEFAULT -1, 
    refseq TEXT NOT NULL,
    ensembl TEXT NOT NULL,
    UNIQUE(gene_id));
CREATE INDEX human_gene_id_idx ON human (gene_id);
CREATE INDEX human_gene_symbol_idx ON human (gene_symbol);
CREATE INDEX human_entrez_idx ON human (entrez);
CREATE INDEX human_refseq_idx ON human (refseq); 
CREATE INDEX human_ensembl_idx ON human (ensembl); 

DROP TABLE IF EXISTS mouse_terms;
CREATE TABLE mouse_terms (
    id INTEGER PRIMARY KEY ASC, 
    gene_id TEXT NOT NULL,
    term TEXT NOT NULL);
CREATE INDEX mouse_terms_term_idx ON mouse_terms (term);
 

DROP TABLE IF EXISTS mouse;
CREATE TABLE mouse (
    id INTEGER PRIMARY KEY ASC, 
    gene_id TEXT NOT NULL,
    gene_symbol TEXT NOT NULL, 
    aliases TEXT NOT NULL,
    entrez INTEGER NOT NULL DEFAULT -1, 
    refseq TEXT NOT NULL,
    ensembl TEXT NOT NULL,
    UNIQUE(gene_id));
CREATE INDEX mouse_gene_id_idx ON mouse (gene_id);
CREATE INDEX mouse_gene_symbol_idx ON mouse (gene_symbol);
CREATE INDEX mouse_entrez_idx ON mouse (entrez);
CREATE INDEX mouse_refseq_idx ON mouse (refseq); 
CREATE INDEX mouse_ensembl_idx ON mouse (ensembl); 