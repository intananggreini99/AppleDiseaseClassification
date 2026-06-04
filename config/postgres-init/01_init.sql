-- =====================================================================
-- Skrip inisialisasi PostgreSQL (database aplikasi: appledb)
-- Dijalankan otomatis oleh image postgres saat container pertama kali
-- dibuat (folder /docker-entrypoint-initdb.d).
--
-- Catatan: tabel fitur (apple.features) & hasil benchmark
-- (apple.benchmark_results) dibuat otomatis oleh pipeline Spark/Python
-- memakai mode overwrite. Di sini kita hanya menyiapkan schema dan
-- sebuah view bantu.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS apple;

-- Memberi hak akses penuh pada user aplikasi
GRANT ALL PRIVILEGES ON SCHEMA apple TO appleuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA apple
    GRANT ALL PRIVILEGES ON TABLES TO appleuser;

-- Tabel log eksekusi pipeline (opsional, diisi oleh pipeline)
CREATE TABLE IF NOT EXISTS apple.pipeline_log (
    id          SERIAL PRIMARY KEY,
    stage       VARCHAR(64)  NOT NULL,
    message     TEXT,
    duration_s  DOUBLE PRECISION,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON SCHEMA apple IS 'Schema utama untuk Apple Leaf Disease Classification';
COMMENT ON TABLE apple.pipeline_log IS 'Catatan durasi setiap tahap pipeline';
