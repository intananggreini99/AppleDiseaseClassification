"""
step2_postgres_to_hive.py
=========================
TAHAP 2 PIPELINE - PostgreSQL -> HDFS (Parquet) -> Hive.

Alur:
  1. Membaca data hasil preprocessing dari PostgreSQL (apple.features)
     melalui JDBC.
  2. Menulisnya ke HDFS dalam format kolumnar Parquet (terkompresi snappy).
  3. Mendaftarkannya sebagai tabel terkelola Hive (apple_db.features)
     sehingga dapat di-query lewat HiveServer2 / Spark SQL.
  4. Verifikasi: menjalankan beberapa query Hive sederhana.

Dijalankan via spark-submit di dalam container spark-master.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import utils


def main():
    spark = utils.build_spark("02-Postgres-to-Hive")

    # ---------------------------------------------------------------
    # 1. Baca dari PostgreSQL
    # ---------------------------------------------------------------
    utils.print_header(f"Membaca {config.TBL_FEATURES} dari PostgreSQL")
    with utils.Timer("Baca PostgreSQL") as t_r:
        df = spark.read.jdbc(
            config.PG_JDBC_URL,
            config.TBL_FEATURES,
            properties=config.PG_JDBC_PROPERTIES,
        )
    df.cache()
    n = df.count()
    print(f"  -> {n} baris terbaca, {len(df.columns)} kolom")
    df.printSchema()
    utils.log_stage("read_postgres", f"{n} baris", t_r.elapsed)

    # ---------------------------------------------------------------
    # 2. Tulis ke HDFS sebagai Parquet
    # ---------------------------------------------------------------
    utils.print_header(f"Menulis Parquet ke {config.HDFS_PARQUET_PATH}")
    with utils.Timer("Tulis Parquet") as t_p:
        (df.write
           .mode("overwrite")
           .option("compression", "snappy")
           .parquet(config.HDFS_PARQUET_PATH))
    utils.log_stage("write_parquet", config.HDFS_PARQUET_PATH, t_p.elapsed)

    # ---------------------------------------------------------------
    # 3. Daftarkan sebagai tabel Hive
    # ---------------------------------------------------------------
    utils.print_header(f"Mendaftarkan tabel Hive {config.HIVE_TABLE_FEATURES}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.HIVE_DATABASE}")
    spark.sql(f"DROP TABLE IF EXISTS {config.HIVE_TABLE_FEATURES}")

    with utils.Timer("saveAsTable (Hive)") as t_h:
        (df.write
           .mode("overwrite")
           .format("parquet")
           .saveAsTable(config.HIVE_TABLE_FEATURES))
    utils.log_stage("register_hive", config.HIVE_TABLE_FEATURES, t_h.elapsed)

    # ---------------------------------------------------------------
    # 4. Verifikasi query Hive
    # ---------------------------------------------------------------
    utils.print_header("Verifikasi tabel Hive")
    print("Daftar database:")
    spark.sql("SHOW DATABASES").show(truncate=False)

    print(f"Skema tabel {config.HIVE_TABLE_FEATURES}:")
    spark.sql(f"DESCRIBE {config.HIVE_TABLE_FEATURES}").show(truncate=False)

    print("Jumlah baris per split (via Hive SQL):")
    spark.sql(
        f"SELECT split, COUNT(*) AS jumlah "
        f"FROM {config.HIVE_TABLE_FEATURES} GROUP BY split ORDER BY split"
    ).show(truncate=False)

    print("Contoh 5 baris:")
    spark.sql(
        f"SELECT * FROM {config.HIVE_TABLE_FEATURES} LIMIT 5"
    ).show()

    print(f"\nSelesai. Data tersimpan sebagai Parquet di HDFS dan terdaftar "
          f"sebagai tabel Hive '{config.HIVE_TABLE_FEATURES}'.")
    spark.stop()


if __name__ == "__main__":
    main()
