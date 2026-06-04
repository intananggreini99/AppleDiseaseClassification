#!/usr/bin/env bash
# =====================================================================
# 02_upload_to_hdfs.sh
# Mengunggah dataset gambar (train/val/test) dari ./data ke HDFS.
#
# Struktur ./data yang diharapkan (hasil ekstrak dataset Kaggle):
#   data/Apple_Disease_Dataset/train/<kelas>/*.jpg
#   data/Apple_Disease_Dataset/val/<kelas>/*.jpg     (atau valid/validation)
#   data/Apple_Disease_Dataset/test/<kelas>/*.jpg
#
# Folder ./data sudah di-mount ke container namenode pada /source-data.
# Jika nama subfolder berbeda, ubah variabel DATASET_SUBDIR di bawah.
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

# Subfolder di dalam ./data yang berisi train/ val/ test/.
# Kosongkan ("") jika train/val/test berada langsung di ./data.
DATASET_SUBDIR="${DATASET_SUBDIR:-Apple_Disease_Dataset}"

echo ">>> Mengunggah dataset ke HDFS (sumber: ./data -> /source-data di namenode)"
echo ">>> DATASET_SUBDIR='${DATASET_SUBDIR}'"

docker exec -e DATASET_SUBDIR="${DATASET_SUBDIR}" namenode bash -c '
set -e
BASE="/source-data"
if [ -n "${DATASET_SUBDIR}" ]; then
  BASE="/source-data/${DATASET_SUBDIR}"
fi

if [ ! -d "${BASE}" ]; then
  echo "ERROR: Folder ${BASE} tidak ditemukan di dalam container."
  echo "Pastikan dataset sudah diekstrak ke ./data pada host."
  exit 1
fi

echo ">>> Isi folder sumber:"
ls -1 "${BASE}"

# Reset direktori tujuan di HDFS
hdfs dfs -rm -r -f /data/apple >/dev/null 2>&1 || true
hdfs dfs -mkdir -p /data/apple

for split in train val test; do
  SRC=""
  case "${split}" in
    train) for c in train Train TRAIN; do [ -d "${BASE}/${c}" ] && SRC="${BASE}/${c}" && break; done ;;
    val)   for c in val valid validation Val Valid Validation; do [ -d "${BASE}/${c}" ] && SRC="${BASE}/${c}" && break; done ;;
    test)  for c in test Test TEST testing; do [ -d "${BASE}/${c}" ] && SRC="${BASE}/${c}" && break; done ;;
  esac

  if [ -z "${SRC}" ]; then
    echo "PERINGATAN: split ${split} tidak ditemukan, dilewati."
    continue
  fi

  echo ">>> Unggah split ${split}  (dari ${SRC})"
  hdfs dfs -mkdir -p "/data/apple/${split}"
  # Salin ISI folder split (folder-folder kelas) ke /data/apple/<split>/
  hdfs dfs -put -f "${SRC}"/* "/data/apple/${split}/"
done

echo
echo ">>> Struktur akhir di HDFS:"
hdfs dfs -ls -R /data/apple | head -40
echo "..."
echo ">>> Jumlah file per split:"
for split in train val test; do
  n=$(hdfs dfs -ls -R "/data/apple/${split}" 2>/dev/null | grep -vE "^d" | wc -l || echo 0)
  echo "    ${split}: ${n} file"
done
'

echo
echo ">>> Selesai. Data gambar sudah tersedia di HDFS pada /data/apple/<split>/<kelas>/"
