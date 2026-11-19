#!/bin/bash
# JobCompass - Chargement des données de référence (offres et candidats) dans HDFS.
# Les interactions arrivent par Kafka ; les descriptions complètes des offres et
# des candidats sont des données de référence, copiées une fois dans HDFS.
#   Usage (depuis la racine du dépôt) : bash infra/load_reference.sh
set -e

load() {
  local src_dir=$1 name=$2
  local f
  f=$(find "data/$src_dir" -name "*.parquet" | head -1)
  if [ -z "$f" ]; then echo "Fichier introuvable dans data/$src_dir"; exit 1; fi
  echo ">>> $name : $f"
  docker cp "$f" namenode:/tmp/$name.parquet
  docker exec namenode hdfs dfs -mkdir -p /jobcompass/reference/$name
  docker exec namenode hdfs dfs -put -f /tmp/$name.parquet /jobcompass/reference/$name/
  docker exec namenode rm -f /tmp/$name.parquet
}

load recruitment-dataset-job-descriptions-english jobs
load recruitment-dataset-candidate-profiles-english candidates

docker exec namenode hdfs dfs -ls -R /jobcompass/reference
docker exec namenode hdfs dfs -du -s -h /jobcompass/reference/*
