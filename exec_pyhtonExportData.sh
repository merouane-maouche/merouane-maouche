#!/bin/bash

start_time=$(date +"%Y-%m-%d %H:%M:%S")
echo "Début du traitement : $start_time"

python exportDataFromHdpToLocalOnCsv.py /chemin/vers/repertoire/sql

end_time=$(date +"%Y-%m-%d %H:%M:%S")
echo "Fin du traitement : $end_time"

start_seconds=$(date -d "$start_time" +%s)
end_seconds=$(date -d "$end_time" +%s)
processing_time=$((end_seconds - start_seconds))

echo "Temps total de traitement : $processing_time secondes"

sql_files=$(ls /chemin/vers/repertoire/sql/*.sql)
for sql_file in $sql_files; do
    query_name=$(basename "$sql_file" .sql)
    query_start_time=$(date +"%Y-%m-%d %H:%M:%S")

    python exportDataFromHdpToLocalOnCsv.py "$sql_file"

    query_end_time=$(date +"%Y-%m-%d %H:%M:%S")
    query_start_seconds=$(date -d "$query_start_time" +%s)
    query_end_seconds=$(date -d "$query_end_time" +%s)
    query_processing_time=$((query_end_seconds - query_start_seconds))

    echo "Temps de traitement de la requête $query_name : $query_processing_time secondes"
done
