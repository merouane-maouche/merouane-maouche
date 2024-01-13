#!/bin/bash

# Script pour lancer le script Python de copie de données entre Hadoop et PostgreSQL

# Chemin vers le répertoire commun
COMMON_PATH="/srv/hadoop/tools/sid/commun"
HDFS_DIR="/user/u02e224/tmp_file"
LOCAL_DIR="/srv/hadoop/tools/sid/restitution/restitution-dsn/data_out"


# Utilisateurs et groupes
U_HDFS="hdfs"
U_SPARK="spark"
Q_SPARK="spark"
APPLI="SID"

# Date et heure de début
DEBUT_LANCEMENT=$(date "+%Y%m%d%H%M%S")

# Chargement des paramètres de connexion depuis le fichier .credential
source "$COMMON_PATH/param/.credential"

# Initialisation Kerberos
"$HOME/krbini.sh"

# Fichier de log
FIC_LOG="/srv/logs/hadoop/sid/exec_SQL_${TableName}_${APPLI}_${DEBUT_LANCEMENT}"

# En-tête et instructions d'utilisation
echo -e "\n#------------------------------------------------------------------------------------------------------------------------------------------------------#"
echo "* 1 ARGUMENTS OBLIGATOIRES:                                                                                                                            *"
echo "* - Parametre n°1 : DirecrtoryToScriptsSQL                                                                                                             *"
echo "*                                                                                                                                                      *"
echo "* USAGE: ./copie_data_exec_SQL_MMA.sh <DirecrtoryToScriptsSQL>                                                                                         *"
echo "* Exemple_1 : ./copie_data_exec_SQL_MMA.sh /srv/hadoop/tools/sid/restitution/TestPythonCopieHDPPSQL/TestPythonHdpToPsql                                *"
echo "* Exemple_2 : ./copie_data_exec_SQL_MMA.sh /srv/hadoop/tools/sid/restitution/TestPythonCopieHDPPSQL/TestPythonPsqlToHdp                                *"
echo "* Exemple_2 : ./copie_data_exec_SQL_MMA.sh /srv/hadoop/tools/sid/restitution/TestPythonCopieHDPPSQL/tabelToExport                                      *"
echo "#------------------------------------------------------------------------------------------------------------------------------------------------------#"

# Affichage du menu pour l'utilisateur
echo -e "\nVeuillez choisir une ACTION :"
echo "1. ACTION 1 - HP"
echo "2. ACTION 2 - PH"
echo "3. ACTION 3 - EX"
read -p "choix (HP/PH/EX): " choix

case $choix in
    [Hh][Pp])
        ACTION="HP"
        ;;
    [Pp][Hh])
        ACTION="PH"
        ;;
    [Ee][Xx])
        ACTION="EX"
        ;;
    *)
        echo "choix invalide."
        exit 1
        ;;
esac

# Affichage de l'heure de début du traitement
echo -e "\nDébut du traitement le $(date '+%Y-%m-%d') à $(date '+%H:%M:%S')\n" | tee -a "$FIC_LOG.log"

if [ "$ACTION" = "EX" ]; then
    spark-submit --jars $COMMON_PATH/shell/postgresql-42.6.0.jar --master yarn --queue $Q_SPARK $COMMON_PATH/python/copie_hadoop_postgresql_VF.py $url $port $db_name $user $password $ACTION $1 1>>$FIC_LOG.log 2>>$FIC_LOG.dtl
    hdfs dfs -rm "$HDFS_DIR/_SUCCESS"
    hdfs dfs -rm "$HDFS_DIR/part*"
    hdfs dfs -rm -r "$HDFS_DIR"/*.csv

    # Parcourir les sous-répertoires de HDFS
    hdfs dfs -ls "$HDFS_DIR" | while read -r line; do
        SUB_DIR=$(echo "$line" | awk '{print $NF}' | awk -F '/' '{print $NF}')

        # Créer un sous-répertoire correspondant dans LOCAL_DIR
        mkdir -p "$LOCAL_DIR/$SUB_DIR"

        # Copier les fichiers "part-" vers LOCAL_DIR
        hdfs dfs -copyToLocal "$HDFS_DIR/$SUB_DIR"/part-* "$LOCAL_DIR/$SUB_DIR/"

        # Concaténer les fichiers "part-" en un seul fichier CSV
        header_copied=false
        for file in "$LOCAL_DIR/$SUB_DIR"/part-*; do
            if [ "$header_copied" = false ]; then
                cp "$file" "$LOCAL_DIR/$SUB_DIR/$SUB_DIR.csv"
                header_copied=true
            else
                tail -n +2 "$file" >> "$LOCAL_DIR/$SUB_DIR/$SUB_DIR.csv"
            fi
            rm "$file"
        done
        echo "Fichiers concaténés depuis $SUB_DIR vers $LOCAL_DIR/$SUB_DIR/$SUB_DIR.csv"
    done

    # Supprimer le contenu du dossier "tmp_file" dans HDFS
    hdfs dfs -rm -r -skipTrash "$HDFS_DIR/*"
rm -rf /srv/hadoop/tools/sid/restitution/restitution-dsn/data_out/items
else
    spark-submit --jars $COMMON_PATH/shell/postgresql-42.6.0.jar --master yarn --queue $Q_SPARK $COMMON_PATH/python/copie_hadoop_postgresql_VF.py $url $port $db_name $user $password $ACTION $1 1>>$FIC_LOG.log 2>>$FIC_LOG.dtl
fi

if [ "$(tail -1 "$FIC_LOG.log" | awk '{print $1}')" != "______________________________________FIN______________________________________" ]; then
    echo "Verifier la log"
    echo -e "\nfin de traitement le $(date '+%Y-%m-%d') à $(date '+%H:%M:%S') ${NOCOLOR}"  | tee -a "$FIC_LOG.log"
else
    echo -e "\nfin de traitement le $(date '+%Y-%m-%d') à $(date '+%H:%M:%S') ${NOCOLOR}"  | tee -a "$FIC_LOG.log"
    rm -f "$FIC_LOG.dtl"
fi

# Calcul de la durée du traitement
FIN_LANCEMENT=$(date "+%Y%m%d%H%M%S")
echo -e "\nLe traitement a duré : $((FIN_LANCEMENT - DEBUT_LANCEMENT)) secondes\n" | tee -a "$FIC_LOG.log"

echo "Terminé."