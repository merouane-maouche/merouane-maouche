#!/usr/bin/env python
import time
from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import os
import psycopg2
import subprocess

def create_spark_context(app_name):
    conf = SparkConf().setMaster('yarn').setAppName(app_name)
    sc = SparkContext(conf=conf)
    return SQLContext(sc)

def copy_hadoop_to_postgres(url, port, db_name, user, password, directory_to_scripts_sql):
    spark = create_spark_context('ChargementHadoopToPostgresql')
   
    num_req = 1
    num_enr_total = 0
   
    print("_" * 55 + "DEBUT GLOBAL" + "_" * 55)
    print("Le : {} a : {}".format(str(datetime.now()).split(' ')[0], str(datetime.now()).split(' ')[1].replace('.', ':')[:-3]))
   
    listeDesFichiers = os.listdir(directory_to_scripts_sql)
    print("\nLe nombre de fichier(s) dans le dossier : [{}] est : {}\n".format(directory_to_scripts_sql.split('/')[-1], len(listeDesFichiers)))
   
    for nom_fichier in listeDesFichiers:
        chemin_fichier = os.path.join(directory_to_scripts_sql, nom_fichier)
        with open(chemin_fichier, 'r') as fichier:
            contenu = fichier.read()
        requetes = contenu.split(';')
       
        print("Le nombre de requetes dans le fichier : [{}] est : {}".format(nom_fichier, len(requetes) - 1))
        print("\nLe(s) requete(s) sont :")
        for requete_index in range(len(requetes) - 1):
            print("\nLa requete numero {} est : {}".format(requete_index + 1, requetes[requete_index]))
        print("")
       
        start_time_global = time.time()
        for requete in requetes:
            requete = requete.strip()
            if requete:
                print("_" * 60 + "DEBUT" + "_" * 60 + "\n")
                start_time_local = time.time()
                end_time_local = time.time()
                elapsed_time_local = end_time_local - start_time_local
                print('')
                print("Requete [{}] executee : {}".format(num_req, requete))
                num_req += 1
                print('')
                print("=" * 126)
               
                start_time_local_psql = time.time()
                if "sid_integ01." in requete:
                    conn = psycopg2.connect(host=url,
                                            port=port,
                                            dbname=db_name,
                                            user=user,
                                            password=password)
                    conn.cursor().execute(requete)
                    conn.cursor().close()
                    conn.close()
                else:
                    print("/\\" * 30 + "DEBUT" + "/\\" * 30)
                    print("La requete dont le contenu sera copie est : {}\nEt la table en question est : {}".format(requete, requete.split('.')[1]))
                    print("")
                   
                    TableName = requete.split('.')[1]
                    print("Ecriture dans le PSQL...\n")
                   
                    spark.sql(requete).write\
                        .format("jdbc").option("url", "jdbc:postgresql://{}:{}/{}".format(url, port, db_name))\
                        .option("user", user)\
                        .option("password", password)\
                        .option("dbtable", "sid_integ01.{}".format(TableName))\
                        .option("driver", "org.postgresql.Driver")\
                        .option("truncate", "true")\
                        .mode("overwrite")\
                        .save()
                    num_enr = spark.read\
                                .format("jdbc").option("url", "jdbc:postgresql://{}:{}/{}".format(url, port, db_name))\
                                .option("user", user)\
                                .option("password", password)\
                                .option("dbtable", "sid_integ01.{}".format(TableName))\
                                .option("driver", "org.postgresql.Driver")\
                                .load()\
                                .count()
                    print("La table [sid_integ01.{}] a {} ligne(s) d'enregistrement (apres l'ecriture)".format(TableName, num_enr))
                    print("/\\" * 31 + "FIN" + "/\\" * 31 + "\n")
                    end_time_local_psql = time.time()
                    elapsed_time_local_psql = end_time_local_psql - start_time_local_psql  
                    print("Le temps ecoule pour le traitement de la requete : [{}] sur HDP est : {} secondes ~ : {}:{}:{}:{}".format(requete, round(elapsed_time_local_psql, 3), int(elapsed_time_local_psql // 3600), int(elapsed_time_local_psql // 60), int(elapsed_time_local_psql % 60), str(round(elapsed_time_local_psql, 3)).split('.')[1]) + "\n\n" + "_" * 61 + "FIN" + "_" * 61)
                    num_enr_total += num_enr
        end_time_global = time.time()
        elapsed_time_global = end_time_global - start_time_global
        print("=" * 126)
        print("Le temps ecoule pour le traitement des {} requete(s) est : {} secondes qui equivaut a : {}:{}:{}:{} (format : hh:mm:ss:mm)".format(len(requetes) - 1, round(elapsed_time_global, 3), int(elapsed_time_global // 3600), int(elapsed_time_global // 60), int(elapsed_time_global % 60), str(round(elapsed_time_global, 3)).split('.')[1]))
        print("=" * 126)
        print("Le nombre total d'enregistrements copies est : {} ligne(s) d'enregistrement".format(num_enr_total))

    print("_" * 61 + "FIN GLOBAL" + "_" * 61)
    print("Le : {} a : {}".format(str(datetime.now()).split(' ')[0], str(datetime.now()).split(' ')[1].replace('.', ':')[:-3]))


def copy_postgres_to_hadoop(url, port, db_name, user, password, directory_to_scripts_sql):
    spark = create_spark_context('ChargementPostgresqlToHadoop')
   
    num_req = 1
    num_enr_total = 0
   
    con_prop = {"user": user, "password": password, "driver": "org.postgresql.Driver"}
   
    listeDesFichiers = os.listdir(directory_to_scripts_sql)
   
    print("_" * 55 + "DEBUT GLOBAL" + "_" * 55)
    print("Le : {} a : {}".format(str(datetime.now()).split(" ")[0], str(datetime.now()).split(" ")[1].replace(".", ":")[:-3]))
   
    print("\nLe nombre de fichier(s) dans le dossier : [{}] est : {}\n".format(directory_to_scripts_sql.split("/")[-1], len(listeDesFichiers)))
   
    for nom_fichier in listeDesFichiers:
        chemin_fichier = os.path.join(directory_to_scripts_sql, nom_fichier)
        with open(chemin_fichier, "r") as fichier:
            contenu = fichier.read()
        requetes = contenu.split(";")
       
        print("Le nombre de requetes dans le fichier : [{}] est : {}".format(nom_fichier, len(requetes) - 1))
        print("\nLe(s) requete(s) sont :")
        for requete_index in range(len(requetes) - 1):
            print("\nLa requete numero {} est : {}".format(requete_index + 1, requetes[requete_index]))
        print("")
       
        start_time_global = time.time()
        for requete in requetes:
            requete = requete.strip()
            if requete:
                print("_" * 60 + "DEBUT" + "_" * 60 + "\n")
                start_time_local = time.time()
               
                DfPsql = spark.read.format("jdbc")\
                    .option("url", "jdbc:postgresql://{}:{}/{}".format(url, port, db_name))\
                    .options(**con_prop)\
                    .option("dbtable", "({}) AS subquery".format(requete))\
                    .load()
                num_enr = DfPsql.count()
                end_time_local = time.time()
                elapsed_time_local = end_time_local - start_time_local
                print('')
                print("Requete [{}] executee  est : {}".format(num_req, requete))
                num_req += 1
                print('')
                print("=" * 126)
                print("Le temps ecoule pour le traitement de la requete [{}] sur HDP est : {} sec ~ : {}:{}:{}:{}".format(requete, round(elapsed_time_local, 3), int(elapsed_time_local // 3600), int(elapsed_time_local // 60), int(elapsed_time_local % 60), str(round(elapsed_time_local, 3)).split('.')[1]) + "\n" + "=" * 126 + "\n")
                print("Ecriture dans le HDP...\n")
               
                start_time_local_psql = time.time()
                print("/\\" * 30 + "DEBUT" + "/\\" * 30 + "\n")
                print("La requete dont le contenu sera copie est : {}\nEt la table en question est : {}".format(requete, requete.split(".")[1]))
                print("")
                TableName = requete.split(".")[1]
                print("La table [{}] a {} ligne(s) d'enregistrement".format(TableName, num_enr))
                spark.sql("truncate table db_work_sid_prex18." + TableName)
                DfPsql.write.mode("overwrite").saveAsTable("db_work_sid_prex18." + TableName)
                print("/\\" * 31 + "FIN" + "/\\" * 31 + "\n")
                end_time_local_psql = time.time()
                elapsed_time_local_psql = end_time_local_psql - start_time_local_psql  
                print("Le temps ecoule pour le traitement de la requete : [{}] sur HDP est : {} secondes ~ : {}:{}:{}:{}".format(requete, round(elapsed_time_local_psql, 3), int(elapsed_time_local_psql // 3600), int(elapsed_time_local_psql // 60), int(elapsed_time_local_psql % 60), str(round(elapsed_time_local_psql, 3)).split('.')[1]) + "\n" + "_" * 61 + "FIN" + "_" * 61 )
                num_enr_total += num_enr
                   
        end_time_global = time.time()
        elapsed_time_global = end_time_global - start_time_global
        print("=" * 126)
        print("Le temps ecoule pour le traitement des {} requete(s) est : {} secondes qui equivaut a : {}:{}:{}:{} (format : hh:mm:ss:mm)".format(len(requetes) - 1, round(elapsed_time_global, 3), int(elapsed_time_global // 3600), int(elapsed_time_global // 60), int(elapsed_time_global % 60), str(round(elapsed_time_global, 3)).split('.')[1]))
        print("=" * 126)
        print("\nLe nombres total d'enregistrement copie est : {} ligne(s) d'enregistrement\n".format(num_enr_total))
   
    print("\n" + "_" * 61 + "FIN GLOBAL" + "_" * 61)
    print("Le : {} a : {}".format(str(datetime.now()).split(" ")[0], str(datetime.now()).split(" ")[1].replace(".", ":")[:-3]))

def export_data_on_file(directory_to_scripts_sql):
    spark = create_spark_context('ExportDataToFile')
    tmp_file = "/user/u02e224/tmp_file"
   
   
    listeDesFichiers = os.listdir(directory_to_scripts_sql)
   
    print("_" * 55 + "DEBUT GLOBAL EXPORT" + "_" * 55)
    print("Le : {} a : {}".format(str(datetime.now()).split(" ")[0], str(datetime.now()).split(" ")[1].replace(".", ":")[:-3]))
   
    print("\nLe nombre de fichier(s) dans le dossier : [{}] est : {}\n".format(directory_to_scripts_sql.split("/")[-1], len(listeDesFichiers)))
   
    for nom_fichier in listeDesFichiers:
        chemin_fichier = os.path.join(directory_to_scripts_sql, nom_fichier)
        with open(chemin_fichier, "r") as fichier:
            contenu = fichier.read()
        requetes = contenu.split(";")
       
        print("Le nombre de requetes dans le fichier : [{}] est : {}".format(nom_fichier, len(requetes) - 1))
        print("\nLe(s) requete(s) sont :")
        for requete_index in range(len(requetes) - 1):
            print("\nLa requete numero {} est : {}".format(requete_index + 1, requetes[requete_index]))
        print("")
       
        for requete in requetes:
            requete = requete.strip()
            if requete:
                print("_" * 60 + "DEBUT EXPORT" + "_" * 60 + "\n")
                output_file = os.path.join(tmp_file, "{}".format(requete.split('.')[-1]))
                spark.sql(requete).write.csv(output_file, header=True, mode="overwrite")
                print("Les donnees ont ete exportees au fichier : {}".format(output_file))
                print("_" * 60 + "FIN EXPORT" + "_" * 60 + "\n")
                   
    print("\n" + "_" * 61 + "FIN GLOBAL EXPORT" + "_" * 61)
    print("Le : {} a : {}".format(str(datetime.now()).split(" ")[0], str(datetime.now()).split(" ")[1].replace(".", ":")[:-3]))


if __name__ == "__main__":
    if len(sys.argv) >= 8:
        url, port, db_name, user, password, action, directory_to_scripts_sql = sys.argv[1:8]

        if action == "HP":
            copy_hadoop_to_postgres(url, port, db_name, user, password, directory_to_scripts_sql)
        elif action == "PH":
            copy_postgres_to_hadoop(url, port, db_name, user, password, directory_to_scripts_sql)
        elif action == "EX":
            export_data_on_file(directory_to_scripts_sql)
        else:
            print("Action invalide.\n/srv/hadoop/tools/sid/commun/shell/copie_data_exec_SQL_MMA.sh /srv/hadoop/tools/sid/commun/pythonListeTablesI.txt  [HP|PH|EX]")
    else:
        print("Pas assez d'arguments. Utilisation :")
        print("Rappel d'Usage :\n/srv/hadoop/tools/sid/commun/shell/copie_data_exec_SQL_MMA.sh <DirecrtoryFileTables> <tmp_file> <action>\nexemple : /srv/hadoop/tools/sid/commun/shell/copie_data_exec_SQL_MMA.sh /srv/hadoop/tools/sid/commun/pythonListeTablesI.txt /tmp_file [HP|PH|EX]")