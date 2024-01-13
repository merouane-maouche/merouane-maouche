!/usr/bin/env python
# USAGE : exec_SQL.py SID

import sys
from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

fmt_tsnb = '%Y/%m/%d %H:%M:%S'
ts = datetime.now().strftime(fmt_tsnb)

def exec_fichier(spark, sc, chemin_sql):
    for SQL in sc.wholeTextFiles(chemin_sql).take(1)[0][1].split(';') :
        if len(SQL.strip())>0 : spark.sql(SQL.strip())
 
   
def run_file(file, spark, sc):
    pathfile = str(file.getPath())
    start = datetime.now().strftime(fmt_tsnb)
    print('Debut de traitement : '+start)
    exec_fichier(spark, sc, pathfile)
    end = datetime.now().strftime(fmt_tsnb)
    print('Fin de traitement : '+end)
    elapsed_time = (datetime.strptime(end, fmt_tsnb)-datetime.strptime(start, fmt_tsnb)).total_seconds()
    print('Temps de traitement : '+str(timedelta(seconds=elapsed_time))+'\n')
    return  elapsed_time



def main():
    if len(sys.argv)>=2:
        appli = sys.argv[1]
        mode_recursif = sys.argv[2]
         
        sc = SparkContext(conf=SparkConf().setMaster('yarn').setAppName('Chargement_'+appli+'_0'))
        spark = SQLContext(sc)
        fs = (sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration()))
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        total_elapsed_time = 0
        folder = fs.globStatus(Path(appli+'/*.sql'))
        print(len(folder))
        if mode_recursif =='O':
            if  len(folder) == 4 :
                    print(folder[3].getPath())
                    total_elapsed_time = total_elapsed_time +run_file(folder[0], spark, sc)
                    chemin_sql = str(folder[3].getPath())
                    print(sc.wholeTextFiles(chemin_sql).take(1)[0][1])
                    reqSQL = spark.sql(sc.wholeTextFiles(chemin_sql).take(1)[0][1].split(';')[0].strip())
                    print(reqSQL.first())
                   
                   
                    while(reqSQL.rdd.isEmpty()==False):
                        print("nombre lignes restantes", reqSQL.count())
                        print('recurive started')
                       
                        total_elapsed_time =  total_elapsed_time +run_file(folder[1], spark, sc)
                        reqSQL = spark.sql(sc.wholeTextFiles(chemin_sql).take(1)[0][1].split(';')[0].strip())
                        spark.sql(sc.wholeTextFiles(chemin_sql).take(1)[0][1].split(';')[2].strip())
                    print('recursive ended')
                    total_elapsed_time =  total_elapsed_time +run_file(folder[2], spark, sc)
            else :
                print(" Erreur : Pas assez d'arguments \n")
        else:
            for fichier in folder:
                total_elapsed_time =  total_elapsed_time + run_file(fichier, spark, sc)
           
       
        print('Temps de traitement total : '+str(timedelta(seconds=total_elapsed_time))+'\n')
        print('______________________________________FIN______________________________________ : '+datetime.now().strftime(fmt_tsnb))
       
    else: print('Repertoire Manquant')

if __name__ == "__main__":
    main()