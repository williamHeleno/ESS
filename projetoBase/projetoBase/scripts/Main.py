# -*- coding: utf-8 -*- 
    
##################################################################################################################################################################
# Created on 24 de Fev de 2021
# 
#     Projeto Teste Unilever:
#     MAIN
#     Author: Bruno William
# 
##################################################################################################################################################################

##################################################################################################################################################################
#IMPORT
from pyspark.sql.types              import *
from pyspark.sql.functions          import *
from pyspark.sql                    import HiveContext
from pyspark                        import SparkContext

##################################################################################################################################################################

##################################################################################################################################################################
#Set arguments
dicParameteres = {}
dicParameteres['arg1'] = argv[1] # arg1: argumento 1 vindo da shell

#Create context
yarn_name = "NOME_QUE_IRA_APARECER_NO_YARN"
sc = SparkContext('yarn-cluster', yarn_name)

sqlContx = HiveContext(sc)
sqlContx.setConf('spark.sql.parquet.compression.codec', 'snappy')
sqlContx.setConf('hive.exec.dynamic.partition', 'true')
sqlContx.setConf('hive.exec.dynamic.partition.mode', 'nonstrict')

#imprimindo conteúdo no dicionário criado
for key, value in dicParameters.items():
    if not key:
        print('Configuracao inexistente para {0} : {1}'.format(key, value))
    else:
        print('Parametro {0} : {1}'.format(key, value))    
##################################################################################################################################################################
    
##################################################################################################################################################################
def startProcess(dicParameters):

    func_valid = False
    
    print ":..........:  PROCESS STARTED  :..........:"
    
    if dicParameters["arg1"] == 'executaCase1':
        class_python = Case1(sc, sqlContx, dicParameters)
        issucceed = class_python.start_process()    
        func_valid = True

    if dicParameters["arg1"] == 'executaCase2':
        class_python = Case2(sc, sqlContx, dicParameters)
        issucceed = class_python.start_process()    
        func_valid = True

    if func_valid == False:
        print('ERRO: Funcao {0} nao existe'.format(argument))
##################################################################################################################################################################
    
##################################################################################################################################################################
startProcess(dicParameteres)
sc.close()
print('Executado com sucesso')
##################################################################################################################################################################