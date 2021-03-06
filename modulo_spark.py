from pyspark import SparkConf, SQLContext, SparkContext
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col, sum, year, month, dayofweek
from pyspark.sql.types import DateType

import numpy as np 
import matplotlib.pyplot as plt 
import pandas as pd 
import traceback 
import scipy.stats as stats


class SparkDataframe:  

    def __init__(self):
        self.template = '.\dados\sample\*'

        sc = SparkContext("local[3]", "teste")
        spark = SQLContext(sc)  
        self.arquivo = spark.read.format('csv').options(header=True, inferSchema = True).load(self.template)
        self.arquivo = self.arquivo.withColumn("DATE", col("DATE").cast(DateType()))

        self.arquivo.createOrReplaceTempView("source")
        self.selecionado = self.arquivo
        self.a_atual = 0
        self.b_atual = 0
        self.y_min = 0
        self.y_max = 0
        self.dP = 0
        self.med = 0

    def query(self, string):
        self.selecionado = self.arquivo.sql(string)

    def filter(self, string):
        self.selecionado = self.arquivo.filter(string)

    def agrupar(self, op, colunaLabel, colunaAlvo, tempo):
        if tempo:
            if tempo == 'year':
                self.selecionado = self.selecionado.groupBy(year(colunaLabel))
            elif tempo == 'month':
                self.selecionado = self.selecionado.groupBy(month(colunaLabel)) 
            elif tempo == 'day':
                self.selecionado = self.selecionado.groupBy(dayofweek(colunaLabel))
            else: self.selecionado = self.selecionado.groupBy(colunaLabel)

        if op == "sum":
            self.selecionado = self.selecionado.sum(colunaAlvo)
        elif op == "count":
            self.selecionado = self.selecionado.count()
        elif op == "max":
            self.selecionado = self.selecionado.max(colunaAlvo)
        elif op == "min":
            self.selecionado = self.selecionado.min(colunaAlvo)
        elif op == "avg":
            self.selecionado = self.selecionado.avg(colunaAlvo)       
        else:
            print("fun????o n??o reconhecida.")
        

    def media(self, coluna):
        temp = self.selecionado.select(_mean(col(coluna))).alias('mean').collect()
        return temp[0][0]

    def desvioPadrao(self, coluna):
        temp = self.selecionado.select(_stddev(col(coluna))).alias('std').collect()
        return temp[0][0]

quit = False

rdd = SparkDataframe()

while (not quit):  
    try: 
        print()
        entrada = input("> ")
        if entrada == "quit": 
            quit = True
            break

        elif entrada == "filter":
            temp = input("> Filtre:") 
            rdd.filter(temp)            
            rdd.selecionado.show()

        elif entrada == "show":
            print(rdd.selecionado.toPandas())

        elif entrada == "media": 
            valor = rdd.media(temp)
            rdd.med = valor
            print(valor)

        elif entrada == "desvio padrao": 
            temp = input("> Diga qual coluna:")
            valor = rdd.desvioPadrao(temp)
            rdd.dP = valor
            print(valor)
            print('Preparando gr??fico...')
            
            mu = rdd.med
            sigma = valor
            x = np.linspace(mu - 3*sigma, mu + 3*sigma, 100)
            plt.plot(x, stats.norm.pdf(x, mu, sigma))
            plt.show()

        elif entrada == "agrupar": 
            op = input("> Qual fun????o?: ") 
            colunaLabel = input("> Qual coluna de grupo?: ")
            colunaAlvo = input("> Qual a coluna da fun????o?: ")
            tempo = input("> Selecione o per??odo de tempo (year,month,day ou nada): ")
            rdd.agrupar(op, colunaLabel, colunaAlvo, tempo)
            print(rdd.selecionado.toPandas())

        elif entrada == "query": 
            temp = input("> Fa??a uma query SQL(a tabela ?? source): ")
            rdd.query(temp)

        elif entrada == "clear": #<---------------------------------- Limpe a sele????o.
            rdd.selecionado = rdd.arquivo
            rdd.b_atual = 0
            rdd.a_atual = 0
            rdd.y_max = 0
            rdd.y_min = 0
            rdd.dP = 0
            rdd.med = 0
            print("Sele????o atual limpa. Default = o dataset inteiro.")
        
        elif entrada == "schema":
            rdd.printSchema()
        else:
            print("Comando n??o reconhecido.")
    except:
        print("Crashou")
        traceback.print_exc()
