# 2020-deis-chile-deaths
Análisis de las muertes en Chile entre los años 1990-2017 con Apache Pig y Hadoop.  

> Grupo 05
> - Geraldine Alvadiz
> - Cristobal Mesías
> - Franco Sanguineti

# Overview

En este proyecto se realiza un análisis demográfico de las causas de muerte en Chile, utilizando para ello datos entre los años 1990 y 2017. 

Los experimentos de este análisis se agrupan en tres grupos: causas de muerte por rango etario y año, causas de muerte por lugar de residencia y causas de muerte por estado civil. 

Para el primer grupo, se quiere ver si existe una correlación entre la causa de muerte y la edad, más en concreto, comprobar si la causa de muerte de los Adultos Mayores, que en teoría son los muertos con mayor proporción en Chile, difieren mucho de las causas de muerte de otros grupos etarios. 

Por otra parte, en los experimentos que involucran ubicaciones geográficas: para las regiones, se quiere responder la pregunta de si las regiones más frías tienen mayor cantidad de muertes producidas por enfermedades respiratorias, o si las regiones con mayor actividad minera ven reflejada la contaminación ambiental en sus causas de muerte más típicas. En el caso de las comunas de santiago, se quiere saber si las comunas con mayor actividad industrial ven reflejada dicha actividad entre sus causas de muerte. 

En el caso de las causas de muerte por estado civil, se quiere averiguar si estar casado influye en la causa de muerte o no. 
Finalmente, es sabido que la principal causa de muerte en Chile es el Infarto Agudo al Miocardio. Esto es algo que también se quiere comprobar con esta investigación.

# Data

Para realizar este experimento, se utizó un dataset del Departamento de Estadísitcas e Información de Salud, entidad perteneciente al Ministerio de Salud del Gobierno de Chile. Este dataset cuenta con información de las muertes en Chile entre el período 1990 - 2017, sin embargo, existe una mayor cantidad de detalles para el período 1997 - 2017, por lo que la mayoría de experimentos se realizarán utilizando dicha ventana. En estricto rigor, los datos en el período 1990 - 1996, no cuentan con la causa de muerte. 

Este dataset fue escogido principalmente porque es el único archivo que recopila la cantidad de muertes en Chile, además de ser información oficial del Ministerio de Salud. [Fuente de los datos](https://deis.minsal.cl/#datosabiertos).

El dataset original es un archivo `.csv` que cuenta con 2.443.004 tuplas, correspondientes cada una a los datos de una defunción, anonimizada. Además, la cantidad de atributos por tupla o columnas del dataset corresponden a 97. Este archivo original pesa alrededor de 1,6 Gigabytes.

El archivo original fue preprocesado, cambiando su extensión a `.tsv`, esto es, cambiando los puntos y comas por tabulaciónes, para hacer más sencillo su procesamiento, resultando en un archivo de 540 Megabytes. 

Se escogieron, de las 97 columnas con las que se contaban inicialmente, sólo 14: un ID único para cada caso, el año de la defunción, la fecha, el sexo del fallecido, su edad al momento de la defunción, la glosa de la edad, que es un atributo que muestra si la edad del paciente se mide en años, meses o días; el estado civil, el nivel de estudios, la ocupación, el local de defunción, la región del fallecido, la comuna del fallecido, la causa de fallecimiento y su capítulo, que es una causa más global de muerte. Se adjunta un [Diccionario de Datos](https://github.com/cc5212/2020-deis-chile-deaths/blob/master/data/diccionariodedatos.xlsx) donde se explican de mejor forma que representa cada parámetro de las tuplas, las cuales se encuentran en color verde las utilizadas.

# Methods

Se utiliza un script de Python para preprocesar los datos, tal como se explica en el punto anterior. Se decide hacer esto, dado que existía un alto nivel de campos sin información. Además, pese a que el archivo poseía encoding `UTF-8`, se decide reemplazar el caracter `ñ` por `nh` en la columna que describe la edad de la persona, ya que ésta se utilizaba en una cooparación de Strings en la cual fallaba la palabra `años`. Todo este preprocesamiento se realiza utilizando Python, puesto que resulta más fácil la escritura de código en este lenguaje y, a su vez, posee varias librerías que facilitan el trabajo con archivos `.csv`. En este caso, se utiliza la librería `csv` de Python. Por otra parte, para crear datasets de prueba para los distintos experimentos, se utiliza la librería `subsample` de Python, que, a través de ejecutar el comando `subsample -n 100000 <fuente> > <destino>` permite crear datasets con 100.000 tuplas tomadas de forma aleatoria desde el dataset original. 

Para procesar los datos, se requiere utilizar `Hadoop`, un framework que se utiliza para crear aplicaciones distribuidas que manejen grandes volúmenes de datos, para realizar distintas operaciones de Map-Reduce, dado que los experimentos que se desean realizar pueden ser reducidos a estas operaciones. En específico, de `Hadoop` se requiere utilizar el HDFS (Hadoop Distributed File System) para almacenar los datos. Sin embargo, dado que la escritura de un programa que funcione en `hadoop` puede resultar tediosa e incluso muy verbosa, es que se ha escogido utilizar `Apache Pig` para su escritura, que es sencillamente un _wrapper_ que, luego de recibir un conjunto de órdenes en el lenguaje de programación `pig latin`, las transforma a instrucciones que se pueden ejecutar en `hadoop`. Escribir instrucciones en `pig` es mucho más sencillo, puesto que se emplean _queries_ muy parecidas a las que se realizan en el lenguaje `SQL`.

De esta forma, se generan [seis scripts](https://github.com/cc5212/2020-deis-chile-deaths/blob/master/src/) distintos con extensión `.pig`, en donde están escritas las instrucciones necesarias para cada uno de los experimentos a realizar. Finalmente, en la salida, se obtienen archivos `.tsv` con los [resultados](https://github.com/cc5212/2020-deis-chile-deaths/blob/master/results/).  

# Results

## Top 3 de causas de muertes agrupadas por año, entre 1997 y 2017.
La principal causa de muerte en Chile es el Infarto Agudo al Miocardio, para todos los años, con excepción del período 1997-2001, en donde fue la Neumonía. Los Accidentes Vasculares y los Tumores al Pulmón o Estómago también aparecen como causas importantes. 

|             |      Causa 1      |       Causa 2      |       Causa 3      |
|-------------|:-----------------:|:------------------:|:------------------:|
| 1997 - 2001 |      Neumonía     |  Infarto Miocardio | Accidente Vascular |
|     2002    | Infarto Miocardio |      Neumonía      |   Tumor Estómago   |
|     2003    | Infarto Miocardio | Accidente Vascular |   Tumor Estómago   |
|     2004    | Infarto Miocardio |      Neumonía      | Accidente Vascular |
| 2005 - 2014 | Infarto Miocardio |      Neumonía      |   Tumor Estómago   |
| 2015 - 2017 | Infarto Miocardio |   Tumor Estómago   |    Tumor Pulmón    |


## Causas de muerte agrupadas por el estado civil.
El experimento de obtener las causas de muerte por estado civil se realizó sólo entre personas de 18 y 40 años. La causa de muerte común para todos los estados civiles es la Asfixia, con excepción de los viudos que mueren por Traumatismo Intracraneal y los Convivientes que mueren por VIH que resulta en Tumores. Cabe destacar que estos son sólo 12 del total del universo analizado, dado que el Acuerdo de Unión Civil, se promulgó en el año 2015.

|             | Total de muertes |       Causa       | Cantidad por causa | % de causa |
|-------------|:----------------:|:-----------------:|:------------------:|:----------:|
|    Casado   |       28659      |      Asfixia      |        3583        |     13%    |
| Conviviente |        12        |   VIH -> Tumores  |          2         |     17%    |
|  Divorciado |        232       |      Asfixia      |         37         |     16%    |
|   Soltero   |       80642      |      Asfixia      |        10918       |     14%    |
|    Viudo    |        496       | Tr.  Intracraneal |         50         |     10%    |

## Causas de muerte agrupadas por rango etario.
En el experimento se agrupó por los distintos rangos etarios que se observan en la tabla y se obtuvo que la principal causa de muerte en Adolescentes, Adulto Joven y Adultos es Asfixia, la principal causa de muerte en bebés es Trastornos relacionados con duración corta de la gestación y con bajo peso al nacer, lo cual se nombró como Prematuro en la tabla, la principal causa de muerte en Niños es Traumatismo Intracraneal y finalmente la principal causa de muerte en Adulto Maduro y Adulto Mayor es Infarto Agudo al Miocardio. Es importante señalar que el porcentaje de Adulto Mayor, con respecto al total, es bastante grande (68,3%) y la de Niños y Adolescentes es muy pequeño (0,8% y 0,6% respectivamente).  

|                              | Total de muertes | % del total |       Causa       | Cantidad por causa | % de causa |
|------------------------------|:----------------:|-------------|:-----------------:|:------------------:|:----------:|
|      Bebés (menor 1 año)     |       42292      |     2,2%    |     Prematuro     |        4890        |     12%    |
|      Niños (1 - 12 años)     |       14306      |     0,8%    |  Tr. Intracraneal |        1305        |     9%     |
|  Adolescentes (12 - 18 años) |       10877      |     0,6%    |      Asfixia      |        1463        |     13%    |
|  Adulto Joven (18 - 30 años) |       50493      |     2,6%    |      Asfixia      |        8231        |     16%    |
|    Adultos (30 - 50 años)    |      166423      |     8,7%    |      Asfixia      |        12192       |     7%     |
| Adulto Maduro (50 - 65 años) |      320091      |    16,8%    | Infarto Miocardio |        25418       |     8%     |
| Adulto Mayor (65 o más años) |      1301776     |    68,3%    | Infarto Miocardio |        91163       |     7%     |


## Top 3 causas de muerte agrupadas por regiones.
En el caso de las regiones, las tres causas principales de defunción son el Infarto Agudo al Miocardio, la Neumonía y los Tumores al Pulmón o al Estómago. Tres regiones que resultan ser una interesante excepción son la de Atacama y Valparaíso, en donde aparece el Accidente Vascular Encefálico Agudo como nueva causa. En el caso de la región de Antofagasta, la principal causa de muerte es la de Tumor Maligno en los Bronquios o Pulmón. En las regiones de La Araucanía y de Los Lagos hay una tendencia a no registrar o especificar correctamente la causa de muerte de las personas.

| Región          | Causa 1                                                         | Causa 2                                                                           | Causa 3                                                                           |
|-----------------|-----------------------------------------------------------------|-----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| De Atacama      | Infarto Agudo del Miocardio                                     | Accidente vascular encefálico agudo, no especificado como hemorrágico o isquémico | Tumor Maligno de los Bronquios y del Pulmón                                       |
| De Valparaíso   | Infarto Agudo del Miocardio                                     | Neumonía, Organismo no Especificado                                               | Accidente vascular encefálico agudo, no especificado como hemorrágico o isquémico |
| De Antofagasta  | Tumor Maligno de los Bronquios y del Pulmón                     | Infarto Agudo del Miocardio                                                       | Neumonía, organismo no especificado                                               |
| De Los Lagos    | Infarto agudo del miocardio                                     | Otras causas mal definidas y las no especificadas de mortalidad                   | Neumonía, organismo no especificado                                               |
| De La Araucanía | Otras causas mal definidas y las no especificadas de mortalidad | Infarto agudo del miocardio                                                       | Neumonía, organismo no especificado                                               |
| Otras           | Infarto agudo del miocardio                                     | Neumonía, organismo no especificado                                               | Tumor maligno del estómago/los bronquios y del pulmón                             |

## Causas de muerte agrupadas por comuna, sólo Región Metropolitana.
En el caso de las comunas de la Región Metropolitana, en todas la principal causa de muerte es el Infarto Agudo al Miocardio. Se producen excepciones en las comunas de Lampa, Tiltil y San José de Maipo, en donde la principal causa de muerte es la Neumonía. 

| Comuna            | Total de muertes | Causa                                                           | Cantidad por causa | % por causa |
|-------------------|------------------|-----------------------------------------------------------------|--------------------|-------------|
| San José de Maipo | 1723             | Neumonía, organismo no especificado                             | 114                | 7%          |
| Lampa             | 5201             | Neumonía, organismo no especificado                             | 338                | 6%          |
| Tiltil            | 1624             | Neumonía, organismo no especificado                             | 96                 | 6%          |
| Alhué             | 578              | Otras causas mal definidas y las no especificadas de mortalidad | 59                 | 10%         |
| Otras             | 707024           | Infarto Agudo del Miocardio                                     | 44588              | 6%          |

# Conclusion

La principal conclusión que se puede obtener de los experimentos realizados es que efectivamente, la causa principal de muerte en Chile es el Infarto Agudo al Miocardio. Sin embargo, el experimento de los rangos etarios nos permite concluir que esto se debe a que, como en Chile el 68,3% de las muertes que se registran pertenece a gente de más de 65 años, es que los experimentos por región y comuna se ven inflados en muertes que tienen como principal causa el Infarto Agudo al Miocardio gracias a esta proporción. En ese sentido, habría sido interesante haber realizado los experimentos por ubicación geográfica excluyendo a los Adultos Mayores e incluso a los Adultos Maduros. Esto sería interesante de calcular si se quisiera realizar una continuación de este trabajo. 

Resulta interesante que las mayores causas de muertes cambien en función de los años. No es mucho lo que se puede concluir en este apartado, puesto que para ello se necesitaría, quizás, una investigación más detallada de los comportamientos de los chilenos durante esos años y, probablemente, durante los anteriores, para poder comprender por qué se producen esas variaciones.

Para el caso del análisis por estado civil, como la causa común es la de asfixia, que coincide curiosamente con ser la causa de mayores muertes en el rango etario de 12 a 50 años, entonces no se puede concluir nada relevante. Queda descartado de que la gente que se encuentra casada la pase peor que la gente que no, debido a que tienen la misma muerte que el común de la gente de la edad analizada.

Para la causa de muerte por rango etario, al igual que para la causa de muerte por años, es interesante ver cómo cambia la causa de muerte a medida que la edad va aumentando, pero no es mucho lo que se puede concluir. Lo que sí se puede notar es que los Adultos Maduros y Adultos Mayores, al morir más de Infarto Agudo al Miocardio puede indicar que a medida que una persona envejece, su corazón se va deteriorando. Además, también es interesante observar que la mayor causa de muerte en bebés son los trastornos relacionados con duración corta de la gestación y con bajo peso al nacer con un 12%, lo que significa que una cantidad no menor de Bebés que fallece es por nacer prematuros.

Volviendo a los experimentos por ubicación geográfica, en el caso de las regiones, se puede afirmar que efectivamente la alta actividad minera en la región de Antofagasta tiene un impacto en la causa de muerte de las personas, puesto que la mayor causa de muerte es la de Tumor en los Bronquios o Pulmón. También habría sido interesante realizar dicho análisis separado por grupos etarios.  Respecto a las regiones más frías y las causas de muerte por enfermedades respiratorias, no se puede concluir que en estos lugares se vean acentuadas, debido a que el top tres de causas es el mismo que en el resto del país, con excepción de las cuatro regiones que se analizaron previamente.

Si se miran las comunas de la Región Metropolitana, con los datos obtenidos, no se puede concluir que las comunas con mayor actividad industrial, como lo son, por ejemplo, Pudahuel, Estación Central, La Pintana o La Cisterna, tengan causas relacionadas a contaminación como las de mayores muertes, puesto que tienen la misma causa de muerte princiapl que otras comunas. Resulta interesante el caso de las tres comunas que son excepcionales en su causa de muerte principal, puesto que pertenecen a las comunas de la RM con menores temperaturas mínimas, por lo que se podría sugerir que hay una relación entre la temperatura y la causa de muerte, sin embargo, al contrastarlo con las regiones, en donde esto no pasa, incluso para regiones con temperaturas promedio y extremas menores, entonces se puede desechar esta sugerencia.