# 2020-deis-chile-deaths
Análisis de las muertes en Chile entre los años 1990-2017 con Apache Pig y Hadoop.  [Geraldine Alvadiz, Cristobal Mesías, Franco Sanguineti. Grupo 05]

# Overview

En este proyecto se realiza un análisis demográfico de las causas de muerte en Chile, utilizando para ello datos entre los años 1990 y 2017. Los experimentos de este análisis se agruparán en tres grupos: causas de muerte por rango etario y año, causas de muerte por lugar de residencia y causas de muerte por estado civil. Para el primer grupo, se quiere ver si existe una correlación entre la causa de muerte y la edad, más en concreto, comprobar si la causa de muerte de los adultos mayores, que son los muertos con mayor proporción en Chile, difieren mucho de las causas de muerte de otros grupos etarios. Por otra parte, en los experimentos que involucran ubicaciones geográficas: para las regiones, se quiere responder la pregunta de si las regiones más frías tienen mayor cantidad de muertes producidas por enfermedades respiratorias, o si las regiones con mayor actividad minera ven reflejada la contaminación ambiental en sus causas de muerte más típicas. En el caso de las comunas de santiago, se quiere saber si las comunas con mayor actividad industrial ven reflejada dicha actividad entre sus causas de muerte. Finalmente, en el caso de las causas de muerte por estado civil, se quiere averiguar si estar casado influye en la causa de muerte o no. Finalmente, es sabido que la principal causa de muerte en Chile es el infarto al miocardio. Esto es algo que también se quiere comprobar con esta investigación.

# Data

Para realizar este experimento, se utizó un dataset del Departamento de Estadísitcas e Información de Salud, entidad perteneciente al Ministerio de Salud del Gobierno de Chile. Este dataset cuenta con información de las muertes en Chile entre el período 1990 - 2017, sin embargo, existe una mayor cantidad de detalles para el período 1997 - 2017, por lo que la mayoría de experimentos se realizarán utilizando dicha ventana. En estricto rigor, los datos en el período 1990 - 1996, no cuentan con la causa de muerte. Este dataset fue escogido principalmente porque es el único archivo que recopila la cantidad de muertes en Chile, además de ser información oficial del Ministerio de Salud. [Fuente de los datos](https://deis.minsal.cl/#datosabiertos).

El dataset original es un archivo .csv que cuenta con 2.443.004 tuplas, correspondientes cada una a los datos de una defunción, anonimizada. Además, la cantidad de atributos por tupla o columnas del dataset corresponden a 97. Este archivo original pesa alrededor de 1,6 Gigabytes.

El archivo original fue preprocesado, cambiando su extensión a .tsv, esto es, cambiando los puntos y comas por tabulaciónes, para hacer más sencillo su procesamiento, resultando en un archivo de 540 Megabytes. Se escogieron, de las 97 columnas con las que se contaban inicialmente, sólo 13: un ID único para cada caso, el año de la defunción, la fecha, el sexo del fallecido, su edad al momento de la defunción, la glosa de la edad, que es un atributo que muestra si la edad del paciente se mide en años, meses o días; el estado civil, el nivel de estudios, la ocupación, el local de defunción, la región del fallecido, la comuna del fallecido, la causa de fallecimiento y su capítulo, que es una causa más global de muerte.

# Methods


Se utiliza un script de Python para preprocesar los datos (explicados en el punto anterior). Luego de esto se utiliza Pig y Hadoop para procedar dichos datos, utilizando MapReduce como principal método para lograr los resultas esperados.

Se elige Pig dado su similitud a SQL.

# Results

|             |      Causa 1      |       Causa 2      |       Causa 3      |
|-------------|:-----------------:|:------------------:|:------------------:|
| 1997 - 2001 |      Neumonía     |  Infarto Miocardio | Accidente Vascular |
|     2002    | Infarto Miocardio |      Neumonía      |   Tumor Estómago   |
|     2003    | Infarto Miocardio | Accidente Vascular |   Tumor Estómago   |
|     2004    | Infarto Miocardio |      Neumonía      | Accidente Vascular |
| 2005 - 2014 | Infarto Miocardio |      Neumonía      |   Tumor Estómago   |
| 2015 - 2017 | Infarto Miocardio |   Tumor Estómago   |    Tumor Pulmón    |

|             | Total de muertes |       Causa       | Cantidad por causa | % de causa |
|-------------|:----------------:|:-----------------:|:------------------:|:----------:|
|    Casado   |       28659      |      Asfixia      |        3583        |     13%    |
| Conviviente |        12        |   VIH -> Tumores  |          2         |     17%    |
|  Divorciado |        232       |      Asfixia      |         37         |     16%    |
|   Soltero   |       80642      |      Asfixia      |        10918       |     14%    |
|    Viudo    |        496       | Tr.  Intracraneal |         50         |     10%    |

|                              | Total de muertes | % del total |       Causa       | Cantidad por causa | % de causa |
|------------------------------|:----------------:|-------------|:-----------------:|:------------------:|:----------:|
|      Bebés (menor 1 año)     |       42292      |     2,2%    |     Prematuro     |        4890        |     12%    |
|      Niños (1 - 12 años)     |       14306      |     0,8%    |  Tr. Intracraneal |        1305        |     9%     |
|  Adolescentes (12 - 18 años) |       10877      |     0,6%    |      Asfixia      |        1463        |     13%    |
|  Adulto Joven (18 - 30 años) |       50493      |     2,6%    |      Asfixia      |        8231        |     16%    |
|    Adultos (30 - 50 años)    |      166423      |     8,7%    |      Asfixia      |        12192       |     7%     |
| Adulto Maduro (50 - 65 años) |      320091      |    16,8%    | Infarto Miocardio |        25418       |     8%     |
| Adulto Mayor (65 o más años) |      1301776     |    68,3%    | Infarto Miocardio |        91163       |     7%     |


| Región          | Causa 1                                                         | Causa 2                                                                           | Causa 3                                                                           |
|-----------------|-----------------------------------------------------------------|-----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| De Atacama      | Infarto agudo del miocardio                                     | Accidente vascular encefálico agudo, no especificado como hemorrágico o isquémico | Tumor maligno de los bronquios y del pulmón                                       |
| De Valparaíso   | Infarto agudo del miocardio                                     | Neumonía, organismo no especificado                                               | Accidente vascular encefálico agudo, no especificado como hemorrágico o isquémico |
| De Antofagasta  | Tumor maligno de los bronquios y del pulmón                     | Infarto agudo del miocardio                                                       | Neumonía, organismo no especificado                                               |
| De Los Lagos    | Infarto agudo del miocardio                                     | Otras causas mal definidas y las no especificadas de mortalidad                   | Neumonía, organismo no especificado                                               |
| De La Araucanía | Otras causas mal definidas y las no especificadas de mortalidad | Infarto agudo del miocardio                                                       | Neumonía, organismo no especificado                                               |
| Otras           | Infarto agudo del miocardio                                     | Neumonía, organismo no especificado                                               | Tumor maligno del estómago/los bronquios y del pulmón                             |


La principal causa de muerte en Chile es el infarto al miocardio, para todos los años, con excepción del período 1997-2001, en donde fue la neumonía. Los accidentes vasculares y los tumores al pulmón o estómago también aparecen como causas importantes. 

El experimento de obtener las causas de muerte por estado civil se realizó sólo entre personas de 18 y 40 años. La causa de muerte común para todos los estados civiles es la asfixia, con excepción de los viudos que mueren por traumatismo intracraneal y los convivientes que mueren por VIH que resulta en tumores. Cabe destacar que estos son sólo 12 del total del universo analizado.

En el caso de las regiones, las tres causas principales de defunción son el infarto al miocardio, la neumOnía y los tumores al pulmón o al estómago. Tres regiones que resultan ser una interesante excepción son la de Atacama, en donde aparece el accidente vascular encefálico agudo como nueva causa. Lo mismo ocurre en la región de Valparaíso. En el caso de la región de Antofagasta, la principal causa de muerte es la de tumor maligno en los bronquios o pulmón. En las regiones de La Araucanía y de Los Lagos hay una tendencia a no registar o especificar correctamente la causa de muerte de las personas.

En el caso de las comunas de la región metropolitana, en todas la principal causa de muerte es el infarto agudo al miocardio. Se producen excepciones en las comunas de Lampa, Tiltil y San José de Maipo, en donde la princial causa de muerte es la neumonía. 

# Conclusion

La principal conclusión que se puede obtener de los experimentos realizados es que efectivamente, la causa principal de muerte en Chile es el infarto al miocardio. Sin embargo, el experimento de los rangos etarios nos permite concluir que esto se debe a que, como en chile el 68,3% de las muertes que se registran pertenece a gente de más de 65 años, es que los experimentos por región y comuna se ven inflados en muertes que tienen como principal causa el infarto al miocardio gracias a esta proporción. En ese sentido, habría sido interesante haber realizado los experimentos por ubicación geográfica excluyendo a los adultos mayores e incluso a los adultos maduros. Esto sería interesante de calcular si se quisiera realizar una continuación de este trabajo. 

Resulta interesante que las mayores causas de muertes cambien en función de los años. No es mucho lo que se puede concluir en este apartado, puesto que para ello se necesitaría, quizás, una investigación más detallada de los comportamientos de los chilenos durante esos años y, probablemente, durante los anteriores, para poder comprender por qué se producen esas variaciones.

Para el caso del análisis por estado civil, como la causa común es la de asfixia, que coincide curiosamente con ser la causa de mayores muertes en el rango etario de 12 a 50 años, entonces no se puede concluir nada relevante. Queda descartado de que la gente que se encuentra casada la pase peor que la gente que no, debido a que tienen la misma muerte que el común de la gente de la edad analizada.

Volviendo a los experimentos por ubicación geográfica, en el caso de las regiones, se puede afirmar que efectivamente la alta actividad minera en la región de Antofagasta tiene un impacto en la causa de muerte de las personas, puesto que la mayor causa de muerte es la de tumor en los bronquios o pulmón. También habría sido interesante realizar dicho análisis separado por grupos etarios.  Respecto a las regiones más frías y las causas de muerte por enfermedades respiratorias, no se puede concluir que en estos lugares se vean acentuadas, debido a que el top tres de causas es el mismo que en el resto del país, con excepción de las cuatro regiones que se analizaron previamente.

Si se miran las comunas de la Región Metropolitana, con los datos obtenidos, no se puede concluir que las comunas con mayor actividad industrial, como lo son, por ejemplo, Pudahuel, Estación Central, La Pintana o La Cisterna, tengan causas relacionadas a contaminación como las de mayores muertes, puesto que tienen la misma causa de muerte princiapl que otras comunas. Resulta interesante el caso de las tres comunas que son excepcionales en su causa de muerte principal, puesto que pertenecen a las comunas de la RM con menores tempraturas, por lo que se podría sugerir que hay una relación entre la temperatura y la causa de muerte, sin embargo, al contrastarlo con las regiones, en donde esto no pasa incluso para regiones con temperaturas promedio y extremas menores, entonces se puede desechar esta sugerencia.