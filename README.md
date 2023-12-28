# PROYECTO INDIVIDUAL Nº1:

## Machine Learning Operations (MLOps)

![Logo de Mi Proyecto](steam.1698425948.767.webp)

## Introducción

Este proyecto, propuesto por SoyHenry, constituye la primera fase de Labs, en la cual se simula desempeñar el papel de un MLOps Engineer para Steam, una plataforma de distribución digital de videojuegos y programas diversos. En este contexto, se emplea un paquete de datos con el objetivo de desarrollar un Producto Mínimo Viable (MVP). Este MVP se logrará a través de la implementación de un servicio web y la aplicación de un modelo de recomendación mediante Machine Learning, con la expectativa de crear endpoints específicos para su funcionalidad.

## Desarrollo

El paquete de datos proporcionado consta de 3 archivos tipo JSON.

- **output_steam_games.json:**  este dataset esta compuesto por datos relacionados a los juegos o programas disponibles en la plataforma, desarrollador, titulo, fecha de lanzamiento, id, precio, etc.

- **australian_user_reviews.json:**  este dataset esta compuesto por datos relacionados al feedback de los usuarios a los items consumidos, id de usuario, url del usuario y del item, reseñas, si recomienda o no, etc.

- **australian_users_items.json:** este dataset esta compuesto por datos relacionados a los items que consume cada usuario, cuantos items poseen, tiempo de uso acumulado, etc. 


Este conjunto de datos fue sometido a un proceso de Extracción, Transformación y Carga (**ETL**). Debido a la complejidad inherente de los datos proporcionados, se implementaron varias técnicas para desanidar datos, eliminar valores nulos y duplicados, así como suprimir columnas que no contribuían al desarrollo de la API. En algunos casos, la eliminación de ciertas columnas se llevó a cabo para mitigar problemas potenciales de rendimiento y almacenamiento durante la implementación de la API, especialmente cuando su volumen era considerable.


Después de llevar a cabo el proceso de Extracción, Transformación y Carga (ETL), se procedió con un **Análisis Exploratorio de Datos (EDA)** para comprender la distribución de los datos resultantes. Este análisis tuvo como objetivo identificar patrones y tendencias que posibilitaran la detección de outliers y otros insights relevantes en la naturaleza de los datos.

Una vez finalizado el EDA, se paso a resolver los endpoints propuestos para el MVP.

- **PlayTimeGenre:** Esta función debe devolver el usuario que acumula la mayor cantidad de horas jugadas para el genero proporcionado como input.

- **UserForGenre:** Esta función debe devolver el usuario que acumula mayor cantidad de horas jugadas para el año proporcionado como input.

- **UsersRecommend:** Esta función debe devolver los 3 juegos **más** recomendados por usuarios para el año proporcionado como input.

- **UsersNotRecommend:** Esta función debe devolver los 3 juegos **menos** recomendados por usuarios para el año proporcionado como input.

- **Sentiment_Analysis:** Esta funcion debe devolver una lista con la cantidad de registros de reseñas de usuarios que se encuentran categorizados por un analisis de sentimiento para el año de lanzamiento del item proporcionado como input.

- **Recomendacion_Juego:** Esta función debe devolver una lista con los 5 items mas similares al id del item proporcionado como input.


## Modelo de machine learning

Para desarrollar el sistema de recomendación mediante aprendizaje no supervisado, se optó por utilizar la **similitud del coseno**. Esta métrica permite evaluar cuán similares son dos conjuntos de datos o elementos al calcular el coseno del ángulo entre los vectores que los representan.

Este enfoque ha permitido establecer una relación **item-item**, creando una conexión entre juegos. Se toma un juego como referencia y, basándose en cuán similar es ese juego con el resto de los juegos, se generan recomendaciones de títulos similares. Este enfoque de recomendación item-item permite ofrecer sugerencias relevantes basadas en similitudes entre los elementos del conjunto de datos.


## Deployment

El deployment de la API se llevó a cabo mediante el servicio web proporcionado por la plataforma Render, aprovechando la capacidad de consumir los archivos directamente desde este repositorio alojado en Github.

Este proceso implicó tener en cuenta diversas consideraciones, como el límite de espacio en Render al utilizar un servicio gratuito. Se optimizó el archivo main.py compuesto por las funciones, se comprimieron los conjuntos de datos en formato parquet, se excluyó la carga de archivos no esenciales para el deploy y se garantizó la compatibilidad de las bibliotecas utilizadas con las disponibles en Render. Estas medidas se tomaron con el objetivo de asegurar un despliegue eficiente y exitoso de la API en este entorno específico.