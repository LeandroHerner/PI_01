import pandas as pd
#import json
#import ast
#import pyarrow as pa
#import pyarrow.parquet as pq
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

# Se instancia la clase a utilizar
app = FastAPI()

# Direccion de la API
# http://127.0.0.1:8000/docs

# Direccion del web service Render
# https://pi-01-api-lh.onrender.com/docs#/





# Se cargan los archivos parquet a utilizar
steam_games = pd.read_parquet('datos_parquet/steam_games.parquet')
users_items = pd.read_parquet('datos_parquet/user_items_chunk.parquet')
user_reviews = pd.read_parquet('datos_parquet/user_reviews.parquet')
user_sim = pd.read_parquet('datos_parquet/user_sim.parquet')
item_recortado = pd.read_parquet('datos_parquet/item_chunk.parquet')





#### PRIMERA CONSULTA (PlayTimeGenre) ####

# Se crean dos variables en las que se almacenan dos dataframes con las columnas a utilizar para la consulta
items_genre = users_items[['id', 'item_name', 'playtime_forever']]
games_genre = steam_games[['id', 'genres', 'year']]

# Se crea un dataframe realizando un merge entre las variables anteriores utilizando la columna 'id' que tienen en comun.
playTimeGenre = pd.merge(items_genre, games_genre, on='id', how='inner')

# Se establece la ruta de la funcion
@app.get("/PlayTimeGenre/{genero}", response_model=dict)

# Se ingresa un genero y devuelve el año que mas horas jugadas tuvo
def PlayTimeGenre(consulta : str):
    
    # Convertir la consulta a minúsculas
    consulta = consulta.lower()
    
    # Se filtra el DataFrame con la consulta
    genero = playTimeGenre[playTimeGenre['genres'] == consulta]

    # Se verifica si hay datos para el género dado
    if not genero.empty:
        
        # Se agrupa por año y suma las horas jugadas
        resultados = genero.groupby('year')['playtime_forever'].sum().reset_index()

        # Se encuentra el año con más horas jugadas
        if not resultados.empty:
            año_max_horas = resultados.loc[resultados['playtime_forever'].idxmax()]

            # Devuelve el resultado como un diccionario
            return {"genero": consulta, "año_max_horas": {"año": int(año_max_horas['year'])}}
        else:
            return {"mensaje": f"No hay datos disponibles para el género {consulta} en ningún año."}
    else:
        return {"mensaje": f"No hay datos disponibles para el género {consulta} o el género no existe."}





#### SEGUNDA CONSULTA (UserForGenre) #### 

# Se definen clases que van a ser utiles a la segunda funcion
class UsuarioMasJugador(BaseModel):
    nombre: str
    horas_acumuladas: int

class AcumulacionPorAño(BaseModel):
    año: int
    horas_acumuladas: int

class ConsultaResultado(BaseModel):
    usuario_mas_jugado: UsuarioMasJugador
    acumulacion_por_año: List[AcumulacionPorAño]
    

# Se crean dos variables en las que se almacenan dos dataframes con las columnas a utilizar para la consulta
items_UserForGenre = users_items[['id', 'user_id', 'playtime_forever']]

games_UserForGenre = steam_games[['id', 'genres', 'year']]

# Se crea un dataframe realizando un merge entre las variables anteriores utilizando la columna 'id' que tienen en comun.
UserForGenre = pd.merge(items_UserForGenre, games_UserForGenre, on='id', how='inner')

# Se establece la ruta de la funcion
@app.get("/UserForGenre/{genero}", response_model=ConsultaResultado)

def User_For_Genre(consulta : str):
    
    # Convertir la consulta a minúsculas
    consulta = consulta.lower()

    #Filtro el dataFrame con la consulta
    genero = UserForGenre[UserForGenre['genres'] == consulta]

    # Agrupa por usuario y año y suma las horas jugadas
    resultados_usuario = genero.groupby('user_id')['playtime_forever'].sum().reset_index()
    usuario_mas_jugado = resultados_usuario.loc[resultados_usuario['playtime_forever'].idxmax()]

    # Agrupa por año y suma las horas jugadas
    resultados_año = genero.groupby('year')['playtime_forever'].sum().reset_index()

    # Crea las instancias de modelos Pydantic para la respuesta
    usuario_resultado = UsuarioMasJugador(nombre=str(usuario_mas_jugado['user_id']), horas_acumuladas=int(usuario_mas_jugado['playtime_forever']))
    acumulacion_por_año_resultado = [AcumulacionPorAño(año=int(row['year']), horas_acumuladas=int(row['playtime_forever'])) for index, row in resultados_año.iterrows()]

    # Devuelve el resultado como un diccionario
    return ConsultaResultado(usuario_mas_jugado=usuario_resultado, acumulacion_por_año=acumulacion_por_año_resultado)





#### TERCERA CONSULTA (UsersRecommend) ####

# Se almacena en un dataframe las columnas a utilizar para la consulta
games_UsersRecommend = steam_games[['id', 'title']]

UsersRecommend = pd.merge(user_reviews, games_UsersRecommend, on='id', how='inner')

# Definir la ruta en FastAPI
@app.get("/UsersRecommend/{year}")


def Users_Recommend(year : int):
    

    if 2010 <= year <= 2015:
        # filtro por año en primera instancia
        UsersRecommend_filtrado_anio = UsersRecommend[UsersRecommend['review_year'] == year]

        # luego filtro contemplando solo las filas que cumplen con ambos booleanos:
        # ser True en la columna 'recommend' y tener valor 1 o 2 en la columna 'sentiment_analysis'
        UsersRecommend_filtrado = UsersRecommend_filtrado_anio[(UsersRecommend_filtrado_anio['recommend'] == True) & (UsersRecommend_filtrado_anio['sentiment_analysis'].isin([1, 2]))]

        # Agrupar por id de juego y sumar las recomendaciones
        top_games = UsersRecommend_filtrado.groupby('id').agg({'title': 'first', 'recommend': 'sum'}).reset_index()

        # Ordenar por el número de recomendaciones en orden descendente
        top_games_sorted = top_games.sort_values(by='recommend', ascending=False)

        top3Games = top_games_sorted.head(3)

        resultado = [{"Top 1": {top3Games.iloc[0]['title']}}, {"Top 2": {top3Games.iloc[1]['title']}}, {"Top 3": {top3Games.iloc[2]['title']}}]
        
        return resultado
    else:
        return "El año ingresado está fuera del periodo estudiado o es invalido"





#### CUARTA CONSULTA (UsersNotRecommend) ####

# Definir la ruta en FastAPI
@app.get("/UsersNotRecommend/{year}")

def Users_Not_Recommend(year : int):
    
    if 2010 <= year <= 2015:
        # filtro por año en primera instancia
        UsersRecommend_filtrado_anio = UsersRecommend[UsersRecommend['review_year'] == year]

        # luego filtro contemplando solo las filas que cumplen con ambos booleanos:
        # ser True en la columna 'recommend' y tener valor 1 o 2 en la columna 'sentiment_analysis'
        UsersRecommend_filtrado = UsersRecommend_filtrado_anio[(UsersRecommend_filtrado_anio['recommend'] == False) & (UsersRecommend_filtrado_anio['sentiment_analysis'].isin([0]))]

        # Agrupar por id de juego y sumar las recomendaciones
        top_games = UsersRecommend_filtrado.groupby('id').agg({'title': 'first', 'recommend': 'sum'}).reset_index()

        # Ordenar por el número de recomendaciones en orden descendente
        top_games_sorted = top_games.sort_values(by='recommend', ascending=True)

        top3Games = top_games_sorted.head(3)

        resultado = [{"Top 1": {top3Games.iloc[0]['title']}}, {"Top 2": {top3Games.iloc[1]['title']}}, {"Top 3": {top3Games.iloc[2]['title']}}]
        
        return resultado
    else:
        return "El año ingresado está fuera del periodo estudiado o es invalido"





#### QUINTA CONSULTA (sentiment_analysis) ####

games_sentiment = steam_games[['id', 'title', 'year']]

user_sentiment = user_reviews[['id', 'user_id', 'sentiment_analysis' ]]

sentiment = pd.merge(user_sentiment, games_sentiment, on='id', how='inner')


@app.get("/sentiment_analysis/{year}")
def sentiment_analysis(year: int):
    
    # Filtrar por año específico
    df_filtrado = sentiment[sentiment['year'] == year]

    # Se verifica si hay datos después del filtrado
    if df_filtrado.empty:
        return {"No hay datos para el año dado."}
    
    # Se filtra de acuerdo al periodo 2010/2015
    elif 2010 <= year <= 2015:
        # Contar la cantidad de registros por categoría de análisis de sentimiento
        cantidad_sentimientos = df_filtrado['sentiment_analysis'].value_counts().to_dict()
        
        # Mapeo los valores a los nombres deseados
        cantidad_sentimientos_nombres = {
            "Positivo": cantidad_sentimientos.get(2, 0),
            "Neutral": cantidad_sentimientos.get(1, 0),
            "Negativo": cantidad_sentimientos.get(0, 0)}

        # Devolver el resultado como un diccionario
        return {"sentiment_analysis": cantidad_sentimientos_nombres}
    else:
        return "El año ingresado es invalido"





#### SEXTA CONSULTA (recomendacion_juego) ####

@app.get("/recomendacion_juego/{item_id}")
def modelo_de_recomendacion(item_id : int):
    
    if item_id not in item_recortado['item_id'].values:
        return f"Error: El ID de juego {item_id} no se encuentra en el DataFrame."
    
    # Verificar si hay suficientes datos para calcular recomendaciones
    elif len(user_sim) <= item_id:
        return "Error: No hay suficientes datos para calcular recomendaciones."
    
    else:
            # Obtener el índice del juego en la matriz
        id_to_name = item_recortado.loc[item_recortado['item_id'] == item_id, 'item_name']
        
        item_name = id_to_name.values[0]
    
        encabezado = f'Juegos similares a {item_name}:'
        
        user_sim_val = user_sim.sort_values(by=item_name, ascending=False).index[1:6]
        return encabezado, [i for i in user_sim_val]