import pandas as pd
import json
import ast
import warnings
warnings.filterwarnings('ignore')
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

# Se instancia la clase a utilizar
app = FastAPI()

# Se cargan los archivos parquet a utilizar
steam_games = pd.read_parquet('datos_parquet/steam_games.parquet')
users_items = pd.read_parquet('datos_parquet/user_items.parquet')
user_reviews = pd.read_parquet('datos_parquet/user_reviews.parquet')

#### PRIMERA CONSULTA ####

# Creo dos variables en las que se almacenan dos dataframes con las columnas a utilizar para la consulta

items_genre = users_items[['id', 'item_name', 'playtime_forever']]

games_genre = steam_games[['id', 'genres', 'year']]

# Se crea un dataframe realizando un merge entre las variables anteriores utilizando la columna 'id' que tienen en comun.
playTimeGenre = pd.merge(items_genre, games_genre, on='id', how='inner')

def obtener_año_max_horas(consulta):
    # Filtro el DataFrame con la consulta
    genero = playTimeGenre[playTimeGenre['genres'] == consulta]

    # Agrupo por año y suma las horas jugadas
    resultados = genero.groupby('year')['playtime_forever'].sum().reset_index()

    # Encuentra el año con más horas jugadas
    año_max_horas = resultados.loc[resultados['playtime_forever'].idxmax()]

    # Devuelve el resultado como un diccionario
    return {"genero": consulta, "año_max_horas": {"año": int(año_max_horas['year'])}} #NECESITO LA RESPUESTA EN FORMATO STR
    
    #return {f"Para el género {consulta}, el año con más horas jugadas es {int(año_max_horas['year'])}"}
    
    #return ConsultaResultado(mensaje=retorno)


# Primer consulta de la API
@app.get("/PlayTimeGenre/{genero}", response_model=dict)

def consulta_año_max_horas(genero: str):
    retorno = obtener_año_max_horas(genero)
    return retorno


#### SEGUNDA CONSULTA #### 

class UsuarioMasJugador(BaseModel):
    nombre: str
    horas_acumuladas: int

class AcumulacionPorAño(BaseModel):
    año: int
    horas_acumuladas: int

class ConsultaResultado(BaseModel):
    usuario_mas_jugado: UsuarioMasJugador
    acumulacion_por_año: List[AcumulacionPorAño]


items_UserForGenre = users_items[['id', 'user_id', 'playtime_forever']]

games_UserForGenre = steam_games[['id', 'genres', 'year']]

UserForGenre = pd.merge(items_UserForGenre, games_UserForGenre, on='id', how='inner')



def obtener_año_y_usuario_max_horas(consulta):

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



# Segunda consulta de la API

@app.get("/UserForGenre/{genero}", response_model=ConsultaResultado)
def consulta_año_y_usuario_max_horas(genero: str):
    resultado = obtener_año_y_usuario_max_horas(genero)
    return resultado




#### TERCERA CONSULTA ####

games_UsersRecommend = steam_games[['id', 'title']]

UsersRecommend = pd.merge(user_reviews, games_UsersRecommend, on='id', how='inner')

def obtener_top3_positivo(consulta):
    
    # filtro por año en primera instancia
    UsersRecommend_filtrado_anio = UsersRecommend[UsersRecommend['review_year'] == consulta]

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
    # Devolver el resultado como un diccionario
    #return {"top3": top3Games[['id', 'title', 'recommend']].to_dict(orient='records')}

# Definir la ruta en FastAPI
@app.get("/UsersRecommend/{consulta}")
def obtener_top3(consulta: int):
    resultado = obtener_top3_positivo(consulta)
    return resultado



#### CUARTA CONSULTA ####

def obtener_top3_negativo(consulta):
    
    # filtro por año en primera instancia
    UsersRecommend_filtrado_anio = UsersRecommend[UsersRecommend['review_year'] == consulta]

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

# Definir la ruta en FastAPI
@app.get("/UsersNotRecommend/{consulta}")
def obtener_top3(consulta: int):
    resultado = obtener_top3_negativo(consulta)
    return resultado


#### QUINTA CONSULTA ####

games_sentiment = steam_games[['id', 'title', 'year']]

user_sentiment = user_reviews[['id', 'user_id', 'sentiment_analysis' ]]

sentiment = pd.merge(user_sentiment, games_sentiment, on='id', how='inner')


@app.get("/sentiment_analysis/")
def obtener_sentimiento_analisis_por_año(year: int):
    # Filtrar por año específico
    df_filtrado = sentiment[sentiment['year'] == year]

    # Verificar si hay datos después del filtrado
    if df_filtrado.empty:
        return {"message": "No hay datos para el año dado."}

    # Contar la cantidad de registros por categoría de análisis de sentimiento
    cantidad_sentimientos = df_filtrado['sentiment_analysis'].value_counts().to_dict()
    
    # Mapeo los valores a los nombres deseados
    cantidad_sentimientos_nombres = {
        "Positivo": cantidad_sentimientos.get(2, 0),
        "Neutral": cantidad_sentimientos.get(1, 0),
        "Negativo": cantidad_sentimientos.get(0, 0)}

    # Devolver el resultado como un diccionario
    return {"sentiment_analysis": cantidad_sentimientos_nombres}
