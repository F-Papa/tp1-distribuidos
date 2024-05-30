"""
dataset-reducer.py

Descripción:
    Este script lee archivos de resultados esperados del dataset original, y en base a eso crea un dataset reducido,
    que aunque no garantiza para todas las queries dar el mismo resultado, se puede usar para hacer pruebas en menos
    tiempo y revisar que siempre se obtengan los mismos resultados con el dataset reducido.

Uso:
    *** IMPORTANTE ***
    Ejecute este script desde el directorio raíz del proyecto.
    > python3 scripts/dataset-reducer.py

    Nota:
    La constante BULK es un indicador del tamaño final del dataset reducido. Se puede aumentar/disminuir para obtener tamaños
    y por lo tanto tiempos de ejecución distintos y hacer distintas pruebas.
    Los resultados de las queries 2 y 5 resultan distintos para datasets creados con distinto BULK.
    BULK = 0 para max-velocidad que aún conserve results para query1,3,5
    BULK = 300 para ejecuciones muy rápidas
    BULK = 1000 para ejecuciones rápidas
    BULK = 3500 para más lento
    

Archivos Necesarios:
    - expected_results/query1.txt: Resultados con el dataset original de la consulta 1 en formato de texto.
    - expected_results/query2.txt: Resultados con el dataset original de la consulta 2 en formato de texto.
    - expected_results/query3.txt: Resultados con el dataset original de la consulta 3 en formato de texto.
    - expected_results/query4.txt: Resultados con el dataset original de la consulta 4 en formato de texto.
    - expected_results/query5.txt: Resultados con el dataset original de la consulta 5 en formato de texto.
    - data/books_data.csv: Dataset ORIGINAL de libros en formato CSV.
    - data/Books_rating.csv: Dataset ORIGINAL de reviews de libros en formato CSV.

Salida:
    Se generarán dos archivos CSV en el directorio data:
    - books_data_reduced.csv: Dataset reducido de libros.
    - Books_rating_reduced.csv: Dataset reducido de reviews de libros.
"""

import pandas as pd
import json
import ast

BULK = 1000

print("Consultando resultados dataset original")

query1_titles = []
with open('expected_results/query1.txt', 'r', encoding='utf-8') as file:
    for line in file:
        book_info = json.loads(line)
        query1_titles.append(book_info['title'])

#print("Títulos leídos de query1.txt:", query1_titles)

query3_titles = []
with open('expected_results/query3.txt', 'r', encoding='utf-8') as file:
    for line in file:
        book_info = json.loads(line)
        query3_titles.append(book_info['title'])


#print("Títulos leídos de query3.txt:", query3_titles)

with open('expected_results/query4.txt', 'r', encoding='utf-8') as file:
    query4_titles = [json.loads(line)['title'] for line in file]

#print("Títulos leídos de query4.txt:", query4_titles)

with open('expected_results/query5.txt', 'r', encoding='utf-8') as file:
    query5_titles = [json.loads(line)['title'] for line in file]

#print("Títulos leídos de query5.txt:", query5_titles)

all_query_titles = set(query1_titles + query3_titles + query4_titles + query5_titles)

with open('expected_results/query2.txt', 'r', encoding='utf-8') as file:
    query2_authors = [line.strip().strip('"') for line in file]
    #if BULK is None:
    #    slice_size = len(query2_authors)
    #else:
    #    slice_size = BULK // 10
    #query2_authors = query2_authors[:slice_size]
query2_authors_set = set(query2_authors)
#print(len(query2_authors))
#print("autores", query2_authors_set)

print("Armando dataset reducido")

books_data = pd.read_csv('data/books_data.csv')
books_data.dropna(subset=['authors'], inplace=True)
books_data['authors'] = books_data['authors'].apply(ast.literal_eval)

#print(books_data.loc[10, 'authors'])
#print(type(books_data.loc[10, 'authors']))

books_ratings = pd.read_csv('data/Books_rating.csv')

filtered_books_title = books_data[books_data['Title'].isin(all_query_titles)]
#print(len(filtered_books_title))

def contiene_autor(autores):
    for autor in autores:
        if autor in query2_authors_set:
            return True
    return False

filtered_books_authors = books_data[books_data['authors'].apply(contiene_autor)]

print("b", len(filtered_books_authors))

combined_filtered_books = pd.concat([filtered_books_title, filtered_books_authors])
#print(len(combined_filtered_books))
combined_filtered_books.drop_duplicates(subset=['Title'], inplace=True)
#print(len(combined_filtered_books))
combined_filtered_books = combined_filtered_books.head(len(filtered_books_title) + BULK)
#print("final", len(combined_filtered_books))

combined_filtered_books.to_csv('data/books_data_reduced.csv', index=False)

books_ratings_reduced = books_ratings[books_ratings['Title'].isin(combined_filtered_books['Title'])]

books_ratings_reduced.to_csv('data/Books_rating_reduced.csv', index=False)

print("Archivos reducidos guardados exitosamente.")