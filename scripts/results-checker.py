"""
results-checker.py

Descripción:
    Este script compara los resultados obtenidos por el cliente con los resultados esperados,
    ya sea los resultados originales o los resultados reducidos. 

Uso:
    *** IMPORTANTE ***
    Ejecute este script desde el directorio raíz del proyecto.
    > python3 scripts/results-checker.py [-r | --reduced]

Opciones:
    -r, --reduced: Comparar los resultados del cliente con los resultados esperados reducidos.
        Si se proporciona esta opción, el script buscará los archivos de resultados reducidos
        en el directorio expected_results con el formato queryi_red.txt. 
        Si no se proporciona esta opción, el script buscará los archivos de resultados originales
        en el directorio expected_results con el formato queryi.txt.

Archivos Necesarios:
    - Archivos de resultados del cliente: Se esperan archivos de resultados del cliente en el directorio
    src/client/results con el formato query_i.txt, donde i va desde 1 hasta 5.
    - Archivos de resultados esperados:
        - Si la opción --reduced no está presente:
            - Archivos de resultados esperados originales: Se esperan archivos de resultados esperados
            en el directorio expected_results con el formato queryi.txt, donde i va desde 1 hasta 5.
        - Si la opción --reduced está presente:
            - Archivos de resultados esperados reducidos: Se esperan archivos de resultados esperados
            reducidos en el directorio expected_results con el formato queryi_red.txt, donde i va desde 1 hasta 5.

Salida:
    El script mostrará por consola las diferencias entre los resultados del cliente
    y los resultados esperados para cada par de archivos comparados.
"""
import argparse
import difflib

def compare_files(result_file, expected_file):
    with open(result_file, 'r', encoding='utf-8') as file1, \
         open(expected_file, 'r', encoding='utf-8') as file2:
        
        diff = difflib.unified_diff(file1.readlines(), file2.readlines(), 
                                     fromfile=result_file, tofile=expected_file, 
                                     lineterm='', n=0)
        
        for line in diff:
            print(line)

def main(check_against_reduced):
    for i in range(1, 6):
        if check_against_reduced:
            result_file = f"src/client/results/query_{i}.txt"
            expected_file = f"expected_results/query{i}_red.txt"
        else:
            result_file = f"src/client/results/query_{i}.txt"
            expected_file = f"expected_results/query{i}.txt"
        
        print(f"Diferencias entre {result_file} y {expected_file}:")
        compare_files(result_file, expected_file)
        print()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script para comparar archivos de resultados.")
    parser.add_argument("-r", "--reduced", action="store_true", help="Comparar los resultados del cliente con los resultados esperados reducidos.")
    args = parser.parse_args()

    main(args.reduced)