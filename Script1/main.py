import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Configurações do banco
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'csvhandlerdb',
    'user': 'admin',
    'password': 'admin'
}

CSV_FILE = 'dados.csv'
TABLE_NAME = 'bloqueio'
CHUNK_SIZE = 500_000

def process_chunk(chunk):
    chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    # Converte tipos
    chunk['data_arquivo'] = pd.to_datetime(chunk['data_arquivo'], errors='coerce')
    chunk['telefone'] = pd.to_numeric(chunk['telefone'], errors='coerce')
    chunk['cpf'] = chunk['cpf'].astype(str).fillna('')

    chunk = chunk.dropna(subset=['telefone', 'data_arquivo'])

    return [tuple(x) for x in chunk.to_numpy()]

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    columns = "origem, telefone, data_arquivo, empresa, cpf"
    sql = f"""
    INSERT INTO {TABLE_NAME} (origem, telefone, data_arquivo, empresa, cpf)
    VALUES %s
    ON CONFLICT (telefone) DO NOTHING
    """

    total_inserted = 0
    try:
        for chunk in pd.read_csv(CSV_FILE, sep=';', chunksize=CHUNK_SIZE):
            data_tuples = process_chunk(chunk)
            if data_tuples:
                execute_values(cursor, sql, data_tuples)
                conn.commit()
                total_inserted += len(data_tuples)
                print(f"{total_inserted} registros inseridos...")
        print("Inserção completa.")
    except Exception as e:
        conn.rollback()
        print("Erro ao inserir dados:", e)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()