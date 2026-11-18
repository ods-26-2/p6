import sqlite3

def setup_banco():
    # Conecta ao seu banco local do P6
    conn = sqlite3.connect("p6_metadata.db")
    cursor = conn.cursor()

    # Cria a tabela exata que a arquitetura pede
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS scenarios (
        recording_id TEXT PRIMARY KEY,
        description TEXT NOT NULL,
        expected_outcome TEXT NOT NULL,
        file_path TEXT NOT NULL
    )
    ''')

    # Insere o seu primeiro cenário de validação
    cursor.execute('''
    INSERT OR IGNORE INTO scenarios (recording_id, description, expected_outcome, file_path)
    VALUES (
        'rec-001',
        'Pessoa não autorizada entra na zona restrita (cam0)',
        'Espera-se ods.s2.event de invasao e ods.s3.alert ALTO',
        'videos/rec-001.mp4'
    )
    ''')
    
    conn.commit()
    conn.close()
    print("Banco de cenários pronto e populado!")

if __name__ == "__main__":
    setup_banco()