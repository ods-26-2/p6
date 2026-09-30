import sqlite3

conn = sqlite3.connect('p6_metadata.db')
cursor = conn.cursor()

# Consulta os 5 primeiros e os 5 últimos quadros gravados
cursor.execute("SELECT * FROM frame_timestamps ORDER BY frame_index ASC LIMIT 5")
primeiros = cursor.fetchall()

print("--- Primeiros 5 quadros guardados ---")
for linha in primeiros:
    print(f"Quadro: {linha[1]} | Timestamp: {linha[2]} | Sessão: {linha[0].split('-')[0]}")

cursor.execute("SELECT COUNT(*) FROM frame_timestamps")
total = cursor.fetchone()[0]
print(f"\nTotal de quadros sincronizados no banco: {total}")

conn.close()