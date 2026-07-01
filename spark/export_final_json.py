import json
import psycopg2

connection = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname="tickets_db",
    user="root",
    password="root"
)

cursor = connection.cursor()

cursor.execute("SELECT equipe, count FROM agg_equipe ORDER BY equipe")
agg_equipe = [{"equipe": equipe, "count": count} for equipe, count in cursor.fetchall()]

cursor.execute("SELECT type_demande, priorite, count FROM agg_type_priorite ORDER BY type_demande, priorite")
agg_type_priorite = [
    {"type_demande": type_demande, "priorite": priorite, "count": count}
    for type_demande, priorite, count in cursor.fetchall()
]

with open("/opt/spark/work/output/final_agg_equipe.json", "w", encoding="utf-8") as f:
    json.dump(agg_equipe, f, ensure_ascii=False, indent=2)

with open("/opt/spark/work/output/final_agg_type_priorite.json", "w", encoding="utf-8") as f:
    json.dump(agg_type_priorite, f, ensure_ascii=False, indent=2)

cursor.close()
connection.close()