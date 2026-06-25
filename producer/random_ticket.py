import random
import json
from datetime import datetime, timedelta
from kafka import KafkaProducer


demandes = ["devis", "facture", "remboursement", "question", "contact"]
type_demandes = ["commerciale", "SAV"]
priorites = ["low", "medium", "high"]
            

def gen_datetime(min_year=2010, max_year=datetime.now().year):
    start = datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    return start + (end - start) * random.random()


def gen_ticket():
    id_ticket = f"id_tk{random.randint(1000,9999)}"
    id_client = f"id_cl{random.randint(1000,9999)}"
    date_time = gen_datetime().strftime("%Y-%m-%d %H:%M:%S")
    demande = random.choice(demandes)
    type_demande = random.choice(type_demandes)
    priorite = random.choice(priorites)
    return {
    "id_ticket": id_ticket,
    "id_client": id_client,
    "date_time": date_time,
    "demande": demande,
    "type_demande": type_demande,
    "priorite": priorite
}

producer = KafkaProducer( 
    bootstrap_servers="redpanda-0:9092", # adresse interne Docker
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for _ in range(500):
    ticket = gen_ticket()
    producer.send("client_tickets", value=ticket)

producer.flush()
producer.close()

print("500 tickets envoyés dans le topic 'client_tickets'")