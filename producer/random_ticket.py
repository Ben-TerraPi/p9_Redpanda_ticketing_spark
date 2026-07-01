import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer


demandes = ["devis", "facture", "remboursement", "question", "contact"]
type_demandes = ["commerciale", "SAV"]
priorites = ["low", "medium", "high"]


# def gen_datetime(min_year=2010, max_year=datetime.now().year):
#     start = datetime(min_year, 1, 1, 00, 00, 00)
#     years = max_year - min_year + 1
#     end = start + timedelta(days=365 * years)
#     return start + (end - start) * random.random()


def gen_ticket(i):
    id_ticket = f"id_tk{i}"
    id_client = f"id_cl{random.randint(1000,9999)}"
    date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S") #  plus besoin de générer date aléatoire car plus de batch
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
    acks="all", # attends l’accusé de réception du broker avant de considérer l’envoi comme réussi
    value_serializer=lambda v: json.dumps(v).encode("utf-8") # sérialise le ticket avec value_serializer
)

for i in range(1, 501):
    ticket = gen_ticket(i)
    producer.send("client_tickets", value=ticket) # envoi asynchrone, non bloquant dans la plupart des cas
    print(f"ticket {i} envoyé")
    time.sleep(0.5)

producer.flush() # blocage final pour garantir que tout est bien parti
producer.close()

print(f" 500 tickets envoyés dans le topic 'client_tickets'")