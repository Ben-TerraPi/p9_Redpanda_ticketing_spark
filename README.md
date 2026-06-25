## Context

Ce projet a été réalisé dans le cadre de mon parcours de formation 'Data Engineer' avec OpenClassrooms.

Titre du projet :

`Gérez des tickets clients avec Redpanda et PySpark`

Ce projet consiste à mettre en place un pipeline de données pour ingérer, traiter et analyser ces tickets en temps réel en utilisant Redpanda et Pyspasrk.

Le but de cet exercice est de simuler l’utilisation de Redpanda dans un conteneur Docker, voici ce qui a été réalisé en local:
- 1 broker Redpanda en mode dev-container (pas de réplication, pas de tolérance aux pannes)
- Producer qui génère des données aléatoires en une seule fois
- Spark dans le même conteneur

![Pipeline](image/pipeline)


## Installations

### Docker

[Docker Desktop](https://www.docker.com/products/docker-desktop/) (windows/mac)
[Docker Engine](https://docs.docker.com/engine/install/) (Linux)



# p9Voici le déroulé complet de docker compose up :
1. Redpanda démarre
redpanda-0 → healthcheck (rpk cluster info) → service_healthy
Docker attend que Redpanda soit réellement prêt à accepter des connexions avant de continuer.
2. Console démarre
redpanda-0 healthy → console démarre
L'interface web Redpanda devient accessible sur localhost:8080.
3. Producer démarre
redpanda-0 healthy → producer démarre → envoie 500 tickets → s'arrête (exit 0)
Le script random_ticket.py génère et envoie les 500 tickets dans le topic client_tickets, puis le conteneur se termine proprement.
4. Spark démarre
producer service_completed_successfully → spark démarre
Docker attend que le producer soit terminé avant de lancer Spark — garantissant que tous les tickets sont dans Redpanda.
5. Spark traite les tickets
spark-submit → lit client_tickets (startingOffsets=earliest) → transformations → agrégations → écrit JSON
Le job Spark :

lit tous les tickets depuis le début du topic
applique la transformation equipe
calcule les agrégations
écrit les fichiers JSON dans /opt/spark/work/output → monté sur ./output sur ton PC

Résultat final dans ./output/ :
output/
├── agg_type_priorite/
├── agg_equipe/
└── agg_demande/
Le pipeline complet en une image :
Docker Compose
     │
     ├── redpanda-0  ←─────────────────────┐
     │      ↓ healthy                      │
     ├── console                           │
     │                                     │
     ├── producer → envoie 500 tickets ────┘
     │      ↓ completed
     └── spark → lit → transforme → agrège → JSON