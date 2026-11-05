"""
JobCompass - Producteur Kafka des interactions candidat <-> offre.

Exemples :
  python producer_interactions.py --dry-run                     # test sans Kafka
  python producer_interactions.py --mode history --events 200000  # historique (rapide)
  python producer_interactions.py --mode live --rate 20           # flux temps réel
"""
import argparse
import json
import os
import time

from generator import InteractionGenerator


def main():
    p = argparse.ArgumentParser(description="Producteur Kafka des interactions JobCompass")
    p.add_argument("--mode", choices=["history", "live"], default="live")
    p.add_argument("--events", type=int, default=200000,
                   help="nombre d'événements à générer en mode history")
    p.add_argument("--rate", type=float, default=None,
                   help="événements/seconde (0 = au plus vite). "
                        "Par défaut : 0 en history, EVENTS_PER_SEC (20) en live")
    p.add_argument("--topic", default=os.getenv("TOPIC_INTERACTIONS", "candidate-interactions"))
    p.add_argument("--dry-run", action="store_true",
                   help="affiche 10 événements au lieu de les envoyer à Kafka")
    args = p.parse_args()

    if args.rate is None:
        args.rate = 0 if args.mode == "history" else float(os.getenv("EVENTS_PER_SEC", 20))

    gen = InteractionGenerator(n_candidates=int(os.getenv("N_CANDIDATES", 20000)),
                               seed=int(os.getenv("SEED", 42)))
    events = gen.history(args.events) if args.mode == "history" else gen.live()

    if args.dry_run:
        for i, ev in enumerate(events):
            print(json.dumps(ev))
            if i >= 9:
                break
        return

    from confluent_kafka import Producer  # importé ici : le dry-run marche sans Kafka

    errors = 0

    def on_delivery(err, _msg):
        nonlocal errors
        if err is not None:
            errors += 1
            print(f"Erreur de livraison : {err}", flush=True)

    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        "acks": "all",
        "linger.ms": 50,
    })
    delay = 1 / args.rate if args.rate > 0 else 0
    print(f"Envoi vers le topic '{args.topic}' (mode {args.mode})", flush=True)

    for i, ev in enumerate(events, 1):
        while True:
            try:
                producer.produce(args.topic, key=ev["candidate_id"],
                                 value=json.dumps(ev).encode("utf-8"),
                                 on_delivery=on_delivery)
                break
            except BufferError:          # file d'attente locale pleine : on vide un peu
                producer.poll(1)
        producer.poll(0)
        if i % 5000 == 0:
            print(f"{i} événements envoyés", flush=True)
        if delay:
            time.sleep(delay)

    producer.flush()
    print(f"Terminé. Erreurs de livraison : {errors}", flush=True)


if __name__ == "__main__":
    main()
