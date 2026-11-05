"""
JobCompass - Producteur Kafka des offres d'emploi (rejoue le dataset Djinni
dans l'ordre chronologique de publication).

Exemples :
  python producer_offers.py --dry-run
  python producer_offers.py --limit 5000 --rate 10
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone

from generator import load_jobs


def to_event(row):
    return {
        "job_id": row["id"],
        "position": row["Position"],
        "company": row["Company Name"],
        "primary_keyword": row["Primary Keyword"],
        "exp_years": row["Exp Years"],
        "english_level": row["English Level"],
        "published": row["published_ts"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "description": row["Long Description"],
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def main():
    p = argparse.ArgumentParser(description="Producteur Kafka des offres JobCompass")
    p.add_argument("--limit", type=int, default=0, help="nombre max d'offres (0 = toutes)")
    p.add_argument("--rate", type=float, default=float(os.getenv("OFFERS_PER_SEC", 5)),
                   help="offres/seconde (0 = au plus vite)")
    p.add_argument("--topic", default=os.getenv("TOPIC_OFFERS", "job-offers"))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    jobs = load_jobs()
    if args.limit:
        jobs = jobs.head(args.limit)

    # Les valeurs manquantes (NaN) deviennent null en JSON
    jobs = jobs.astype(object).where(jobs.notna(), None)

    if args.dry_run:
        for _, row in jobs.head(3).iterrows():
            ev = to_event(row)
            ev["description"] = (ev["description"] or "")[:120] + "..."
            print(json.dumps(ev, ensure_ascii=False))
        return

    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                         "acks": "all", "linger.ms": 50})
    delay = 1 / args.rate if args.rate > 0 else 0
    print(f"Envoi de {len(jobs)} offres vers '{args.topic}'", flush=True)

    for i, (_, row) in enumerate(jobs.iterrows(), 1):
        ev = to_event(row)
        while True:
            try:
                producer.produce(args.topic, key=ev["job_id"],
                                 value=json.dumps(ev, ensure_ascii=False).encode("utf-8"))
                break
            except BufferError:
                producer.poll(1)
        producer.poll(0)
        if i % 1000 == 0:
            print(f"{i} offres envoyées", flush=True)
        if delay:
            time.sleep(delay)

    producer.flush()
    print("Terminé.", flush=True)


if __name__ == "__main__":
    main()
