"""
JobCompass - Générateur d'interactions candidat <-> offre (données simulées).

Le dataset Djinni ne contient pas d'interactions. Ce module les simule à partir
des champs communs aux offres et aux candidats :
  - Primary Keyword  (domaine : Java, Python, DevOps...)
  - English Level    (niveau d'anglais demandé / possédé)
  - Exp Years / Experience Years (expérience demandée / possédée)

Règles (à documenter dans docs/donnees.md) :
  1. Un candidat choisit surtout des offres de son domaine (p_same_keyword = 0.8).
  2. Activité et popularité inégales (loi de Pareto) : quelques candidats très
     actifs et quelques offres très consultées, comme sur une vraie plateforme.
  3. Score d'affinité = 0.5 * même domaine + 0.25 * expérience suffisante
     + 0.25 * anglais suffisant + bruit gaussien.
  4. Plus le score est élevé, plus l'événement est fort : view (1) < save (2) < apply (3).
  5. Cohérence temporelle : une interaction a lieu APRÈS la publication de l'offre.
  6. Graine fixe (seed) : mêmes données à chaque exécution -> reproductible.

Le score d'affinité n'est PAS envoyé dans l'événement : sinon le modèle ML
pourrait « tricher » en apprenant directement la règle du simulateur.
"""
import glob
import os
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

DATA_DIR = os.getenv("DATA_DIR", "data")

ENGLISH_RANK = {"no_english": 0, "basic": 1, "pre": 2,
                "intermediate": 3, "upper": 4, "fluent": 5}
EXP_YEARS = {"no_exp": 0, "1y": 1, "2y": 2, "3y": 3, "5y": 5}
EVENT_WEIGHT = {"view": 1, "save": 2, "apply": 3}


def _read(name, cols):
    pattern = f"{DATA_DIR}/recruitment-dataset-{name}-english/**/*.parquet"
    files = glob.glob(pattern, recursive=True)
    if not files:
        raise FileNotFoundError(f"Aucun fichier parquet trouvé : {pattern}")
    return pd.read_parquet(files[0], columns=cols)


def load_jobs(with_description=True):
    """Offres d'emploi nettoyées, triées par date de publication.
    with_description=False économise la mémoire (description inutile à la simulation)."""
    cols = ["id", "Position", "Company Name", "Primary Keyword",
            "Exp Years", "English Level", "Published"]
    if with_description:
        cols.append("Long Description")
    df = _read("job-descriptions", cols)
    df = df.dropna(subset=["id", "Primary Keyword", "Published"]).drop_duplicates("id")
    df["published_ts"] = pd.to_datetime(df["Published"], utc=True)
    df["exp_req"] = df["Exp Years"].map(EXP_YEARS).fillna(0)
    df["eng_req"] = df["English Level"].map(ENGLISH_RANK).fillna(0)
    return df.sort_values("published_ts").reset_index(drop=True)


def load_candidates():
    """Profils candidats nettoyés (champs utiles à la simulation uniquement)."""
    df = _read("candidate-profiles", ["id", "Primary Keyword",
                                      "Experience Years", "English Level"])
    df = df.dropna(subset=["id", "Primary Keyword"]).drop_duplicates("id")
    df["exp"] = df["Experience Years"].fillna(0)
    df["eng"] = df["English Level"].map(ENGLISH_RANK).fillna(0)
    return df.reset_index(drop=True)


def _cdf(weights):
    c = np.cumsum(weights)
    return c / c[-1]


class InteractionGenerator:
    def __init__(self, n_candidates=20000, p_same_keyword=0.8, seed=42):
        self.rng = np.random.default_rng(seed)
        self.p_same = p_same_keyword

        jobs = load_jobs(with_description=False)
        cands = load_candidates()
        if n_candidates and n_candidates < len(cands):
            cands = cands.sample(n_candidates, random_state=seed).reset_index(drop=True)
        self.jobs, self.cands = jobs, cands

        # Tableaux numpy : beaucoup plus rapides que l'accès ligne par ligne pandas
        self.j_id = jobs["id"].to_numpy()
        self.j_kw = jobs["Primary Keyword"].to_numpy()
        self.j_exp = jobs["exp_req"].to_numpy()
        self.j_eng = jobs["eng_req"].to_numpy()
        self.j_pub = jobs["published_ts"].tolist()
        self.c_id = cands["id"].to_numpy()
        self.c_kw = cands["Primary Keyword"].to_numpy()
        self.c_exp = cands["exp"].to_numpy()
        self.c_eng = cands["eng"].to_numpy()

        # Activité des candidats et popularité des offres : inégales (Pareto)
        self.cand_cdf = _cdf(self.rng.pareto(1.5, len(cands)) + 1)
        job_pop = self.rng.pareto(1.5, len(jobs)) + 1
        self.global_pool = (np.arange(len(jobs)), _cdf(job_pop))
        self.pools = {}
        for kw, labels in jobs.groupby("Primary Keyword").groups.items():
            idx = np.asarray(labels)
            self.pools[kw] = (idx, _cdf(job_pop[idx]))

    # --- tirages -------------------------------------------------------------
    def _pick_candidate(self):
        return int(np.searchsorted(self.cand_cdf, self.rng.random()))

    def _pick_job(self, keyword):
        if keyword in self.pools and self.rng.random() < self.p_same:
            idx, cdf = self.pools[keyword]
        else:
            idx, cdf = self.global_pool
        return int(idx[np.searchsorted(cdf, self.rng.random())])

    # --- règles d'affinité ---------------------------------------------------
    def _score(self, ci, ji):
        s = 0.5 * (self.c_kw[ci] == self.j_kw[ji])
        s += 0.25 * (self.c_exp[ci] >= self.j_exp[ji])
        s += 0.25 * (self.c_eng[ci] >= self.j_eng[ji])
        return float(np.clip(s + self.rng.normal(0, 0.1), 0, 1))

    def _event_type(self, score):
        r = self.rng.random()
        if r < 0.03 + 0.30 * score:
            return "apply"
        if r < 0.10 + 0.45 * score:
            return "save"
        return "view"

    def _make(self, ci, ji, ts):
        etype = self._event_type(self._score(ci, ji))
        return {
            "event_id": str(uuid.UUID(bytes=self.rng.bytes(16), version=4)),
            "candidate_id": self.c_id[ci],
            "job_id": self.j_id[ji],
            "event_type": etype,
            "weight": EVENT_WEIGHT[etype],
            "event_time": ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    # --- modes de génération -------------------------------------------------
    def history(self, n_events):
        """Historique daté : chaque interaction a lieu 0 à 45 jours après la
        publication de l'offre. Retourne une liste triée chronologiquement."""
        draws = []
        for _ in range(n_events):
            ci = self._pick_candidate()
            ji = self._pick_job(self.c_kw[ci])
            ts = self.j_pub[ji] + timedelta(days=float(self.rng.uniform(0, 45)))
            draws.append((ts, ci, ji))
        draws.sort(key=lambda d: d[0])
        return [self._make(ci, ji, ts) for ts, ci, ji in draws]

    def live(self):
        """Flux infini d'interactions horodatées à l'instant présent."""
        while True:
            ci = self._pick_candidate()
            ji = self._pick_job(self.c_kw[ci])
            yield self._make(ci, ji, datetime.now(timezone.utc))


if __name__ == "__main__":
    # Test local rapide, sans Kafka : python ingestion/generator.py
    gen = InteractionGenerator()
    print(f"Offres : {len(gen.jobs)} | Candidats simulés : {len(gen.cands)}")
    df = pd.DataFrame(gen.history(20000))
    print(df.head(5).to_string(), "\n")
    print("Répartition des événements :")
    print(df["event_type"].value_counts(normalize=True).round(3), "\n")
    print(f"Candidats distincts : {df.candidate_id.nunique()} | "
          f"Offres distinctes : {df.job_id.nunique()}")
    print(f"Période : {df.event_time.min()} -> {df.event_time.max()}")
