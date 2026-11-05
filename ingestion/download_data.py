from huggingface_hub import snapshot_download

for repo in ["lang-uk/recruitment-dataset-job-descriptions-english",
             "lang-uk/recruitment-dataset-candidate-profiles-english"]:
    name = repo.split("/")[1]
    snapshot_download(repo_id=repo, repo_type="dataset",
                      allow_patterns=["*.parquet"],
                      local_dir=f"data/{name}")
    print(f"OK : {name}")
