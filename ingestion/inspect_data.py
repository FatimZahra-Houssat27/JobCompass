import pandas as pd, glob

for d in ["job-descriptions", "candidate-profiles"]:
    f = glob.glob(f"data/recruitment-dataset-{d}-english/**/*.parquet", recursive=True)[0]
    df = pd.read_parquet(f)
    print("=" * 60)
    print(d, df.shape)
    print(df.dtypes, "\n")
    pd.set_option("display.max_colwidth", 80)
    print(df.head(2).T, "\n")
