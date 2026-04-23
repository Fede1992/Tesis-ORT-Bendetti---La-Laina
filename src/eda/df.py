import pandas as pd
from pathlib import Path

rows = []
for path in Path("data/interim/txt_refined").glob("*.txt"):
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    rows.append({
        "file_name": path.name,
        "text": text,
        "n_words": len(text.split())
    })

df = pd.DataFrame(rows)
print(df.shape)
head = df.head(5)
print (head)


