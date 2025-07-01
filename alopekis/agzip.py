import os
import glob
import gzip
import shutil



os.chdir('/work/pidgraph/staging')
files = glob.glob('*/*.jsonl')
for filename in files:
    with open(filename, "rb") as f_in:
        with gzip.open(f"{filename}.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)  # You forgot this line in your example.
    print(f"success. Zipped {filename}")