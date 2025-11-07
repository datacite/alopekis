from glob import iglob
import os
from .config import OUTPUT_PATH


def generate_manifest_file() -> None:
    """Generate a listing of the files within the datafile and save as MANIFEST."""
    with open(f'{OUTPUT_PATH}/MANIFEST', 'w') as manifest_file:
        for file in iglob(f'dois/*/*.gz', root_dir=OUTPUT_PATH):
            manifest_file.write(f'{file} {os.path.getsize(os.path.join(OUTPUT_PATH,file))}\n')
