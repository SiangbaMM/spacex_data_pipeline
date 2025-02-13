from pathlib import Path

rel_path = Path(__file__)

base_dir = rel_path.parts[:-1]

base_path = Path(*base_dir)

print(base_path)
