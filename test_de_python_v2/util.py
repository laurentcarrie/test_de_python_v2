from pathlib import Path


def rmdir_f(d: Path):
    if not d.exists():
        return
    if d.is_file():
        d.unlink()
    else:
        for f in d.iterdir():
            rmdir_f(f)
        d.rmdir()
