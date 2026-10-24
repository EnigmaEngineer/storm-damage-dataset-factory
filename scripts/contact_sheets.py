"""Lay the tile pairs out as sheets so one annotator can label them without a browser.

The Label Studio project is the thing this repo ships and it is not what produced the
committed labels. A scheduled run has no browser, so no Label Studio server ever saw the
project, and the labelling pass was done by reading these sheets instead. The export in
`labels/` is written in Label Studio's import shape so the project can be loaded with the
labels already on it, and the README says that the shape is asserted here rather than
confirmed by a server.

Tiles are drawn at their native 256 pixels. Enlarging them would make the sheet look
sharper and add nothing a label can rest on.
"""

import json
import sys
from pathlib import Path

from PIL import Image, ImageDraw

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl

from stormfactory import imagery, release

TILE = 256
PAIRS_PER_ROW = 3
ROWS = 2
LABEL_STRIP = 18


def sheet(pairs, post_dir: Path, pre_dir: Path, out: Path) -> None:
    cell_w = TILE * 2
    cell_h = TILE + LABEL_STRIP
    canvas = Image.new("RGB", (cell_w * PAIRS_PER_ROW, cell_h * ROWS), (16, 16, 16))
    draw = ImageDraw.Draw(canvas)
    for i, row in enumerate(pairs):
        cx = (i % PAIRS_PER_ROW) * cell_w
        cy = (i // PAIRS_PER_ROW) * cell_h
        name = f"{row['zoom']}_{row['tile_x']}_{row['tile_y']}.jpg"
        for j, folder in enumerate((pre_dir, post_dir)):
            path = folder / name
            if path.exists():
                canvas.paste(Image.open(path).convert("RGB").resize((TILE, TILE)), (cx + j * TILE, cy + LABEL_STRIP))
        draw.text((cx + 4, cy + 4), f"{row['tile_id']}   before | after", fill=(255, 255, 0))
    canvas.save(out)


def main() -> int:
    frame = pl.read_parquet(release.artifact_path("imagery_tiles")).sort("tile_id")
    rows = frame.filter(pl.col("post_has_imagery") & pl.col("pre_has_imagery")).to_dicts()
    post_dir = imagery.tile_dir(rows[0]["post_collection"])
    pre_dir = imagery.tile_dir(rows[0]["pre_collection"])
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/sheets")
    out_dir.mkdir(parents=True, exist_ok=True)
    per = PAIRS_PER_ROW * ROWS
    sheets = []
    for n, start in enumerate(range(0, len(rows), per), start=1):
        out = out_dir / f"sheet{n:02d}.png"
        sheet(rows[start:start + per], post_dir, pre_dir, out)
        sheets.append({"sheet": str(out), "tiles": [r["tile_id"] for r in rows[start:start + per]]})
    print(json.dumps({"pairs": len(rows), "sheets": sheets}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
