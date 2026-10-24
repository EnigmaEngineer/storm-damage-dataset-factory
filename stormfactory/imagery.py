"""Finding and tiling NOAA emergency response imagery for one storm event.

NOAA's National Geodetic Survey flies aerial imagery after a landfalling storm and
publishes each sortie as an XYZ tile pyramid. Two things about that catalogue decide
the whole shape of this module.

The catalogue has two formats. A recent storm publishes `services/index.json` carrying
a name, a bounding box and a zoom range per collection. An older one publishes nothing
machine readable at all, and the collection names are only visible as tile layer
constructor arguments inside the viewer's `js/app.js`. Both are parsed here, and the
shape each collection came from is recorded beside it, because a collection from the
older shape has no footprint and cannot be prefiltered by one.

A declared bounding box is not coverage. A sortie is a set of flight lines and the box
is their hull, so most of the box can hold no imagery. The server answers a tile it does
not have with a redirect to a transparent placeholder and an HTTP 200, so a run that
reads status codes concludes it has imagery everywhere. Coverage is therefore probed and
counted rather than read off the catalogue, and the placeholder is identified by hash.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from . import paths

VIEWER = "https://storms.ngs.noaa.gov/storms"
TILE_HOST = "https://stormscdn.ngs.noaa.gov"
USER_AGENT = "storm-damage-dataset-factory (portfolio project, contact via GitHub EnigmaEngineer)"

# The storms whose viewers were read to build the catalogue. Not every name on NOAA's
# index page answers: several older ones return 403 for the service document and only
# the viewer script is readable, and a few return nothing at all. The lock records what
# each one actually gave.
STORMS = (
    "barry", "debby", "delta", "dorian", "erin", "florence", "francine", "gordon",
    "harvey", "helene", "henri", "ian", "ida", "idalia", "irma", "isaias", "laura",
    "lee", "maria", "melissa", "michael", "milton", "nashville", "nate", "nicole",
    "sally", "zeta", "2023_hawaii",
)

COLLECTION_DATE = re.compile(r"^(?P<stamp>\d{8})(?P<sortie>[a-z0-9_]*)$")
LAYER_IN_VIEWER = re.compile(r"stormscdn\.ngs\.noaa\.gov/([0-9a-z_]+)-rgb/")

# The event has to be in the release years and the imagery has to be close enough to the
# event to be about it. Four days rather than two, because a sortie is weather dependent
# and the Ida flights over Mississippi ran to the third day.
POST_WINDOW_DAYS = 7


class ImageryError(Exception):
    pass


@dataclass(frozen=True)
class Collection:
    name: str
    storm: str
    catalogue_shape: str
    acquired: date
    bounds: tuple[float, float, float, float] | None

    @property
    def tile_template(self) -> str:
        return f"{TILE_HOST}/{self.name}-rgb/{{z}}/{{x}}/{{y}}"

    def tile_url(self, z: int, x: int, y: int) -> str:
        return f"{TILE_HOST}/{self.name}-rgb/{z}/{x}/{y}"

    def box_holds(self, lat: float, lon: float) -> bool:
        if self.bounds is None:
            return False
        west, south, east, north = self.bounds
        return west <= lon <= east and south <= lat <= north


def collection_acquired(name: str) -> date:
    """The acquisition date out of the collection name.

    `20210831b` is the second sortie published for 2021-08-31. The date is the only
    acquisition metadata the catalogue carries, so a collection whose name does not
    start with eight digits is refused rather than dated by guesswork.
    """
    m = COLLECTION_DATE.match(name)
    if m is None:
        raise ImageryError(f"collection {name!r} does not start with a yyyymmdd stamp")
    return datetime.strptime(m.group("stamp"), "%Y%m%d").date()


def _get(url: str, timeout: int = 30) -> tuple[int, str, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.geturl(), r.read()


class _Head(urllib.request.Request):
    def get_method(self) -> str:
        return "HEAD"


def _head(url: str, timeout: int = 30) -> tuple[int, str, int]:
    """Status, final url and declared length, without the body.

    The redirect to the placeholder survives a HEAD, so selection can ask whether a
    collection holds a tile without pulling the tile. The body is still pulled for the
    collections that are chosen, because what lands on disk is identified by its hash
    and a content length is not a hash.
    """
    with urllib.request.urlopen(_Head(url, headers={"User-Agent": USER_AGENT}), timeout=timeout) as r:
        return r.status, r.geturl(), int(r.headers.get("Content-Length") or 0)


def read_catalogue(storm: str) -> tuple[str, list[Collection], str]:
    """Every collection one storm publishes, from whichever shape answers.

    Returns the shape, the collections, and the sha256 of the document they were parsed
    from, so the lock can pin what was read.
    """
    try:
        _, _, body = _get(f"{VIEWER}/{storm}/services/index.json")
        entries = json.loads(body)
        out = []
        for entry in entries:
            name = re.sub(r"_RGB$", "", entry["name"]).lower()
            bounds = entry.get("bounds")
            out.append(Collection(
                name=name, storm=storm, catalogue_shape="services/index.json",
                acquired=collection_acquired(name),
                bounds=tuple(bounds) if bounds else None,
            ))
        return "services/index.json", out, hashlib.sha256(body).hexdigest()
    except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError):
        pass

    _, _, body = _get(f"{VIEWER}/{storm}/js/app.js")
    names = dict.fromkeys(LAYER_IN_VIEWER.findall(body.decode("utf-8", "replace")))
    out = [
        Collection(name=n, storm=storm, catalogue_shape="js/app.js",
                   acquired=collection_acquired(n), bounds=None)
        for n in names
    ]
    return "js/app.js", out, hashlib.sha256(body).hexdigest()


def build_catalogue(storms=STORMS) -> tuple[list[Collection], dict]:
    """The whole catalogue, plus what each storm's document hashed to.

    Threaded because it is 28 independent fetches and a serial pass spends most of a
    minute waiting. A storm that answers nothing is recorded as such rather than
    dropped, since a collection appearing later would change a selection.
    """
    lock: dict[str, dict] = {}
    collections: list[Collection] = []

    def one(storm: str):
        try:
            shape, cols, sha = read_catalogue(storm)
            return storm, shape, cols, sha, None
        except Exception as exc:  # noqa: BLE001 - the reason goes in the lock
            return storm, "none", [], None, f"{type(exc).__name__}: {exc}"

    with ThreadPoolExecutor(max_workers=8) as pool:
        for storm, shape, cols, sha, err in pool.map(one, storms):
            lock[storm] = {
                "catalogue_shape": shape,
                "collections": len(cols),
                "sha256": sha,
                "unavailable": err,
            }
            collections.extend(cols)

    collections.sort(key=lambda c: (c.acquired, c.name))
    return collections, lock


# Web Mercator tile arithmetic. Written out rather than taken from a dependency, because
# the whole of it is four lines and a tile grid that disagrees with the imagery server by
# one row is the kind of defect that shows up as a plausible looking picture.

def tile_of(lat: float, lon: float, z: int) -> tuple[int, int]:
    n = 2 ** z
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
    return x, y


def tile_north_west(z: int, x: int, y: int) -> tuple[float, float]:
    n = 2 ** z
    lon = x / n * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    return lat, lon


def tile_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    north, west = tile_north_west(z, x, y)
    south, east = tile_north_west(z, x + 1, y + 1)
    return west, south, east, north


def tile_centre(z: int, x: int, y: int) -> tuple[float, float]:
    west, south, east, north = tile_bounds(z, x, y)
    return (south + north) / 2.0, (west + east) / 2.0


def metres_per_pixel(lat: float, z: int) -> float:
    """Ground sample distance of one tile pixel, for a 256 pixel tile.

    Published because the zoom a tile set is cut at is a resolution claim, and a reader
    should not have to rederive it from the zoom.
    """
    return 156543.03392804097 * math.cos(math.radians(lat)) / (2 ** z)


def haversine_m(a: tuple[float, float], b: tuple[float, float]) -> float:
    r = 6371008.8
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))


def distance_to_segment_m(point, start, end) -> float:
    """Distance from a tile centre to the event's damage track.

    Projected locally rather than solved on the sphere. Over a track of a few kilometres
    the error is far below one tile, and a local projection keeps the function readable.
    """
    lat0 = math.radians((start[0] + end[0]) / 2)
    def xy(p):
        return (math.radians(p[1]) * math.cos(lat0) * 6371008.8, math.radians(p[0]) * 6371008.8)
    px, py = xy(point)
    ax, ay = xy(start)
    bx, by = xy(end)
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return math.hypot(px - ax, py - ay)
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    return math.hypot(px - (ax + t * dx), py - (ay + t * dy))


def corridor_tiles(start, end, z: int, buffer_m: float) -> list[tuple[int, int]]:
    """Every tile at this zoom whose centre is within `buffer_m` of the damage track.

    A tornado's footprint is a line and not a disc, so a square window around the
    midpoint would spend most of its tiles on ground the storm never touched. The
    candidate rectangle is the bounding box of the track grown by one tile on each side,
    and the buffer then decides which of those candidates are kept.
    """
    xs, ys = zip(*(tile_of(lat, lon, z) for lat, lon in (start, end)))
    out = []
    for x in range(min(xs) - 1, max(xs) + 2):
        for y in range(min(ys) - 1, max(ys) + 2):
            if distance_to_segment_m(tile_centre(z, x, y), start, end) <= buffer_m:
                out.append((x, y))
    return sorted(out)


def placeholder_sha256() -> str:
    """The hash of the image the tile server sends for a tile it does not hold.

    Measured rather than assumed. The server answers with a 302 to a transparent PNG and
    an HTTP 200 after the redirect, so the only reliable tell is the content, and the
    redirect target is read once here and hashed.
    """
    _, _, body = _get(f"{VIEWER}/ian/images/clear.png")
    return hashlib.sha256(body).hexdigest()


@dataclass(frozen=True)
class TileFetch:
    z: int
    x: int
    y: int
    status: int
    bytes: int
    sha256: str
    is_imagery: bool
    final_url: str


def fetch_tiles(collection: Collection, tiles, z: int, dest: Path, placeholder: str,
                workers: int = 8) -> list[TileFetch]:
    """Fetch one collection's tiles, writing only the ones that hold imagery.

    The placeholder is identified by hash and by the redirect target, and both are
    recorded. Writing the placeholder to disk would leave a tile set that looks complete
    and carries nothing, which is the failure this whole module is shaped around.
    """
    dest.mkdir(parents=True, exist_ok=True)

    def one(xy):
        x, y = xy
        url = collection.tile_url(z, x, y)
        status, final, body = _get(url)
        sha = hashlib.sha256(body).hexdigest()
        imagery = sha != placeholder and "clear.png" not in final
        if imagery:
            (dest / f"{z}_{x}_{y}.jpg").write_bytes(body)
        return TileFetch(z, x, y, status, len(body), sha, imagery, final)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return sorted(pool.map(one, tiles), key=lambda t: (t.x, t.y))


def probe_coverage(collection: Collection, tiles, z: int, placeholder: str,
                   workers: int = 10) -> set[tuple[int, int]]:
    """Which of these tiles the collection holds, asked with a HEAD and kept nowhere.

    Selection probes every candidate over the whole tile set, so pulling bodies here
    would download several collections' worth of imagery to throw all but one away. The
    placeholder redirect survives a HEAD, so the question is answerable without the
    bytes. The `placeholder` hash is still the argument, because the chosen collections
    go through `fetch_tiles` and the two verdicts are compared there.
    """
    def one(xy):
        x, y = xy
        _, final, length = _head(collection.tile_url(z, x, y))
        return (x, y) if ("clear.png" not in final) else None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return {r for r in pool.map(one, tiles) if r is not None}


def coverage(fetched: list[TileFetch]) -> dict:
    held = [t for t in fetched if t.is_imagery]
    return {
        "tiles_probed": len(fetched),
        "tiles_with_imagery": len(held),
        "tiles_placeholder": len(fetched) - len(held),
        "distinct_image_hashes": len({t.sha256 for t in held}),
    }


# NOAA writes the event's local clock time in BEGIN_DATE_TIME and names the zone in
# CZ_TIMEZONE as an abbreviation with its offset, for example `CST-6`. The offset is read
# from the digits and applied literally. It is not corrected for daylight saving, because
# the field says standard time all year in the overwhelming majority of rows and says
# daylight time in a few hundred that include winter dates, so no rule derived from it
# would be right in both directions. The count of each case is published beside the
# column rather than hidden behind a correction.
TZ_FIELD = re.compile(r"^(?P<abbrev>[A-Z]+)(?P<offset>[+-]?\d{1,2})$")
DAYLIGHT_ABBREVS = frozenset({"ADT", "AKDT", "CDT", "EDT", "HDT", "MDT", "PDT"})


def stated_offset_hours(cz_timezone: str) -> int:
    m = TZ_FIELD.match(cz_timezone.strip())
    if m is None:
        raise ImageryError(f"CZ_TIMEZONE {cz_timezone!r} is not an abbreviation with an offset")
    return int(m.group("offset"))


def begin_utc(begin_date_time: str, cz_timezone: str) -> datetime:
    """The event's begin time as an instant, under the offset the row states.

    NOAA's own format for the field is `29-AUG-21 21:11:00`, a two digit year with an
    uppercase month abbreviation.
    """
    local = datetime.strptime(begin_date_time, "%d-%b-%y %H:%M:%S")
    return (local - timedelta(hours=stated_offset_hours(cz_timezone))).replace(tzinfo=timezone.utc)


def timezone_field_audit(frame: pl.DataFrame) -> dict:
    """How often the stated zone abbreviation disagrees with the date it sits on.

    Every figure here is a count over the release's own rows. It exists so the README can
    say what applying the stated offset literally costs, with a number rather than a
    hedge.
    """
    rows = frame.select(["CZ_TIMEZONE", "BEGIN_DATE_TIME", "MONTH_NAME"])
    daylight = rows.filter(
        pl.col("CZ_TIMEZONE").str.extract(r"^([A-Z]+)", 1).is_in(list(DAYLIGHT_ABBREVS))
    )
    winter = daylight.filter(pl.col("MONTH_NAME").is_in(["December", "January", "February"]))
    summer_standard = rows.filter(
        (~pl.col("CZ_TIMEZONE").str.extract(r"^([A-Z]+)", 1).is_in(list(DAYLIGHT_ABBREVS)))
        & pl.col("MONTH_NAME").is_in(["June", "July", "August"])
    )
    return {
        "rows": rows.height,
        "rows_naming_a_daylight_zone": daylight.height,
        "daylight_named_in_december_to_february": winter.height,
        "rows_naming_a_standard_zone_in_june_to_august": summer_standard.height,
        "distinct_zone_fields": rows["CZ_TIMEZONE"].n_unique(),
    }


def tiles_frame(event: dict, z: int, post: Collection, pre: Collection,
                post_fetch: list[TileFetch], pre_fetch: list[TileFetch]) -> pl.DataFrame:
    """One row per tile, carrying both collections' hashes and the event's begin instant.

    The begin instant is the first time zone aware column any release artifact has held,
    which is why `digest.py` grew a Datetime rule before this was written.
    """
    post_by = {(t.x, t.y): t for t in post_fetch}
    pre_by = {(t.x, t.y): t for t in pre_fetch}
    begin = begin_utc(event["BEGIN_DATE_TIME"], event["CZ_TIMEZONE"])
    start = (event["BEGIN_LAT"], event["BEGIN_LON"])
    end = (event["END_LAT"], event["END_LON"])

    rows = []
    for key in sorted(set(post_by) | set(pre_by)):
        x, y = key
        west, south, east, north = tile_bounds(z, x, y)
        lat, lon = tile_centre(z, x, y)
        p, q = post_by.get(key), pre_by.get(key)
        rows.append({
            "tile_id": f"{z}/{x}/{y}",
            "EVENT_ID": event["EVENT_ID"],
            "zoom": z,
            "tile_x": x,
            "tile_y": y,
            "centre_lat": lat,
            "centre_lon": lon,
            "west": west,
            "south": south,
            "east": east,
            "north": north,
            "metres_per_pixel": metres_per_pixel(lat, z),
            "metres_from_track": distance_to_segment_m((lat, lon), start, end),
            "event_begin_utc": begin,
            "post_collection": post.name,
            "post_acquired": post.acquired,
            "post_has_imagery": bool(p and p.is_imagery),
            "post_sha256": p.sha256 if p else None,
            "pre_collection": pre.name,
            "pre_acquired": pre.acquired,
            "pre_has_imagery": bool(q and q.is_imagery),
            "pre_sha256": q.sha256 if q else None,
        })
    frame = pl.DataFrame(rows)
    return frame.with_columns(
        pl.col("event_begin_utc").cast(pl.Datetime("us", "UTC")),
        pl.col("post_acquired").cast(pl.Date),
        pl.col("pre_acquired").cast(pl.Date),
    )


def lock_path() -> Path:
    return paths.release_root() / "imagery.lock.json"


def tile_dir(collection: str) -> Path:
    return paths.data_root() / "tiles" / collection
