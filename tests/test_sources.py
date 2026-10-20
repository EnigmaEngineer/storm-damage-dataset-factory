from stormfactory import sources

SAMPLE = """
<a href="StormEvents_details-ftp_v1.0_d2021_c20260323.csv.gz">StormEvents_details-ftp_v1.0_d2021_c20260323.csv.gz</a>
<a href="StormEvents_details-ftp_v1.0_d2021_c20240716.csv.gz">StormEvents_details-ftp_v1.0_d2021_c20240716.csv.gz</a>
<a href="StormEvents_locations-ftp_v1.0_d2021_c20260323.csv.gz">locations, not details</a>
<a href="StormEvents_details-ftp_v1.0_d2022_c20260625.csv.gz">StormEvents_details-ftp_v1.0_d2022_c20260625.csv.gz</a>
"""


def check_index_deduplicates_the_double_listing():
    files = sources.parse_index(SAMPLE)
    assert len(files) == 3, [f.filename for f in files]


def check_index_ignores_the_locations_files():
    assert all("details" in f.filename for f in sources.parse_index(SAMPLE))


def check_newest_revision_wins():
    picked = sources.newest_per_year(sources.parse_index(SAMPLE), [2021])
    assert [f.revision for f in picked] == ["20260323"], picked


def check_a_missing_year_raises_rather_than_shortening_the_release():
    try:
        sources.newest_per_year(sources.parse_index(SAMPLE), [2021, 1812])
    except sources.SourceError as e:
        assert "1812" in str(e), str(e)
    else:
        raise AssertionError("a year with no file was accepted")


def check_url_is_the_index_plus_the_name():
    f = sources.parse_index(SAMPLE)[0]
    assert f.url == sources.INDEX_URL + f.filename
