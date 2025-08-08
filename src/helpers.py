"""## This file contains functions for the data pipeline powering the dataset behind ATP search tools"""

from bs4 import BeautifulSoup
import requests
from datetime import datetime
import chromadb
from tqdm import tqdm
import webvtt
import io
import patito as pt
import polars as pl
from config import *

# Regex patterns for validating urls
video_url_pattern = r"^https://allthepreaching.com/pages/video.php\?id=\d+$"
mp4_url_pattern = r"^(https?:\/\/)?(www\.)?[a-zA-Z0-9]+([\-\.]{1}[a-zA-Z0-9]+)*\.[a-zA-Z]{2,5}(:[0-9]{1,5})?(\/.*)?mp4$"
mp3_url_pattern = r"^(https?:\/\/)?(www\.)?[a-zA-Z0-9]+([\-\.]{1}[a-zA-Z0-9]+)*\.[a-zA-Z]{2,5}(:[0-9]{1,5})?(\/.*)?mp3$"
vtt_url_pattern = r"^(https?:\/\/)?(www\.)?[a-zA-Z0-9]+([\-\.]{1}[a-zA-Z0-9]+)*\.[a-zA-Z]{2,5}(:[0-9]{1,5})?(\/.*)?vtt$"
valid_url_pattern = r"^(https?:\/\/)?(www\.)?[a-zA-Z0-9]+([\-\.]{1}[a-zA-Z0-9]+)*\.[a-zA-Z]{2,5}(:[0-9]{1,5})?(\/.*)?$"


class PreScrapingDataFrameModel(pt.Model):
    """Validates the records before any scraping occours.

    - video_id: int - Primary key at this stage. Must be unique.
    - section: str - The name of the section this link was extracted from. Must be over 3 chars long
    - title: str = The title associated with this link. Must be over 3 chars long
    - preacher: str = The preacher associated with this link. Must be over 3 chars long
    - video_url: str = The video url derived from video_id. Must be unique.
    Must conform to this pattern: '^https://allthepreaching.com/pages/video.php\?id=\d+$'
    """

    video_id: int = pt.Field(unique=True)
    section: str = pt.Field(min_length=3)
    title: str = pt.Field(min_length=3)
    preacher: str = pt.Field(min_length=3)
    video_url: str = pt.Field(unique=True, pattern=video_url_pattern)
    # This has been handled, and should not break when the site is updated to the expected  urls.
    # XXX ^---- look up here. this may break when the site gets updated, since the new way is just "/video/..." instead of "https:/.../video/..."


# Validates the transcript metadata.
class TranscriptDataFrameModel(PreScrapingDataFrameModel):
    """Does a basic validaton on the transcript records.
    Does not validate the content of the transcript.
    Based on PreScrapingDataFrameModel.

    - mp4_url: str = Must be unique. Must conform to mp4_url_pattern.
    - mp3_url: str = Must be unique. Must conform to mp3_url_pattern. Derived from mp4_url.
    - vtt_url: str = Must be unique. Must conform to vtt_url_pattern. Derived from mp4_url.
    - ~txt_url: str = Currently, vtt_to_txt.php is not available on ATP, so this field is unused~
    - vtt: str = The webvtt text from the video. Must be unique. Must be at least 5 chars long.
    - transcript: str = The transcript from the video. Must be unique. Must be at least 5 chars long. Derived from vtt (.derive() not set, so it does't work)
    - transcript_hash: int = A hash of the transcript (made using pl.col("transcript").hash()). This is meant to speed up checking for duplicate transcripts.

    (inherited fields:)
    - video_id: int - Primary key at this stage. Must be unique.
    - section: str - The name of the section this link was extracted from. Must be over 3 chars long
    - title: str = The title associated with this link. Must be over 3 chars long
    - preacher: str = The preacher associated with this link. Must be over 3 chars long
    - video_url: str = The video url derived from video_id. Must be unique.
    Must conform to this pattern: '^https://allthepreaching.com/pages/video.php\?id=\d+$'
    """

    mp4_url: str = pt.Field(
        unique=True,
        pattern=mp4_url_pattern,
    )
    mp3_url: str = pt.Field(
        unique=True,
        pattern=mp3_url_pattern,
        derived_from=(pl.col("mp4_url").str.replace("mp4", "mp3")),
    )
    vtt_url: str = pt.Field(
        unique=True,
        pattern=vtt_url_pattern,
        derived_from=(pl.col("mp4_url").str.replace("mp4", "vtt")),
    )
    # txt_url: str = pt.Field() # Currently, vtt_to_txt.php is not available on ATP, so this field is unused
    vtt: str = pt.Field(unique=True, min_length=5)
    transcript: str = pt.Field(unique=True, min_length=5)
    transcript_hash: int = pt.Field(unique=True)


# TODO: validate that the chunks are not gibberish
# Validates the chunk record. Once validated, it should be ready for the collection.
class ChunkedRecordDataFrameModel(PreScrapingDataFrameModel):
    """Does a basic validaton on the chunk records.
    Based on PreScrapingDataFrameModel.

    - mp4_url: str - Must be unique. Must conform to mp4_url_pattern.
    - chunk: str - The chunk to be embedded in vector search.

    (inherited fields:)
    - video_id: int - Primary key at this stage. Must be unique.
    - section: str - The name of the section this link was extracted from. Must be over 3 chars long
    - title: str = The title associated with this link. Must be over 3 chars long
    - preacher: str = The preacher associated with this link. Must be over 3 chars long
    - video_url: str = The video url derived from video_id. Must be unique.
    Must conform to this pattern: '^https://allthepreaching.com/pages/video.php\?id=\d+$'
    """

    mp4_url: str = pt.Field(
        unique=True,
        pattern=mp4_url_pattern,
    )
    chunk: str = pt.Field(min_length=5)


def get_records_from_archive_url(
    atp_videos_archive_url: str = "https://allthepreaching.com/pages/archive.php",
) -> list[dict]:
    """Directly scrapes the [video_url, title, section] fields from the given url."""
    response = requests.get(atp_videos_archive_url)
    html_content = response.text
    soup = BeautifulSoup(html_content, "html.parser")
    records = []
    for tag in soup.find_all(["h2", "a"]):
        if tag.name == "h2":
            current_section = tag.get_text(strip=True)
        elif tag.name == "a" and tag.get("title") and tag.get("href"):
            records.append(
                {
                    "section": str(current_section).lower(),
                    "title": str(tag.get("title", "").strip("'\" ")).lower(),
                    # "text": tag.get_text(strip=True).lower().strip(),
                    "video_url": str(tag["href"]),
                }
            )
    return records


def get_records_from_html_file(file: str) -> list[dict]:
    """Scrapes the [video_url, title, section] fields from the given html file."""
    with open(file, "r") as f:
        html_content = f.read()
    soup = BeautifulSoup(html_content, "html.parser")
    records = []
    for tag in soup.find_all(["h2", "a"]):
        if tag.name == "h2":
            current_section = tag.get_text(strip=True)
        elif tag.name == "a" and tag.get("title") and tag.get("href"):
            records.append(
                {
                    "section": str(current_section).lower(),
                    "title": str(tag.get("title", "").strip("'\" ")).lower(),
                    # "text": tag.get_text(strip=True).lower().strip(),
                    "video_url": str(tag["href"]),
                }
            )
    return records


def get_mp4_url_from_video_url(video_url: str) -> str:
    """Scrapes the mp4_url from the video_url page"""
    html_content = requests.get(video_url).text
    soup = BeautifulSoup(html_content, "html.parser")
    mp4_url = soup.find("video").get_attribute_list("src")
    mp4_url = mp4_url[0]
    return mp4_url


def get_html_content_from_url(url: str) -> str:
    """Scrapes the html content from the given url"""
    response = requests.get(url)
    content = response.text
    return content


def vtt_to_text(vtt_text: str) -> str:
    """Converts WebVTT to text. Skips repeating captions."""
    text_captions = [""]
    vtt_buffer = io.StringIO(vtt_text)
    captions = webvtt.from_buffer(vtt_buffer)
    for caption in captions:
        if caption.text != text_captions[-1]:
            text_captions.append(caption.text)
    text = " ".join(text_captions).strip()
    return text


def get_section_preacher_df() -> pl.LazyFrame:
    """Creates a section_preacher lazyframe based on section_preacher_map in config.py"""
    section_preacher_df = (
        pl.from_dicts(section_preacher_map)
        .transpose(include_header=True)
        .rename({"column": "section", "column_0": "preacher"})
        .lazy()
    )
    return section_preacher_df


def evaluate_preacher(title: str) -> str:
    """If the title contains one of the preacher names, it will return the preacher's proper name.

    - e.g. 'sermons pastor anderson' -> 'pastor steven anderson'

    It is based on preacher_names_replacements located in config.py"""
    preacher = "unknown"
    for name, proper_name in preacher_names_replacements.items():
        if name in title:
            preacher = proper_name
            break
    return preacher


def to_pre_scraping_df(
    scraped_records: list | pl.DataFrame,
) -> pt.DataFrame:  # changed this row to include df
    """Takes a df with ["section", "title", "video_url"] fields and returns a df with:

    - video_id: int - Extracts video_id from video_url
    - section: str - Removes sections that are not relevant to RAG, such as music.
    - preacher: str - The name of the preacher. Infers the name based on the section and evaluates when needed. Defaults to "unknown"
    - title: str - If the title was originally just the name of the preacher, such as at a conference, it will change to the section + preacher
    - video_url: str - Reprocesses it to make sure that partial links won't break the url.
    """
    section_preacher_df = get_section_preacher_df()

    df = (
        PreScrapingDataFrameModel.DataFrame(scraped_records)
        .lazy()
        .select(
            [
                "section",
                "title",
                "video_url",
            ]
        )
        .filter(~pl.col("section").is_in(disallowed_sections))
    )

    for pattern, replacement in section_replacements.items():
        df = df.with_columns(pl.col("section").str.replace_all(pattern, replacement))
    for pattern, replacement in title_replacements.items():
        df = df.with_columns(pl.col("title").str.replace_all(pattern, replacement))

    df = (
        df.with_columns(
            # This extracts the video_id and remakes it, to avoid breaking if the video_url is incomplete (the site may have incomplete urls)
            pl.col("video_url")
            .str.extract(r"id=(\d+)", 1)
            .cast(int)
            .alias("video_id")
        )
        .unique("video_id")
        .filter(~pl.col("video_id").is_in(existing_video_ids))
        .with_columns(
            [
                (
                    pl.lit("https://allthepreaching.com/pages/video.php?id=")
                    + pl.col("video_id").cast(str)
                ).alias("video_url"),
            ]
        )
        .join(section_preacher_df, pl.col("section"))
    )

    evaluate_preacher_df = (
        df.filter(pl.col("preacher") == "evaluate")
        # Apply the function directly to the "title" column
        # and overwrite the "preacher" column with the result.
        .with_columns(
            pl.col("title")
            .map_elements(evaluate_preacher, return_dtype=pl.String)
            .alias("preacher")
        )
        # Use the newly updated "preacher" column to create the new "title".
        .with_columns(
            pl.when(pl.col("preacher") != "unknown")
            .then(pl.col("preacher") + " " + pl.col("section"))
            .otherwise(pl.col("title"))
            .alias("title")
        )
    )

    df = (
        df.update(evaluate_preacher_df, on="video_id")
        # Order columns
        .select(
            [
                "video_id",
                "section",
                "preacher",
                "title",
                "video_url",
            ]
        )
        # Collect at the very end
        .collect()
    )

    try:
        df.validate()
    except pt.DataFrameValidationError as e:
        print(e)
        # raise(e)
    return df


def to_transcript_df(df: pt.DataFrame) -> pt.DataFrame:
    """Validates the data for the record. Does not validate the transcript.

    - transcript_hash: int - This is a number to be used for detecting duplicate transcripts, rather than comparing entire transcripts.
    """
    df = (
        TranscriptDataFrameModel.LazyFrame(df)
        .derive()
        .unique(
            "video_id"
        )  # TODO: shouldn't need this when scraping. I used it because I am adapting the existing dataset to the new format, and it has dupes.
        # Filters must match TranscriptDataFrameModel field validation definitions
        .filter([pl.col("mp4_url").str.contains(mp4_url_pattern)])
        .filter([pl.col("mp3_url").str.contains(mp3_url_pattern)])
        .filter([pl.col("vtt_url").str.contains(vtt_url_pattern)])
        .filter([pl.col("transcript") != "."])
        .filter([pl.col("vtt").str.len_chars() > 5])
        .filter([pl.col("transcript").str.len_chars() > 5])
        .with_columns(pl.col("transcript").hash().alias("transcript_hash"))
        .unique(
            [
                "mp4_url",
            ],
            keep="first",
        )
        .unique(
            [
                "transcript_hash",
            ],
            keep="first",
        )
        .collect()
    )
    try:
        df.validate()
    except pt.DataFrameValidationError as e:
        print(e)
        # raise(e)
    return df


def to_chunked_record_df():

    pass


# message_404 = """<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN">\n<html><head>\n<title>404 Not Found</title>\n</head><body>\n<h1>Not Found</h1>\n<p>The requested URL was not found on this server.</p>\n</body></html>\n"""
