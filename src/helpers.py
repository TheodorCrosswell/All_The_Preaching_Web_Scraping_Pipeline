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


class PreScrapingDataFrameModel(pt.Model):
    video_id: int = pt.Field(unique=True, ge=1)
    section: str = pt.Field(min_length=3)
    title: str = pt.Field(min_length=3)
    preacher: str = pt.Field(min_length=3)
    video_url: str = pt.Field(
        unique=True,
        pattern=r"^https://allthepreaching.com/pages/video.php\?id=\d{7}$",
    )  # this may break when the site gets updated, since the new way is just "/video/..." instead of "https:/.../video/..."


class CleanTranscriptDataFrameModel(PreScrapingDataFrameModel):
    mp4_url: str = pt.Field(
        unique=True,
        pattern=r"https://www.kjv1611only.com/video/\w+/\w+/\w+.mp4",
    )
    mp3_url: str = pt.Field(
        unique=True,
        pattern=r"https://www.kjv1611only.com/video/\w+/\w+/\w+.mp3",
        derived_from=(pl.col("mp4_url").str.replace("mp4", "mp3")),
    )
    vtt_url: str = pt.Field(
        unique=True,
        pattern=r"https://www.kjv1611only.com/video/\w+/\w+/\w+.vtt",
        derived_from=(pl.col("mp4_url").str.replace("mp4", "vtt")),
    )
    # txt_url: str = pt.Field() # Currently, vtt_to_txt.php is not available on ATP, so this field is unused
    vtt: str = pt.Field(unique=True, min_length=50)
    transcript: str = pt.Field(unique=True, min_length=50)


# class ChunkedRecordDataFrameModel(PreScrapingDataFrameModel):
#     mp4_url: str = pt.Field(
#       unique=True,
#       pattern=r"https://www.kjv1611only.com/video/\w+/\w+/\w+.mp4",
#     )
#     chunk: str = pt.Field(min_length=5)


def get_records_from_archive_url(
    atp_videos_archive_url: str = "https://allthepreaching.com/pages/archive.php",
) -> list[dict]:
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
    html_content = requests.get(video_url).text
    soup = BeautifulSoup(html_content, "html.parser")
    mp4_url = soup.find("video").get_attribute_list("src")
    mp4_url = mp4_url[0]
    return mp4_url


def get_html_content_from_url(url: str) -> str:
    response = requests.get(url)
    content = response.text
    return content


def vtt_to_text(vtt_text: str) -> str:
    text_captions = [""]
    vtt_buffer = io.StringIO(vtt_text)
    captions = webvtt.from_buffer(vtt_buffer)
    for caption in captions:
        if caption.text != text_captions[-1]:
            text_captions.append(caption.text)
    text = " ".join(text_captions).strip()
    return text


def get_section_preacher_df() -> pl.LazyFrame:
    section_preacher_df = (
        pl.from_dicts(section_preacher_map)
        .transpose(include_header=True)
        .rename({"column": "section", "column_0": "preacher"})
        .lazy()
    )
    return section_preacher_df


def evaluate_preacher(title: str) -> str:
    preacher = "unknown"
    for name, proper_name in preacher_names_replacements.items():
        if name in title:
            preacher = proper_name
            break
    return preacher


evaluate_preacher_output_schema = {
    "video_id": pl.Int64,
    "section": pl.String,
    "preacher": pl.String,
    "title": pl.String,
    "video_url": pl.String,
}


def get_pre_scraping_df(
    scraped_records: list | pl.DataFrame,
) -> pt.DataFrame:  # changed this row to include df

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
        # .drop("raw_video_url", strict=False)
        .filter(~pl.col("video_id").is_in(existing_video_ids))
        .with_columns(
            [
                (
                    pl.lit("https://allthepreaching.com/pages/video.php?id=")
                    + pl.col("video_id").cast(str)
                ).alias("video_url"),
                # pl.col("section"),
            ]
        )
        .join(section_preacher_df, pl.col("section"))
        # .collect()
    )

    # --- Start of refactored block ---
    evaluate_preacher_df = (
        df.filter(pl.col("preacher") == "evaluate")
        # Step 1: Apply the Python function directly to the "title" column
        # and overwrite the "preacher" column with the result.
        .with_columns(
            pl.col("title")
            .map_elements(evaluate_preacher, return_dtype=pl.String)
            .alias("preacher")
        )
        # Step 2: Use the newly updated "preacher" column to create the new "title".
        .with_columns(
            pl.when(pl.col("preacher") != "unknown")
            .then(pl.col("preacher") + " " + pl.col("section"))
            .otherwise(pl.col("title"))
            .alias("title")
        )
    )
    # --- End of refactored block ---

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


# message_404 = """<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN">\n<html><head>\n<title>404 Not Found</title>\n</head><body>\n<h1>Not Found</h1>\n<p>The requested URL was not found on this server.</p>\n</body></html>\n"""
