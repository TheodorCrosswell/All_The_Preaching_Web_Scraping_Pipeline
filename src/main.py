import polars as pl
import patito as pt
import time
import tqdm
import helpers
from config import *


# all_links = helpers.get_records_from_archive_url()
test_links_1 = helpers.get_records_from_html_file(
    r"C:\repos\All_The_Preaching_Web_Scraping_Pipeline\data\test_archive_1.html"
)
test_links_2 = helpers.get_records_from_html_file(
    r"C:\repos\All_The_Preaching_Web_Scraping_Pipeline\data\test_archive_2.html"
)
test_links_3 = helpers.get_records_from_html_file(
    r"C:\repos\All_The_Preaching_Web_Scraping_Pipeline\data\test_archive_3.html"
)
print(len(test_links_1))
print(len(test_links_2))
print(len(test_links_3))

test_links_all = helpers.get_records_from_html_file(
    r"C:\repos\All_The_Preaching_Web_Scraping_Pipeline\data\atp_archive_2025-07-07.html"
)
print(len(test_links_all))
print(test_links_all[0])

section_preacher_df = (
    pl.from_dicts(section_preacher_map)
    .transpose(include_header=True)
    .rename({"column": "section", "column_0": "preacher"})
    .lazy()
)

df = (
    helpers.PreScrapingDataFrameModel.DataFrame(test_links_1)
    .lazy()
    .filter(~pl.col("section").is_in(disallowed_sections))
    .with_columns(
        pl.col("raw_video_url")
        .str.extract(
            r"id=(\d+)", 1
        )  # This regex looks for 'id=' followed by one or more digits (\d+)
        .cast(int)
        .alias("video_id")
    )
    .filter(~pl.col("video_id").is_in(existing_video_ids))
    .with_columns(
        [
            (
                pl.lit("https://allthepreaching.com/pages/video.php?id=")
                + pl.col("video_id").cast(str)
            ).alias("video_url"),
            pl.col("section"),
        ]
    )
    .drop("raw_video_url")
)
for pattern, replacement in section_replacements.items():
    df = df.with_columns(pl.col("section").str.replace_all(pattern, replacement))
for pattern, replacement in title_replacements.items():
    df = df.with_columns(pl.col("title").str.replace_all(pattern, replacement))
df = df.join(section_preacher_df, pl.col("section")).collect()
print(df)
try:
    df.validate()
except pt.DataFrameValidationError as e:
    print(e)
    # raise(e)

scraping_df = pl.DataFrame()

video_urls = df.get_column("video_url")

video_urls = video_urls[
    :5
]  # TODO temporary. remove when ready for production. This limits the amount of web requests to 5

mp4_urls = []
mp3_urls = []
vtt_urls = []
vtts = []
transcripts = []
for video_url in tqdm(video_urls, total=len(video_urls)):
    mp4_url = helpers.get_mp4_url_from_video_url(video_url)
    mp3_url = mp4_url.replace(".mp4", ".mp3")
    vtt_url = mp4_url.replace(".mp4", ".vtt")
    vtt = helpers.get_html_content_from_url(vtt_url)
    transcript = helpers.vtt_to_text(vtt)

    mp4_urls.append(mp4_url)
    mp3_urls.append(mp3_url)
    vtt_urls.append(vtt_url)
    vtts.append(vtt)
    transcripts.append(transcript)
    time.sleep(1)

new_rows = pl.DataFrame(
    {
        "video_url": video_urls,
        "mp4_url": mp4_urls,
        "mp3_url": mp3_urls,
        "vtt_url": vtt_urls,
        "vtt": vtts,
        "transcript": transcripts,
    }
)
scraping_df = pl.concat([scraping_df, new_rows])


scraping_df = scraping_df.with_columns(
    [
        pl.col("mp4_url").str.replace(r".mp4", ".mp3").alias("mp3_url"),
        pl.col("mp4_url").str.replace(r".mp4", ".vtt").alias("vtt_url"),
    ]
)

df = helpers.CleanTranscriptDataFrameModel.DataFrame(df).join(
    scraping_df, pl.col("video_url")
)

print(df)

try:
    df.validate()
except pt.DataFrameValidationError as e:
    print(e)
    # raise (e)
