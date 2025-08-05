from bs4 import BeautifulSoup
import requests
from datetime import datetime
import chromadb
from tqdm import tqdm

import web_scraper_helpers as wsh


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
                    "raw_video_url": tag["href"],
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
                    "raw_video_url": tag["href"],
                }
            )
    return records


def get_mp4_url_from_video_url(video_url: str):
    html_content = requests.get(video_url)
    soup = BeautifulSoup(html_content, "html.parser")
    mp4_url = soup.find("video").get_attribute_list("src")
    mp4_url = mp4_url[0]
    return mp4_url


# def write_df_to_mongodb(
#     df: pd.DataFrame, collection: Collection, operation_text: str, data_source_name: str
# ):
#     log = "start: " + str(datetime.now())
#     log += "\n"
#     try:
#         log += "began {operation}:\n\tfrom: {data_source_name}\n\tinto:\n\t\thost: {mongodb_host}\n\t\tdatabase: {mongodb_db}\n\t\tcollection: {mongodb_collection_name}\n".format(
#             operation=operation_text,
#             data_source_name=data_source_name,
#             mongodb_host=str(collection.database.client.address),
#             mongodb_db=str(collection.database.name),
#             mongodb_collection_name=collection.name,
#         )

#         start_count_documents = str(collection.count_documents({}))
#         log += "start_count_documents: " + start_count_documents + "\n"

#         if df.size != 0:
#             result = collection.insert_many(df.to_dict(orient="records"))

#             acknowledged = str(result.acknowledged)
#             log += "acknowledged: " + acknowledged + "\n"

#         end_count_documents = str(collection.count_documents({}))
#         log += "end_count_documents: " + end_count_documents + "\n"
#     except Exception as e:
#         log += "failed {operation}:\n\tfrom: {data_source_name}\n\tinto:\n\t\thost: {mongodb_host}\n\t\tdatabase: {mongodb_db}\n\t\tcollection: {mongodb_collection_name}\n".format(
#             operation=operation_text,
#             data_source_name=data_source_name,
#             mongodb_host=str(collection.database.client.address),
#             mongodb_db=str(collection.database.name),
#             mongodb_collection_name=collection.name,
#         )
#         log += str(e) + "\n"
#     log += "end: " + str(datetime.now())
#     log += "\n"
#     log += "\n"
#     return log


# def read_df_from_mongodb(collection: Collection):
#     log = "start: " + str(datetime.now())
#     log += "\n"
#     try:
#         log += "began reading:\n\tfrom: \n\t\thost: {mongodb_host}\n\t\tdatabase: {mongodb_db}\n\t\tcollection: {mongodb_collection}\n".format(
#             mongodb_host=str(collection.database.client.address),
#             mongodb_db=str(collection.database.name),
#             mongodb_collection=collection.name,
#         )

#         count_documents = str(collection.count_documents({}))
#         log += "start_count_documents: " + count_documents + "\n"

#         df = pd.DataFrame(list(collection.find({})))
#     except Exception as e:
#         log += "failed reading:\n\tfrom: \n\t\thost: {mongodb_host}\n\t\tdatabase: {mongodb_db}\n\t\tcollection: {mongodb_collection}\n".format(
#             mongodb_host=str(collection.database.client.address),
#             mongodb_db=str(collection.database.name),
#             mongodb_collection=collection.name,
#         )
#         log += str(e) + "\n"
#     log += "end: " + str(datetime.now())
#     log += "\n"
#     log += "\n"
#     return df, log


# def write_df_to_csv(file_path, df: pd.DataFrame):
#     with open(file_path, "w") as csv_file:
#         csv_file.write(df.to_csv())


# def filter_is_video(df: pd.DataFrame):
#     in_df = df[
#         df["raw_video_url"].str.contains(r"video\.php\?id\=", na=False) == True
#     ].copy()
#     out_df = df[
#         df["raw_video_url"].str.contains(r"video\.php\?id\=", na=False) == False
#     ].copy()
#     if not out_df.empty:
#         out_df.loc[:, "failure_point"] = (
#             'raw_video_url "'
#             + df["raw_video_url"].astype(str)
#             + '" does not contain "video.php?id="'
#         )
#         out_df.loc[:, "failure_time"] = str(datetime.now())
#     return in_df, out_df


# def raw_video_url_to_video_url(
#     raw_video_url: str, atp_videos_base_url: str = "https://allthepreaching.com/pages/"
# ):
#     video_url = atp_videos_base_url.strip() + raw_video_url.strip()
#     return video_url


# def filter_sections(df: pd.DataFrame, disallowed_sections: list):
#     in_df = df[~df["section"].isin(disallowed_sections)].copy()
#     out_df = df[df["section"].isin(disallowed_sections)].copy()
#     if not out_df.empty:
#         out_df.loc[:, "failure_point"] = (
#             'section "'
#             + df["section"].astype(str)
#             + '" was in disallowed_sections list:\n'
#             + str(disallowed_sections)
#         )
#         out_df.loc[:, "failure_time"] = str(datetime.now())
#     return in_df, out_df


# def filter_title_ne_text(df: pd.DataFrame):
#     in_df = df[df["title"] == df["text"]].copy()
#     out_df = df[df["title"] != df["text"]].copy()
#     if not out_df.empty:
#         out_df.loc[:, "failure_point"] = (
#             'title "'
#             + df["title"].astype(str)
#             + '" is not equal to text "'
#             + df["text"].astype(str)
#             + '"'
#         )
#         out_df.loc[:, "failure_time"] = str(datetime.now())
#     return in_df, out_df


# def filter_join_on_section_preacher(
#     df: pd.DataFrame, section_preacher_df: pd.DataFrame
# ):
#     in_df = pd.merge(df, section_preacher_df, on="section")
#     in_df = in_df[
#         ["preacher", "section", "title", "text", "raw_video_url", "video_url"]
#     ]
#     out_df = df[~df["section"].isin(in_df["section"])].copy()
#     if not out_df.empty:
#         out_df.loc[:, "failure_point"] = (
#             'df section"'
#             + df["section"].astype(str)
#             + '" was not in section_preacher_df sections:\n'
#             + section_preacher_df["section"].astype(str)
#         )
#         out_df.loc[:, "failure_time"] = str(datetime.now())
#     return in_df, out_df


# def replace_sermon_section(df: pd.DataFrame):
#     df["section"] = df["section"].str.replace("sermon clips", "sermon clips ")
#     df["section"] = df["section"].str.replace("sermonsp", "sermons p")
#     df["section"] = df["section"].str.replace("sermonsb", "sermons b")
#     return df


# def populate_preacher_field(
#     df: pd.DataFrame, preacher_titles: list, preacher_names: list
# ):
#     def extract_name_from_title(title, names):
#         for name in names:
#             if name in title:
#                 return name
#         return title

#     evaluate_preacher_df = df[df["preacher"] == "evaluate"].copy()
#     evaluate_preacher_df["temp"] = evaluate_preacher_df["title"].str.strip(
#         to_strip="1234567890 -./"
#     )
#     evaluate_preacher_df = evaluate_preacher_df[
#         evaluate_preacher_df["temp"].str.contains("|".join(preacher_titles), na=False)
#     ]
#     evaluate_preacher_df["temp"] = evaluate_preacher_df["temp"].apply(
#         lambda t: extract_name_from_title(t, preacher_names)
#     )
#     evaluate_preacher_df["preacher"] = evaluate_preacher_df["temp"]
#     evaluate_preacher_df["title"] = (
#         evaluate_preacher_df["title"] + " " + evaluate_preacher_df["section"]
#     )
#     df.update(evaluate_preacher_df)
#     return df


# def get_mp4_url_and_status_code_into_mongodb(df: pd.DataFrame, collection: Collection):
#     for index, row in tqdm(df.iterrows(), total=len(df), mininterval=0.2):
#         if collection.count_documents({"raw_video_url": row.raw_video_url}) > 0:
#             continue
#         mp4_url, status_code = wsh.get_mp4_url_and_code_from_video_url(row.video_url)
#         df.loc[index, "mp4_url"] = mp4_url
#         df.loc[index, "mp4_status_code"] = status_code
#         collection.insert_one(df.loc[index].to_dict())
#         # print(str(collection.count_documents({}))+'/'+str(len(df)))
#         # time.sleep(np.random.randint(0,2)) # row delay


# def get_mp4_url_from_html_content(html_content: str):
#     soup = BeautifulSoup(html_content, "html.parser")
#     mp4_url = soup.find("video").get_attribute_list("src")
#     mp4_url = mp4_url[0]
#     return mp4_url


# def get_mp4_url_and_code_from_video_url(video_url: str):
#     response = requests.get(video_url)
#     html_content = response.text
#     status_code = response.status_code
#     mp4_url = wsh.get_mp4_url_from_html_content(html_content)
#     return mp4_url, status_code


# def convert_mp4_url_to_vtt_url(mp4_url: str):
#     vtt_url = mp4_url.replace(".mp4", ".vtt")
#     return vtt_url


# def get_content_and_status_code_from_url(url: str):
#     response = requests.get(url)
#     content = response.text
#     status_code = response.status_code
#     return content, status_code
