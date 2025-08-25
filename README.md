# All The Preaching - Web Scraping Pipeline

This repository contains the code for a data pipeline that scrapes, cleans, and processes sermon transcripts from "All The Preaching". The ultimate goal is to embed these transcripts into a vector database to power search and retrieval functionalities.

## Overview

The pipeline is designed to be a robust and maintainable solution for ingesting new sermon transcripts as they become available on the source website. It leverages modern data processing and validation libraries to ensure data quality and consistency throughout the process.

## Features

*   **Web Scraping**: Automatically scrapes sermon metadata and transcripts from "All The Preaching".
*   **Data Cleaning**: Cleans and standardizes fields such as title, section, and preacher.
*   **Data Validation**: Utilizes `patito` to define and enforce data models, ensuring the integrity of the data at each stage of the pipeline.
*   **Transcript Processing**: Converts VTT (WebVTT) transcript files into clean, usable text.
*   **Vector Embedding Preparation**: The pipeline prepares the data for embedding into a vector database like ChromaDB, which is ideal for semantic search and retrieval in AI applications.

## Pipeline Workflow

The data processing workflow is orchestrated in `main.py` and is composed of the following key steps:

1.  **Initial Scrape**: The pipeline starts by scraping the main archive page of "All The Preaching" to gather a list of available sermons, including their titles, sections, and video URLs.
2.  **Pre-Scraping Cleanup and Validation**: The initially scraped data is cleaned and validated against the `PreScrapingDataFrameModel`. This step involves:
    *   Extracting a unique `video_id` from the URL.
    *   Filtering out irrelevant sections.
    *   Standardizing section and title names.
    *   Inferring the preacher's name from the section or title.
    *   Ensuring no duplicate videos are processed.
3.  **Detailed Scraping**: For each new video, the pipeline navigates to its individual page to scrape the `mp4_url`. From the `mp4_url`, it then deduces the URLs for the `mp3` and `vtt` (transcript) files.
4.  **Transcript Retrieval and Conversion**: The `vtt` file is downloaded, and its content is converted from WebVTT format into plain text.
5.  **Final Validation**: The complete record, now including the transcript and all associated metadata, is validated against the `TranscriptDataFrameModel`. This model ensures all URLs are correctly formatted and that the transcript content is present and valid. It also generates a hash of the transcript to easily detect and filter out duplicates.
6.  **Chunking (Future Step)**: The `ChunkedRecordDataFrameModel` is defined in preparation for a future step where the full transcripts will be broken down into smaller chunks, a necessary step for effective vector embedding and retrieval.

## Project Structure

```
.
├── main.py
├── helpers.py
├── config.py
└── data/
```

*   **`main.py`**: The main entry point of the application. It orchestrates the entire scraping and cleaning pipeline from start to finish.
*   **`helpers.py`**: This file is the core of the pipeline, containing all the necessary functions for scraping, data transformation, and validation. It defines the `patito` data models that ensure data quality.
*   **`config.py`**: A configuration file that holds dictionaries and lists used for mapping and cleaning data fields like titles, sections, and preacher names. This separation of configuration from logic makes the pipeline easier to maintain and adapt.
*   **`data/`**: A directory intended to hold data, such as the HTML archive files used for testing and development.

## Data Models

The pipeline's data integrity is enforced by a series of models defined in `helpers.py` using the `patito` library. `patito` is a data validation framework that combines the declarative style of `pydantic` with the high-performance `polars` DataFrame library.

*   **`PreScrapingDataFrameModel`**: Validates the initial data scraped from the archive page. It ensures that essential fields like `video_id`, `section`, `title`, `preacher`, and `video_url` are present and correctly formatted.
*   **`TranscriptDataFrameModel`**: Validates the complete record after the transcript has been scraped and processed. It inherits from the pre-scraping model and adds validation for media URLs (`mp4`, `mp3`, `vtt`) and the transcript itself.
*   **`ChunkedRecordDataFrameModel`**: A model prepared for the future implementation of transcript chunking. It will validate the format of individual text chunks before they are embedded.

## Setup and Usage

1.  **Configuration**: Populate the dictionaries and lists in `config.py` with the necessary mappings for sections, titles, and preachers. You will also need to provide a list of `existing_video_ids` to prevent reprocessing of already scraped content.

2.  **Running the Pipeline**: Execute the `main.py` script to start the web scraping and data processing pipeline.

    ```bash
    python main.py
    ```

## Dependencies

The project relies on the following major Python libraries:

*   **`polars`**: A blazingly fast DataFrame library for data manipulation.
*   **`patito`**: For data model definition and validation.
*   **`requests`**: For making HTTP requests to the website.
*   **`BeautifulSoup4`**: For parsing HTML content.
*   **`webvtt-py`**: For parsing WebVTT files.
*   **`chromadb`**: Mentioned as the likely target for the final vector embeddings.
