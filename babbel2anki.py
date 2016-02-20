#!/usr/env python3

import os
import csv
import math
import asyncio
import logging
import requests
import jsonschema

from concurrent.futures import ThreadPoolExecutor


logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Babbel settings and urls
vocabulary_url = ("https://api.babbel.com/v3/en_GB/learn_languages/{language}/"
                  "trainer_items/search")
media_urls = {
    "image": "https://d3hmols351pw2y.cloudfront.net/image/400x400/{}.jpg",
    "audio": "https://d3hmols351pw2y.cloudfront.net/sound/{}.mp3"
}
schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",

    "definitions": {
        "review": {
            "type": "object",
            "properties": {
                "trainer_items": {"type": "array"},
                "total_filtered_items": {"type": "number"},
                "per_page_limit": {"type": "number"},
                "page": {"type": "number"}
            },
            "required": ["trainer_items", "total_filtered_items",
                         "per_page_limit", "page"]
        }
    },

    "type": "object",
    "properties": {
        "review": {"$ref": "#/definitions/review"}
    }
}

# User specific settings
language = "RUS"
search_params = {"auth_token": "JLjhRr55_B--FNRYMgL-", "page": None,
                 "_": "1453198563056"}
anki_folder = "/home/jsmith/Documents/Anki/JSmith"
csv_filename = os.path.join(anki_folder, "babbel2anki.csv")
media_directory = os.path.join(anki_folder, "collection.media")

# Application settings
executor = ThreadPoolExecutor(20)
media_filenames = {"image": "rus_{}.jpg", "audio": "rus_{}.mp3"}
media_entries = {"image": '<img src="{}">', "audio": "[sound:{}]"}
media_id_fields = {"image": "image_id", "audio": "sound_id"}
media_csv_fields = {"image": "image_path", "audio": "audio_path"}
csv_fieldnames = ["id", "type", "image_id", "image_path", "sound_id",
                  "audio_path", "l1_text", "l2_text",
                  "info_text"]


def select_new_vocabulary(vocabulary):
    """Selects the vocabulary items which do not currently exist in the csv.
    """
    logger.info("Filtering pre-existing vocabulary items...")
    try:
        with open(csv_filename, 'r') as csv_file:
            reader = csv.reader(csv_file, dialect="unix")
            existing_ids = [row[0] for row in reader]
            new_vocabulary = [vocab_item for vocab_item in vocabulary if
                              str(vocab_item["id"]) not in existing_ids]
    except FileNotFoundError:
        existing_ids = []
        new_vocabulary = list(vocabulary)
    logger.info("Identified %d vocabulary items not in the exsiting %d items.",
                len(new_vocabulary), len(existing_ids))
    return new_vocabulary


def write_csv(vocabulary):
    """Write the csv entries out to file."""
    # Clean up the l2_text of the parentheses used by babbel
    for vocab_item in vocabulary:
        for char in [")", "("]:
            vocab_item["l2_text"] = vocab_item["l2_text"].replace(char, "")
    # Write to the csv
    with open(csv_filename, 'a') as csv_file:
        logger.info("Writing csv items to file...")
        writer = csv.DictWriter(csv_file, csv_fieldnames, dialect="unix",
                                extrasaction="ignore")
        writer.writerows(vocabulary)
        logger.info("Wrote %d new rows to the csv file.", len(vocabulary))


@asyncio.coroutine
def download_media(vocabulary, loop):
    """Download all the audio and images for the items in the vocabulary list.
    """
    for media_type in ("image", "audio"):
        existing_media_count = 0
        logger.info("Downloading all media of type %s...", media_type)
        coroutines = [download_media_item(
            vocab_item[media_id_fields[media_type]], media_type, loop) for
            vocab_item in vocabulary]
        results = yield from asyncio.gather(*coroutines, loop=loop)
        for vocab_item, filename_tuple in zip(vocabulary, results):
            vocab_item[media_csv_fields[media_type]] = media_entries[
                media_type].format(filename_tuple[0])
            # Check if the file had already existed
            if filename_tuple[1]:
                existing_media_count += 1
        logger.info("%d of %d %s files already existed.",
                    existing_media_count, len(results), media_type)


@asyncio.coroutine
def download_media_item(media_id, media_type, loop):
    """Downloads audio and image media associated with the words & phrases."""
    assert media_type == "audio" or media_type == "image", "Invalid type"
    media_url = media_urls[media_type].format(media_id)
    media_filename = media_filenames[media_type].format(media_id)
    media_path = os.path.join(media_directory, media_filename)
    file_exists = os.path.isfile(media_path)
    if not file_exists:
        routine = loop.run_in_executor(executor, requests.get, media_url)
        request_object = yield from routine
        with open(media_path, 'wb') as media_file:
            media_file.write(request_object.content)
    return media_filename, file_exists


@asyncio.coroutine
def get_babbel_vocabulary_page(page_number, vocabulary, loop):
    """Get the vocabulary of a particular page and appends it to the vocabulary
    list.

    Returns: The total number of pages.
    """
    def handle_request():
        search_params["page"] = page_number
        return requests.get(vocabulary_url.format(language=language),
                            params=search_params)
    # Get and validate the json data from the server
    routine = loop.run_in_executor(executor, handle_request)
    request_object = yield from routine
    response = request_object.json()
    jsonschema.validate(response, schema)
    response = response["review"]
    # Extend the list
    vocabulary.extend(response["trainer_items"])
    # Return the number of pages
    return math.ceil(response["total_filtered_items"] /
                     response["per_page_limit"])


@asyncio.coroutine
def get_babbel_vocabulary(loop):
    """Get the list of vocabulary words from babbel."""
    vocabulary = []
    logger.info("Downloading list of vocabulary items...")
    num_pages = yield from get_babbel_vocabulary_page(1, vocabulary, loop)
    if num_pages > 1:
        coroutines = [get_babbel_vocabulary_page(page, vocabulary, loop)
                      for page in range(2, num_pages+1)]
        yield from asyncio.gather(*coroutines, loop=loop)
    logger.info("Downloaded %d items over %d page(s).", len(vocabulary),
                num_pages)
    return vocabulary


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    vocabulary = loop.run_until_complete(get_babbel_vocabulary(loop))
    vocabulary = select_new_vocabulary(vocabulary)
    if vocabulary:
        loop.run_until_complete(download_media(vocabulary, loop))
        write_csv(vocabulary)
