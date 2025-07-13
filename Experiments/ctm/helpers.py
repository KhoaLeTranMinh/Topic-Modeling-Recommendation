# Helper functions
from difflib import SequenceMatcher
import os
import re
import warnings
from contextualized_topic_models.models.ctm import CombinedTM
from dotenv import load_dotenv
import lyricsgenius
import numpy as np
import pandas as pd
from contextualized_topic_models.utils.preprocessing import (
    WhiteSpacePreprocessingStopwords,
)
from scipy.spatial.distance import cosine
warnings.filterwarnings("ignore")


def clean_lyrics_for_ctm(text):
    # Remove structural markers like [Chorus], [Verse], etc.
    text = re.sub(r'\[.*?\]', '', text)

    # Clean up whitespace and newlines
    text = re.sub(r'\n+', ' ', text)  # Replace newlines with spaces
    text = re.sub(r'\s+', ' ', text)  # Normalize whitespace

    return text.strip()


def setup_genius_client():
    """
    Setup Genius API client with proper error handling
    """
    # Load environment variables
    env_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath("."))), ".env"
    )
    load_dotenv(env_path, override=True, encoding="utf-8")

    # Get API token
    GENIUS_API_TOKEN = os.getenv("GENIUS_API_TOKEN")
    if not GENIUS_API_TOKEN:
        print("⚠️  Warning: GENIUS_API_TOKEN not found in .env file")
        print("Please create a .env file with your Genius API token:")
        print("GENIUS_API_TOKEN=your_token_here")
        return None

    # Initialize Genius client
    try:
        genius = lyricsgenius.Genius(
            GENIUS_API_TOKEN,
            timeout=15,
            retries=3,
            sleep_time=0.25,
            excluded_terms=["(Remix)", "(Live)"],
            skip_non_songs=True,
        )
        print("✓ Genius API client initialized successfully")
        return genius
    except Exception as e:
        print(f"❌ Error initializing Genius client: {e}")
        return None


def search_and_extract_lyrics(artist_name, song_title, genius_client=None):
    """
    Search for a song and extract its lyrics using Genius API
    """
    if genius_client is None:
        genius_client = setup_genius_client()
        if genius_client is None:
            return None

    try:
        print(f"🔍 Searching for: '{song_title}' by {artist_name}")

        # Search for the song
        song = genius_client.search_song(title=song_title, artist=artist_name)

        if song is None:
            print(f"❌ Song not found: '{song_title}' by {artist_name}")
            return None

        print(f"✓ Found: '{song.title}' by {song.artist}")

        # Extract lyrics
        lyrics = song.lyrics
        if not lyrics:
            print("❌ No lyrics found for this song")
            return None

        print(f"✓ Lyrics extracted successfully ({len(lyrics)} characters)")
        return {
            "title": song.title,
            "artist": song.artist,
            "lyrics": lyrics,
            "url": song.url,
        }

    except Exception as e:
        print(f"❌ Error searching for song: {e}")
        return None


def clean_lyrics_metadata(lyrics_text):
    """
    Clean lyrics by finding the first structural marker [Something] or "Read More" and keeping everything from there.

    Args:
        lyrics_text (str): Raw lyrics text with metadata

    Returns:
        str: Cleaned lyrics starting from first structural marker or after "Read More"
    """
    if pd.isna(lyrics_text) or not lyrics_text:
        return ""

    text = str(lyrics_text)

    # Pattern 1: Find "Read More" and take everything after it
    read_more_pattern = r"read more\s*"
    read_more_match = re.search(read_more_pattern, text, re.IGNORECASE)

    # Pattern 2: Find structural markers [Something]
    structural_pattern = r"\[(Intro|Chorus|Verse|Pre-Chorus|Bridge|.*).*?\]"
    structural_match = re.search(structural_pattern, text)

    # Pattern 3: Find "Lyrics" followed by any alphabetic character
    lyrics_pattern = r"Lyrics(?=[\S])"
    lyrics_match = re.search(lyrics_pattern, text)

    read_more_pos = read_more_match.start() if read_more_match else None
    structural_pos = structural_match.start() if structural_match else None
    lyrics_pos = lyrics_match.start() if lyrics_match else None

    if read_more_pos:
        # If "Read More" appears => just take everything after it
        start_index = read_more_match.end()
        cleaned_text = text[start_index:].strip()
        return cleaned_text
    elif structural_match:
        # Structural marker appears first - take everything from it
        start_index = structural_match.start()
        cleaned_text = text[start_index:].strip()
        return cleaned_text
    elif lyrics_pos:
        # "Lyrics" appears first - take everything from it
        start_index = lyrics_match.end()
        cleaned_text = text[start_index:].strip()
        return cleaned_text
    else:
        # Neither pattern found, return original text
        return text.strip()


def similarity_ratio(str1, str2):
    """Calculate similarity ratio between two strings"""
    return SequenceMatcher(None, str1, str2).ratio()


# Helper function to calculate topic similarity
