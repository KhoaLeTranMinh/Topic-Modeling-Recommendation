import os
import re

from dotenv import load_dotenv
import lyricsgenius
import pandas as pd


def setup_genius_client():
    """
    Setup Genius API client with proper error handling
    """
    # Load environment variables
    env_path = os.path.join(os.path.dirname(
        os.path.dirname(os.path.abspath('.'))), '.env')
    load_dotenv(env_path, override=True, encoding='utf-8')

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
            'title': song.title,
            'artist': song.artist,
            'lyrics': lyrics,
            'url': song.url
        }

    except Exception as e:
        print(f"❌ Error searching for song: {e}")
        return None


def clean_lyrics_metadata(lyrics_text):
    """
    Clean lyrics by finding the first structural marker [Something] and keeping everything from there.

    Args:
        lyrics_text (str): Raw lyrics text with metadata

    Returns:
        str: Cleaned lyrics starting from first structural marker
    """
    if pd.isna(lyrics_text) or not lyrics_text:
        return ""

    text = str(lyrics_text)

    # Find the first occurrence of specific structural markers containing: Intro, Chorus, or Verse 1
    # This pattern allows additional characters within brackets but requires one of the three key terms
    pattern = r'\[(Intro|Chorus|Verse|Pre-Chorus|Bridge).*?\]'
    match = re.search(pattern, text)

    if match:
        # Get the starting position of the first structural marker
        start_index = match.start()
        # Return everything from this point onward
        cleaned_text = text[start_index:].strip()
        return cleaned_text
    else:
        # If no structural marker found, return the original text
        # (this handles edge cases where songs might not have standard structure)
        return text.strip()
