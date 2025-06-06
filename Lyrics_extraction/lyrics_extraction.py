import os
import pandas as pd
import lyricsgenius
import time
import random
from dotenv import load_dotenv

# -----------------------------
# CONFIGURATION
# -----------------------------

# Load environment variables from .env file (using absolute path for reliability)
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath('__file__'))), '.env')
load_dotenv(env_path, override=True, encoding='utf-8')

# 1) Genius API Token
GENIUS_API_TOKEN = os.getenv("GENIUS_API_TOKEN")
if not GENIUS_API_TOKEN:
    raise ValueError("GENIUS_API_TOKEN not found in .env file")

# 2) Path to your artist list (one artist name per line)
ARTIST_LIST_PATH = os.path.join(os.path.dirname(os.path.abspath('__file__')), 'Lyrics_extraction/artists.txt')


# 3) Output CSV (where we'll append results as we go)
OUTPUT_CSV = os.path.join(os.path.dirname(os.path.abspath('__file__')), 'Lyrics_extraction/scraped_lyrics.csv')


# 4) How many songs to fetch per artist
SONGS_PER_ARTIST = int(os.getenv("SONGS_PER_ARTIST", "25"))
print(f"Will fetch up to {SONGS_PER_ARTIST} songs per artist")

# 5) Pause (seconds) between artist requests to avoid rate-limiting
SLEEP_BETWEEN = float(os.getenv("SLEEP_BETWEEN", "1.5"))
print(f"Will sleep {SLEEP_BETWEEN} seconds between artist requests")

# 6) Rate limit handling configuration
INITIAL_BACKOFF = int(os.getenv("INITIAL_BACKOFF", 10))  # Start with 10 seconds
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))       # Try up to 5 times

print(f"Using initial backoff of {INITIAL_BACKOFF}s with {MAX_RETRIES} max retries")
# -----------------------------
# INITIALIZE GENIUS CLIENT
# -----------------------------

# Initialize lyricsgenius.Genius with some options
genius = lyricsgenius.Genius(
    GENIUS_API_TOKEN,
    timeout=15,
    retries=3,
    sleep_time=0.5  # small pause between each page scrape
)
genius.remove_section_headers = True
genius.skip_non_songs = True
genius.excluded_terms = ["(Remix)", "(Live)"]

# -----------------------------
# RATE LIMIT HANDLER
# -----------------------------
def with_rate_limit_handling(api_function):
    """Decorator to handle rate limit errors with exponential backoff"""
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_RETRIES + 1):
            try:
                return api_function(*args, **kwargs)
            except Exception as e:
                error_str = str(e)
                # Check if it's a rate limit error
                if "429" in error_str and attempt < MAX_RETRIES:
                    # Calculate backoff time with jitter
                    backoff_time = INITIAL_BACKOFF * (2 ** attempt) + random.uniform(1, 5)
                    print(f"\nRate limit exceeded. Waiting {backoff_time:.1f} seconds before retry {attempt+1}/{MAX_RETRIES}")
                    time.sleep(backoff_time)
                else:
                    if "429" in error_str:
                        print(f"\nRate limit exceeded after {MAX_RETRIES} retries. Consider increasing wait time.")
                    raise
    return wrapper

# -----------------------------
# HELPER FUNCTION: fetch_artist_lyrics
# -----------------------------
@with_rate_limit_handling
def search_artist(artist_name, max_songs):
    """Search for an artist with rate limit handling"""
    return genius.search_artist(artist_name, max_songs=max_songs, sort="popularity")

@with_rate_limit_handling
def search_song(title, artist):
    """Search for a song with rate limit handling"""
    return genius.search_song(title=title, artist=artist)

def fetch_artist_lyrics(artist_name, max_songs=SONGS_PER_ARTIST):
    """
    Fetch up to max_songs tracks for `artist_name`, returning a list of dicts
    """
    songs_data = []
    try:
        # Search for the artist with rate limit handling
        artist_obj = search_artist(artist_name, max_songs)
        
        if artist_obj is None or not artist_obj.songs:
            print(f"  → No songs found for artist: {artist_name}")
            return songs_data

        for song in artist_obj.songs:
            title = song.title.strip()
            lyrics = song.lyrics.strip()
            # Skip extremely short lyrics (e.g., < 20 chars)
            if len(lyrics) < 20:
                continue
            songs_data.append({
                "artist": artist_name,
                "song_title": title,
                "lyrics": lyrics
            })
            
    except Exception as e:
        print(f"ERROR: Could not search for artist [{artist_name}]: {e}")
        
    return songs_data

# -----------------------------
# MAIN SCRAPE LOOP
# -----------------------------
def main():
    # 1) Read existing CSV (if any), so we don't re‐scrape duplicates
    if os.path.exists(OUTPUT_CSV):
        master_df = pd.read_csv(OUTPUT_CSV)
        # Create a set of (artist, song_title) for quick "already scraped" checks
        existing_pairs = set(zip(master_df["artist"], master_df["song_title"]))
        print(f"Loaded {len(master_df)} existing rows from {OUTPUT_CSV}")
    else:
        master_df = pd.DataFrame(columns=["artist", "song_title", "lyrics"])
        existing_pairs = set()
        print(f"No existing CSV found. A new one will be created: {OUTPUT_CSV}")

    # 2) Read artist list
    with open(ARTIST_LIST_PATH, "r", encoding="utf-8") as f:
        artists = [line.strip() for line in f if line.strip()]
    print(f"Read {len(artists)} artists from {ARTIST_LIST_PATH}")

    # 3) Loop over each artist
    for idx, artist_name in enumerate(artists, 1):
        print(f"[{idx}/{len(artists)}] Scraping artist: {artist_name} ", end="")
        fetched = fetch_artist_lyrics(artist_name, max_songs=SONGS_PER_ARTIST)

        # Filter out any (artist, song) pairs we already have
        new_rows = []
        for item in fetched:
            key = (item["artist"], item["song_title"])
            if key in existing_pairs:
                continue
            new_rows.append(item)
            existing_pairs.add(key)

        # 4) Append new_rows to master_df (and save immediately)
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            master_df = pd.concat([master_df, new_df], ignore_index=True)

            # Save after each artist to avoid data loss if script crashes
            master_df.to_csv(OUTPUT_CSV, index=False)
            print(f"→ Retrieved {len(new_rows)} new songs (total now {len(master_df)})")
        else:
            print("→ No new songs found or all songs already exist.")

        # 5) Sleep to avoid hitting rate limits
        time.sleep(SLEEP_BETWEEN)

    print("Scraping complete.")
    print(f"Final row count: {len(master_df)}")
    print(f"Distinct artists in CSV: {master_df['artist'].nunique()}")
    print(f"Distinct songs in CSV:   {master_df['song_title'].nunique()}")

if __name__ == "__main__":
    main()