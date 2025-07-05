import pandas as pd
from model_utils import similarity_ratio
from genius_api import clean_lyrics_metadata, search_and_extract_lyrics, setup_genius_client
import pandas as pd
from load_model import get_latest_model, load_saved_model
from model_utils import aggregate_lda_artist_topic_distributions, convert_lda_topic_distributions_to_dense, generate_topic_distributions
from lda_recommendation import LDARecommendationSystem
from gensim.models import LdaModel


def interactive_recommendation():
    """
    Interactive function to get song recommendations.
    Enter artist and song title to get similar songs from our corpus.
    """
    df, lda_artist_topic_distributions, lda_rec_system = setup()
    print("🎵 Welcome to the LDA Song Recommendation System!")
    print("=" * 60)
    artist = input("Enter artist name: ").strip()
    song = input("Enter song title: ").strip()

    if not artist or not song:
        print("❌ Please provide both artist name and song title.")
        return

    print(f"\n🔍 Searching for lyrics: {artist} - {song}")
    print("-" * 50)

    try:
        # Setup Genius client if not already done
        if 'genius_client' not in globals():
            print("🔧 Setting up Genius API client...")
            global genius_client
            genius_client = setup_genius_client()

        # First check if we already have this song in our database
        found_in_db = False
        song_idx = -1

        # Normalize search terms for case-insensitive search
        artist_lower = artist.lower().strip()
        song_lower = song.lower().strip()

        for i, row in df.iterrows():
            db_artist = str(row['artist']).lower().strip()
            db_title = str(row['song_title']).lower().strip()

            # Use similarity check to account for variations in artist/title formatting
            artist_similarity = similarity_ratio(db_artist, artist_lower)
            title_similarity = similarity_ratio(db_title, song_lower)

            if artist_similarity > 0.9 and title_similarity > 0.9:
                found_in_db = True
                song_idx = i
                artist = row['artist']  # Use the exact artist name from DB
                song = row['song_title']  # Use the exact song title from DB
                print(f"✅ Song found in database: {artist} - {song}")
                break

        # Get song data and lyrics
        song_data = None
        lyrics = None

        if found_in_db:
            # Use lyrics from our database
            lyrics = df.iloc[song_idx]['lyrics']
            song_data = {
                'artist': artist,
                'title': song,
                'lyrics': lyrics
            }
            print(f"📝 Using lyrics from database (preview): {lyrics[:80]}...")
        else:
            # Search for lyrics using Genius API
            print("🔍 Song not found in database, fetching from Genius API...")
            song_data = search_and_extract_lyrics(artist, song, genius_client)

            if song_data:
                lyrics = song_data['lyrics']
                lyrics = clean_lyrics_metadata(lyrics)
                print(
                    f"✅ Found lyrics from Genius API (preview): {lyrics[:80]}...")
            else:
                print("❌ Lyrics not found")
                return

        # STEP 1: Predict topics using LDARecommendationSystem
        print("\n📊 ANALYZING SONG TOPICS:")
        prediction_result = lda_rec_system.predict_new_song_topics(
            lyrics, top_n=3)
        print(prediction_result)

        # STEP 2: Find similar songs using the recommendation system
        print("\n🎵 SIMILAR SONGS BY TOPIC:")
        song_recommendations = lda_rec_system.recommend_songs_from_lyrics(
            lyrics,
            artist=artist,
            title=song,
            top_k=5
        )
        print(song_recommendations)

        # STEP 3: Get similar artists based on topic distribution
        print("\n🎤 SIMILAR ARTISTS BY TOPIC:")

        # Check if the artist is in our database
        if artist in lda_artist_topic_distributions:
            # Use the artist name directly for recommendations
            artist_recommendations = lda_rec_system.recommend_artists(
                artist,
                strategy='balanced_mmr',
                top_k=5
            )
        else:
            # Use the lyrics for recommendations if artist isn't in our database
            artist_recommendations = lda_rec_system.recommend_artists_from_lyrics(
                lyrics,
                strategy='balanced_mmr',
                top_k=5,
                lambda_param=0.7  # Balance between relevance and diversity
            )

        print(artist_recommendations)

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()  # This will show the full error trace

        print()


def setup():
    df = pd.read_csv("Lyrics_extraction\scraped_lyrics_no_metadata.csv")
    model_name = get_latest_model()
    loaded = load_saved_model(
        model_name=model_name, model_dir="Experiments\saved_models")

    optimal_model: LdaModel = loaded['model']
    corpus = loaded["corpus"]
    # corpus_topic_distributions = loaded['corpus']
    dictionary = loaded['dictionary']
    topic_labels = loaded['topic_labels']
    metadata = loaded['metadata']
    corpus_topic_distributions = metadata.get(
        'corpus_topic_distributions', None)

    dense_topic_distributions = convert_lda_topic_distributions_to_dense(
        corpus_topic_distributions, optimal_model.num_topics
    )

    # Aggregate to artist level
    lda_artist_topic_distributions, lda_artist_info = aggregate_lda_artist_topic_distributions(
        df, dense_topic_distributions, method='weighted_average'
    )

    lda_rec_system = LDARecommendationSystem(
        optimal_model, dictionary, lda_artist_topic_distributions, lda_artist_info, topic_labels=topic_labels,
        corpus_topic_distributions=corpus_topic_distributions, song_df=df
    )
    # Get user input
    df = pd.read_csv("Lyrics_extraction\scraped_lyrics_no_metadata.csv")
    return df, lda_artist_topic_distributions, lda_rec_system
