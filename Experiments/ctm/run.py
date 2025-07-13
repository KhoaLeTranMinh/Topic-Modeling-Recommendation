from interactive_recommendation import interactive_recommendation
from ctm_recommendation import df

if __name__ == "__main__":
    artist = input("Enter artist name: ").strip()
    song = input("Enter song title: ").strip()
    interactive_recommendation(df, artist=artist, song=song)
