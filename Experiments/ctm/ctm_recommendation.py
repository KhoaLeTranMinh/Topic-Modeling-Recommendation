from contextualized_topic_models.models.ctm import CombinedTM
import numpy as np
import pandas as pd
from scipy.spatial.distance import cosine
from load_model import load_model, get_latest_model_name
from helpers import clean_lyrics_metadata, similarity_ratio, clean_lyrics_for_ctm
from contextualized_topic_models.utils.data_preparation import TopicModelDataPreparation
from contextualized_topic_models.utils.preprocessing import (
    WhiteSpacePreprocessingStopwords,
)


class CTMRecommendationSystem:
    """
    Complete LDA-based music recommendation system
    """

    def __init__(
        self,
        ctm_model: CombinedTM,
        artist_topic_distributions,
        artist_info,
        topic_labels,
        stopwords,
        tp: TopicModelDataPreparation,
        song_topic_distributions=None,
        song_df=None,

    ):
        self.ctm_model = ctm_model
        self.artist_distributions = artist_topic_distributions
        self.artist_info = artist_info
        self.topic_labels = topic_labels
        self.num_topics = ctm_model.n_components
        # Store corpus distributions and song dataframe for song recommendations
        self.tp = tp
        self.stopwords = stopwords
        self.song_topic_distributions = song_topic_distributions
        self.song_df = song_df

    def process_new_document(self, ctm: CombinedTM, text, handle_oov="ignore"):
        """_summary_
                process new lyric and return topic distribution with some word coverage info
            """
        document = clean_lyrics_for_ctm(text)
        document = clean_lyrics_metadata(document)
        document = [document]

        sp = WhiteSpacePreprocessingStopwords(
            documents=document, stopwords_list=self.stopwords)
        preprocessed_documents, unpreprocessed_documents, text_vocab, retained_indices = sp.preprocess()
        testing_dataset = self.tp.transform(
            text_for_contextual=unpreprocessed_documents,
            text_for_bow=preprocessed_documents,
        )
        text_vocab = set(text_vocab)
        in_vocab_words = [word for word in text_vocab if word in self.tp.vocab]
        oov_words = [word for word in text_vocab if word not in self.tp.vocab]
        coverage = len(in_vocab_words)/len(text_vocab)
        topic_distribution = ctm.get_doc_topic_distribution(
            testing_dataset, n_samples=5)
        topic_distribution = topic_distribution[0]
        topic_distribution = [(topic_idx, prob)
                              for topic_idx, prob in enumerate(topic_distribution)]
        if oov_words:
            if handle_oov == 'error':
                raise ValueError(f"Out-of-vocabulary words found: {oov_words}")
            elif handle_oov == 'warn':
                print(
                    f"⚠️  Warning: {len(oov_words)} out-of-vocabulary words ignored: {oov_words[:5]}{'...' if len(oov_words) > 5 else ''}")
        return topic_distribution, {
            'oov_words': oov_words,
            'in_vocab_words': in_vocab_words,
            'coverage': coverage,
        }

    def calculate_topic_similarity(self, dist1, dist2, num_topics=None):
        """
        Calculate cosine similarity between two topic distributions
        Works with both sparse (gensim format) and dense (numpy array) distributions
        """
        # Check if inputs are sparse (gensim format) or dense (numpy array)
        if isinstance(dist1, np.ndarray) and isinstance(dist2, np.ndarray):
            # If both are already dense arrays, use scipy's cosine distance
            return 1 - cosine(dist1, dist2)

        # Otherwise, convert sparse to dense
        if num_topics is None:
            raise ValueError(
                "For sparse distributions, num_topics must be provided")

        # Convert sparse to dense
        vec1 = np.zeros(num_topics)
        vec2 = np.zeros(num_topics)

        # Handle sparse format [(topic_id, prob), ...]
        if not isinstance(dist1, np.ndarray):
            for topic_id, prob in dist1:
                vec1[topic_id] = prob
        else:
            vec1 = dist1

        if not isinstance(dist2, np.ndarray):
            for topic_id, prob in dist2:
                vec2[topic_id] = prob
        else:
            vec2 = dist2

        # Calculate cosine similarity
        return 1 - cosine(vec1, vec2)

    def ctm_maximal_marginal_relevance(self,
                                       query_dist, candidate_distributions, candidate_info, lambda_param=0.7, top_k=10, num_topics=10):
        """
        Implement Maximal Marginal Relevance for diverse LDA recommendations

        Args:
            query_dist: Query artist's topic distribution
            candidate_distributions: Dict of candidate artist distributions
            candidate_info: Dict of candidate artist info
            lambda_param: Trade-off between relevance (1.0) and diversity (0.0)
            top_k: Number of recommendations to return

        Returns:
            List of diversified recommendations
        """
        print(
            f"🎯 Applying MMR for diverse LDA recommendations (λ={lambda_param})...")

        # Calculate initial relevance scores
        relevance_scores = {}
        for artist, dist in candidate_distributions.items():
            relevance_scores[artist] = self.calculate_topic_similarity(
                query_dist, dist, num_topics)

        # MMR algorithm
        selected = []
        remaining = list(candidate_distributions.keys())

        # First selection: most relevant
        if remaining:
            first_artist = max(remaining, key=lambda x: relevance_scores[x])
            selected.append(first_artist)
            remaining.remove(first_artist)

        # Subsequent selections: balance relevance and diversity
        while len(selected) < top_k and remaining:
            mmr_scores = {}

            for candidate in remaining:
                # Relevance component
                relevance = relevance_scores[candidate]

                # Diversity component (maximum similarity to any selected item)
                max_similarity = 0
                for selected_artist in selected:
                    similarity = self.calculate_topic_similarity(
                        candidate_distributions[candidate],
                        candidate_distributions[selected_artist],
                        num_topics
                    )
                    max_similarity = max(max_similarity, similarity)

                # MMR score
                mmr_score = lambda_param * relevance - \
                    (1 - lambda_param) * max_similarity
                mmr_scores[candidate] = mmr_score

            # Select candidate with highest MMR score
            next_artist = max(remaining, key=lambda x: mmr_scores[x])
            selected.append(next_artist)
            remaining.remove(next_artist)

        # Format results
        recommendations = []
        for i, artist in enumerate(selected):
            recommendations.append(
                {
                    "rank": i + 1,
                    "artist": artist,
                    "relevance": relevance_scores[artist],
                    "num_songs": candidate_info[artist]["num_songs"],
                    "top_topics": candidate_info[artist]["top_topics"],
                    "sample_songs": candidate_info[artist]["sample_songs"],
                }
            )

        return recommendations

    def recommend_artists(
        self, query_artist, strategy="balanced_mmr", top_k=8, lambda_param=0.7
    ):
        """
        Get artist recommendations using specified strategy
        """
        if query_artist not in self.artist_distributions:
            return f"❌ Artist '{query_artist}' not found in database"

        query_dist = self.artist_distributions[query_artist]
        candidates = {
            k: v for k, v in self.artist_distributions.items() if k != query_artist
        }
        candidate_info = {
            k: v for k, v in self.artist_info.items() if k != query_artist
        }

        if strategy == "similarity":
            # Pure similarity-based recommendations
            similarities = []
            for artist, dist in candidates.items():
                similarity = self.calculate_topic_similarity(query_dist, dist)
                similarities.append(
                    {
                        "artist": artist,
                        "similarity": similarity,
                        "num_songs": candidate_info[artist]["num_songs"],
                        "top_topics": candidate_info[artist]["top_topics"],
                    }
                )
            similarities.sort(key=lambda x: x["similarity"], reverse=True)
            recommendations = similarities[:top_k]

        elif strategy == "balanced_mmr":
            # MMR with balanced relevance/diversity
            recommendations = self.ctm_maximal_marginal_relevance(
                query_dist,
                candidates,
                candidate_info,
                lambda_param=lambda_param,
                top_k=top_k,
                num_topics=self.num_topics,
            )

        elif strategy == "diverse":
            # High diversity MMR
            recommendations = self.ctm_maximal_marginal_relevance(
                query_dist, candidates, candidate_info, lambda_param=0.4, top_k=top_k
            )

        return self.format_recommendations(query_artist, recommendations)

    def recommend_artists_from_lyrics(
        self, lyrics, strategy="balanced_mmr", top_k=5, lambda_param=0.7
    ):
        """
        Get artist recommendations based on lyrics using specified strategy
        """
        # Process the lyrics to get topic distribution
        topic_dist, info = self.process_new_document(
            self.ctm_model, lyrics, handle_oov="warn"
        )

        if topic_dist is None:
            return "❌ Could not process lyrics - insufficient vocabulary coverage"

        # Convert sparse topic distribution to dense array for artist similarity
        dense_dist = np.zeros(self.num_topics)
        for topic_id, prob in topic_dist:
            dense_dist[topic_id] = prob

        # Get all artists as candidates
        candidates = self.artist_distributions
        candidate_info = self.artist_info

        # Get recommendations based on strategy
        if strategy == "similarity":
            # Pure similarity-based recommendations
            similarities = []
            for artist, dist in candidates.items():
                similarity = self.calculate_topic_similarity(dense_dist, dist)
                similarities.append(
                    {
                        "artist": artist,
                        "similarity": similarity,
                        "num_songs": candidate_info[artist]["num_songs"],
                        "top_topics": candidate_info[artist]["top_topics"],
                    }
                )
            similarities.sort(key=lambda x: x["similarity"], reverse=True)
            recommendations = similarities[:top_k]

        elif strategy == "balanced_mmr" or strategy == "diverse":
            # MMR with balanced relevance/diversity
            lambda_val = 0.7 if strategy == "balanced_mmr" else 0.4
            recommendations = self.ctm_maximal_marginal_relevance(
                dense_dist,
                candidates,
                candidate_info,
                lambda_param=lambda_val,
                top_k=top_k,
                num_topics=self.num_topics,
            )

        return self.format_recommendations_from_lyrics(
            lyrics, topic_dist, info, recommendations
        )

    def recommend_songs_from_lyrics(
        self, lyrics, artist=None, title=None, top_k=5, similarity_threshold=0.7
    ):
        """
        Get song recommendations based on provided lyrics
        """
        # Check if we have corpus data available
        if self.song_topic_distributions is None or self.song_df is None:
            return "❌ Song recommendation not available - corpus data not provided"

        # Process the lyrics to get topic distribution
        topic_dist, info = self.process_new_document(
            self.ctm_model, lyrics, handle_oov="warn"
        )

        if topic_dist is None:
            return "❌ Could not process lyrics - insufficient vocabulary coverage"

        # Create exclude_song dict if artist and title are provided
        exclude_song = None
        if artist and title:
            exclude_song = {"artist": artist, "title": title}

        # Find similar songs
        similarities = []
        for i, corpus_topic_dist in enumerate(self.song_topic_distributions):
            # Skip the exact same song if provided
            if exclude_song:
                current_artist = str(
                    self.song_df.iloc[i]["artist"]).lower().strip()
                current_title = str(
                    self.song_df.iloc[i]["song_title"]).lower().strip()
                target_artist = str(exclude_song["artist"]).lower().strip()
                target_title = str(exclude_song["title"]).lower().strip()

                # Skip if it's very similar to our query song
                artist_similarity = similarity_ratio(
                    current_artist, target_artist)
                title_similarity = similarity_ratio(
                    current_title, target_title)

                if (
                    artist_similarity > similarity_threshold
                    and title_similarity > similarity_threshold
                ):
                    continue

            # Calculate topic similarity between query and corpus song
            topic_similarity = self.calculate_topic_similarity(
                topic_dist, corpus_topic_dist, self.num_topics
            )
            similarities.append((i, topic_similarity))

        # Sort by similarity (highest first)
        similarities.sort(key=lambda x: x[1], reverse=True)

        # Format recommendations
        recommendations = []
        for i, (song_idx, similarity) in enumerate(similarities[:top_k]):
            song_info = {
                "rank": i + 1,
                "artist": self.song_df.iloc[song_idx]["artist"],
                "title": self.song_df.iloc[song_idx]["song_title"],
                "similarity": similarity,
                "song_index": song_idx,
            }
            recommendations.append(song_info)

        return self.format_song_recommendations(
            lyrics, topic_dist, info, recommendations
        )

    def format_song_recommendations(self, lyrics, topic_dist, info, recommendations):
        """
        Format song recommendations for display
        """
        output = []
        output.append(f"🎵 SIMILAR SONGS BASED ON LYRICS")
        output.append("=" * 70)

        # Show lyrics profile
        output.append(f"\n📝 Lyrics Profile:")
        output.append(f"   Vocabulary coverage: {info['coverage']:.1%}")
        if info["coverage"] < 0.7:
            output.append(
                "   ⚠️ Low coverage may affect recommendation reliability")

        # Show top topics
        output.append(f"\n   Main themes:")
        # Sort topics by probability
        sorted_topics = sorted(topic_dist, key=lambda x: x[1], reverse=True)
        for i, (topic_id, prob) in enumerate(sorted_topics[:3]):
            label = self.topic_labels.get(topic_id, {}).get(
                "label", f"Topic {topic_id}"
            )
            words = self.topic_labels.get(topic_id, {}).get("words", [])[:3]
            output.append(
                f"      {i+1}. Topic {topic_id}: {label} ({prob:.3f})")
            if words:
                output.append(f"         Keywords: {', '.join(words)}")

        # Show recommendations
        output.append(f"\n🎯 Recommended Songs:")

        if not recommendations:
            output.append("   No similar songs found")
        else:
            for rec in recommendations:
                artist = rec["artist"]
                title = rec["title"]
                similarity = rec["similarity"]

                # Get the top topic for this song
                song_idx = rec["song_index"]
                song_topics = self.song_topic_distributions[song_idx]
                if song_topics.any():
                    top_topic_id = np.argmax(song_topics)
                    top_topic_label = self.topic_labels.get(top_topic_id, {}).get(
                        "label", f"Topic {top_topic_id}"
                    )

                    output.append(f"   {rec['rank']}. {artist} - {title}")
                    output.append(f"      Similarity: {similarity:.3f}")
                    output.append(
                        f"      Main theme: Topic {top_topic_id}: {top_topic_label}"
                    )

        return "\n".join(output)

    def format_recommendations_from_lyrics(
        self, lyrics, topic_dist, info, recommendations
    ):
        """
        Format recommendations from lyrics for display
        """
        output = []
        output.append(f"🎵 LDA RECOMMENDATIONS BASED ON LYRICS")
        output.append("=" * 70)

        # Show lyrics profile
        output.append(f"\n📝 Lyrics Profile:")
        output.append(f"   Vocabulary coverage: {info['coverage']:.1%}")
        if info["coverage"] < 0.7:
            output.append(
                "   ⚠️ Low coverage may affect recommendation reliability")

        # Show top topics
        output.append(f"\n   Main themes:")
        # Sort topics by probability
        sorted_topics = sorted(topic_dist, key=lambda x: x[1], reverse=True)
        for i, (topic_id, prob) in enumerate(sorted_topics[:3]):
            label = self.topic_labels.get(topic_id, {}).get(
                "label", f"Topic {topic_id}"
            )
            words = self.topic_labels.get(topic_id, {}).get("words", [])[:3]
            output.append(
                f"      {i+1}. Topic {topic_id}: {label} ({prob:.3f})")
            if words:
                output.append(f"         Keywords: {', '.join(words)}")

        # Show recommendations
        output.append(f"\n🎯 Recommended Artists:")

        for rec in recommendations:
            artist = rec["artist"]
            similarity = rec.get("relevance", rec.get("similarity", 0))
            num_songs = rec["num_songs"]
            top_topic_idx = rec["top_topics"][0]
            top_topic_label = self.topic_labels.get(top_topic_idx, {}).get(
                "label", f"Topic {top_topic_idx}"
            )

            rank = rec.get("rank", recommendations.index(rec) + 1)
            output.append(
                f"   {rank}. {artist} (similarity: {similarity:.3f})")
            output.append(
                f"      {num_songs} songs, main theme: Topic {top_topic_idx}: {top_topic_label}"
            )

            # Show sample songs
            sample_songs = self.artist_info[artist]["sample_songs"][:2]
            if sample_songs:
                output.append(f"      Sample: {', '.join(sample_songs)}")

        return "\n".join(output)

    def format_recommendations(self, query_artist, recommendations):
        """
        Format recommendations for display
        """
        output = []
        output.append(f"🎵 LDA RECOMMENDATIONS FOR: {query_artist}")
        output.append("=" * 70)

        # Show query artist profile
        query_info = self.artist_info[query_artist]
        query_dist = self.artist_distributions[query_artist]
        top_topics = query_info["top_topics"]

        output.append(f"\n🎤 {query_artist} Profile:")
        output.append(f"   Songs: {query_info['num_songs']}")
        output.append(f"   Main themes:")

        for i, topic_idx in enumerate(top_topics[:3]):
            prob = query_dist[topic_idx]
            label = self.topic_labels.get(topic_idx, {}).get(
                "label", f"Topic {topic_idx}"
            )
            words = self.topic_labels.get(topic_idx, {}).get("words", [])[:3]
            output.append(
                f"      {i+1}. Topic {topic_idx}: {label} ({prob:.3f})")
            if words:
                output.append(f"         Keywords: {', '.join(words)}")

        # Show recommendations
        output.append(f"\n🎯 Recommended Artists:")

        for rec in recommendations:
            artist = rec["artist"]
            similarity = rec.get("relevance", rec.get("similarity", 0))
            num_songs = rec["num_songs"]
            top_topic_idx = rec["top_topics"][0]
            top_topic_label = self.topic_labels.get(top_topic_idx, {}).get(
                "label", f"Topic {top_topic_idx}"
            )

            rank = rec.get("rank", recommendations.index(rec) + 1)
            output.append(
                f"   {rank}. {artist} (similarity: {similarity:.3f})")
            output.append(
                f"      {num_songs} songs, main theme: Topic {top_topic_idx}: {top_topic_label}"
            )

            # Show sample songs
            sample_songs = self.artist_info[artist]["sample_songs"][:2]
            if sample_songs:
                output.append(f"      Sample: {', '.join(sample_songs)}")

        return "\n".join(output)

    def get_artist_profile(self, artist):
        """
        Get detailed profile of an artist
        """
        if artist not in self.artist_distributions:
            return f"❌ Artist '{artist}' not found"

        info = self.artist_info[artist]
        dist = self.artist_distributions[artist]

        output = []
        output.append(f"🎤 LDA ARTIST PROFILE: {artist}")
        output.append("=" * 50)
        output.append(f"Songs: {info['num_songs']}")
        output.append(f"Average confidence: {info['avg_confidence']:.3f}")

        output.append(f"\nTop themes:")
        for i, topic_idx in enumerate(info["top_topics"]):
            prob = dist[topic_idx]
            label = self.topic_labels.get(topic_idx, {}).get(
                "label", f"Topic {topic_idx}"
            )
            words = self.topic_labels.get(topic_idx, {}).get("words", [])[:5]
            output.append(f"   {i+1}. Topic {topic_idx}: {label} ({prob:.3f})")
            if words:
                output.append(f"      Keywords: {', '.join(words)}")

        output.append(f"\nSample songs: {', '.join(info['sample_songs'])}")

        return "\n".join(output)

    def predict_new_song_topics(self, lyrics_text, top_n=3):
        """
        Predict topics for new song lyrics
        """
        topic_dist, info = self.process_new_document(
            self.ctm_model, lyrics_text, handle_oov="warn"
        )

        if topic_dist is None:
            return "❌ Cannot predict topics - insufficient vocabulary coverage"

        # Sort topics by probability
        # sorted_topics = sorted(topic_dist, reverse=True)

        output = []
        output.append(f"📄 Topic Prediction Results:")
        output.append(f"   Vocabulary coverage: {info['coverage']:.1%}")

        if info["coverage"] < 0.7:
            output.append(
                "   ⚠️ Low coverage may affect prediction reliability")

        sorted_topics = sorted(topic_dist, key=lambda x: x[1], reverse=True)
        for i, (topic_id, probability) in enumerate(sorted_topics[:top_n]):
            label = self.topic_labels.get(topic_id, {}).get(
                "label", f"Topic {topic_id}"
            )
            words = self.topic_labels.get(topic_id, {}).get("words", [])[:5]
            # Include topic number in the output
            output.append(
                f"   {topic_id+1}. Topic {topic_id}: {label} ({probability:.3f})")
            if words:
                output.append(f"      Keywords: {', '.join(words)}")

        return "\n".join(output)


df = pd.read_csv("Lyrics_extraction\scraped_lyrics_no_metadata_combined.csv")
loaded = load_model(model_name=get_latest_model_name())
ctm_model = loaded["model"]
artist_topic_distributions = loaded["artist_topic_distributions"]
artist_info = loaded["artist_info"]
topic_labels = loaded["topic_labels"]
song_topic_distributions = loaded["song_topic_distributions"]
song_df = loaded["df"]
tp = loaded["tp"]
stopwords = loaded["stopwords"]

lda_rec_system = CTMRecommendationSystem(
    ctm_model=ctm_model,
    artist_topic_distributions=artist_topic_distributions,
    artist_info=artist_info,
    topic_labels=topic_labels,
    song_topic_distributions=song_topic_distributions,
    song_df=df,
    tp=tp,
    stopwords=stopwords
)
