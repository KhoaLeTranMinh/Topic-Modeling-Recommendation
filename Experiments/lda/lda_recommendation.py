# ===== LDA RECOMMENDATION SYSTEM INTERFACE =====

from difflib import SequenceMatcher

from model_utils import calculate_topic_similarity, lda_maximal_marginal_relevance, process_new_document, similarity_ratio

import numpy as np


class LDARecommendationSystem:
    """
    Complete LDA-based music recommendation system
    """

    def __init__(self, lda_model, dictionary, artist_distributions, artist_info, topic_labels,
                 corpus_topic_distributions=None, song_df=None):
        self.lda_model = lda_model
        self.dictionary = dictionary
        self.artist_distributions = artist_distributions
        self.artist_info = artist_info
        self.topic_labels = topic_labels
        self.num_topics = lda_model.num_topics
        # Store corpus distributions and song dataframe for song recommendations
        self.corpus_topic_distributions = corpus_topic_distributions
        self.song_df = song_df

    def recommend_artists(self, query_artist, strategy='balanced_mmr', top_k=8, lambda_param=0.7):
        """
        Get artist recommendations using spec   ified strategy
        """
        if query_artist not in self.artist_distributions:
            return f"❌ Artist '{query_artist}' not found in database"

        query_dist = self.artist_distributions[query_artist]
        candidates = {
            k: v for k, v in self.artist_distributions.items() if k != query_artist}
        candidate_info = {k: v for k,
                          v in self.artist_info.items() if k != query_artist}

        if strategy == 'similarity':
            # Pure similarity-based recommendations
            similarities = []
            for artist, dist in candidates.items():
                similarity = calculate_topic_similarity(
                    query_dist, dist, self.num_topics)
                similarities.append({
                    'artist': artist,
                    'similarity': similarity,
                    'num_songs': candidate_info[artist]['num_songs'],
                    'top_topics': candidate_info[artist]['top_topics']
                })
            similarities.sort(key=lambda x: x['similarity'], reverse=True)
            recommendations = similarities[:top_k]

        elif strategy == 'balanced_mmr':
            # MMR with balanced relevance/diversity
            recommendations = lda_maximal_marginal_relevance(
                query_dist, candidates, candidate_info,
                lambda_param=lambda_param, top_k=top_k, num_topics=self.num_topics
            )

        elif strategy == 'diverse':
            # High diversity MMR
            recommendations = lda_maximal_marginal_relevance(
                query_dist, candidates, candidate_info,
                lambda_param=0.4, top_k=top_k, num_topics=self.num_topics
            )

        return self.format_recommendations(query_artist, recommendations)

    def recommend_artists_from_lyrics(self, lyrics, strategy='balanced_mmr', top_k=5, lambda_param=0.7):
        """
        Get artist recommendations based on lyrics using specified strategy
        """
        # Process the lyrics to get topic distribution
        topic_dist, info = process_new_document(
            lyrics, self.lda_model, self.dictionary, handle_oov='warn')

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
        if strategy == 'similarity':
            # Pure similarity-based recommendations
            similarities = []
            for artist, dist in candidates.items():
                similarity = calculate_topic_similarity(
                    dense_dist, dist, self.num_topics)
                similarities.append({
                    'artist': artist,
                    'similarity': similarity,
                    'num_songs': candidate_info[artist]['num_songs'],
                    'top_topics': candidate_info[artist]['top_topics']
                })
            similarities.sort(key=lambda x: x['similarity'], reverse=True)
            recommendations = similarities[:top_k]

        elif strategy == 'balanced_mmr' or strategy == 'diverse':
            # MMR with balanced relevance/diversity
            lambda_val = 0.7 if strategy == 'balanced_mmr' else 0.4
            recommendations = lda_maximal_marginal_relevance(
                dense_dist, candidates, candidate_info,
                lambda_param=lambda_param, top_k=top_k, num_topics=self.num_topics
            )

        return self.format_recommendations_from_lyrics(lyrics, topic_dist, info, recommendations)

    def recommend_songs_from_lyrics(self, lyrics, artist=None, title=None, top_k=5, similarity_threshold=0.7):
        """
        Get song recommendations based on provided lyrics
        """
        # Check if we have corpus data available
        if self.corpus_topic_distributions is None or self.song_df is None:
            return "❌ Song recommendation not available - corpus data not provided"

        # Process the lyrics to get topic distribution
        topic_dist, info = process_new_document(
            lyrics, self.lda_model, self.dictionary, handle_oov='warn')

        if topic_dist is None:
            return "❌ Could not process lyrics - insufficient vocabulary coverage"

        # Create exclude_song dict if artist and title are provided
        exclude_song = None
        if artist and title:
            exclude_song = {'artist': artist, 'title': title}

        # Find similar songs
        similarities = []
        for i, corpus_topic_dist in enumerate(self.corpus_topic_distributions):
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

                if artist_similarity > similarity_threshold and title_similarity > similarity_threshold:
                    continue

            # Calculate topic similarity between query and corpus song
            topic_similarity = calculate_topic_similarity(
                topic_dist, corpus_topic_dist, self.num_topics)
            similarities.append((i, topic_similarity))

        # Sort by similarity (highest first)
        similarities.sort(key=lambda x: x[1], reverse=True)

        # Format recommendations
        recommendations = []
        for i, (song_idx, similarity) in enumerate(similarities[:top_k]):
            song_info = {
                'rank': i + 1,
                'artist': self.song_df.iloc[song_idx]['artist'],
                'title': self.song_df.iloc[song_idx]['song_title'],
                'similarity': similarity,
                'song_index': song_idx
            }
            recommendations.append(song_info)

        return self.format_song_recommendations(lyrics, topic_dist, info, recommendations)

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
        if info['coverage'] < 0.7:
            output.append(
                "   ⚠️ Low coverage may affect recommendation reliability")

        # Show top topics
        output.append(f"\n   Main themes:")
        # Sort topics by probability
        sorted_topics = sorted(topic_dist, key=lambda x: x[1], reverse=True)
        for i, (topic_id, prob) in enumerate(sorted_topics[:3]):
            label = self.topic_labels.get(topic_id, {}).get(
                'label', f'Topic {topic_id}')
            words = self.topic_labels.get(topic_id, {}).get('words', [])[:3]
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
                artist = rec['artist']
                title = rec['title']
                similarity = rec['similarity']

                # Get the top topic for this song
                song_idx = rec['song_index']
                song_topics = self.corpus_topic_distributions[song_idx]
                if song_topics:
                    top_topic_id = max(song_topics, key=lambda x: x[1])[0]
                    top_topic_label = self.topic_labels.get(
                        top_topic_id, {}).get('label', f'Topic {top_topic_id}')

                    output.append(f"   {rec['rank']}. {artist} - {title}")
                    output.append(f"      Similarity: {similarity:.3f}")
                    output.append(
                        f"      Main theme: Topic {top_topic_id}: {top_topic_label}")

        return "\n".join(output)

    def format_recommendations_from_lyrics(self, lyrics, topic_dist, info, recommendations):
        """
        Format recommendations from lyrics for display
        """
        output = []
        output.append(f"🎵 LDA RECOMMENDATIONS BASED ON LYRICS")
        output.append("=" * 70)

        # Show lyrics profile
        output.append(f"\n📝 Lyrics Profile:")
        output.append(f"   Vocabulary coverage: {info['coverage']:.1%}")
        if info['coverage'] < 0.7:
            output.append(
                "   ⚠️ Low coverage may affect recommendation reliability")

        # Show top topics
        output.append(f"\n   Main themes:")
        # Sort topics by probability
        sorted_topics = sorted(topic_dist, key=lambda x: x[1], reverse=True)
        for i, (topic_id, prob) in enumerate(sorted_topics[:3]):
            label = self.topic_labels.get(topic_id, {}).get(
                'label', f'Topic {topic_id}')
            words = self.topic_labels.get(topic_id, {}).get('words', [])[:3]
            output.append(
                f"      {i+1}. Topic {topic_id}: {label} ({prob:.3f})")
            if words:
                output.append(f"         Keywords: {', '.join(words)}")

        # Show recommendations
        output.append(f"\n🎯 Recommended Artists:")

        for rec in recommendations:
            artist = rec['artist']
            similarity = rec.get('relevance', rec.get('similarity', 0))
            num_songs = rec['num_songs']
            top_topic_idx = rec['top_topics'][0]
            top_topic_label = self.topic_labels.get(
                top_topic_idx, {}).get('label', f'Topic {top_topic_idx}')

            rank = rec.get('rank', recommendations.index(rec) + 1)
            output.append(
                f"   {rank}. {artist} (similarity: {similarity:.3f})")
            output.append(
                f"      {num_songs} songs, main theme: Topic {top_topic_idx}: {top_topic_label}")

            # Show sample songs
            sample_songs = self.artist_info[artist]['sample_songs'][:2]
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
        top_topics = query_info['top_topics']

        output.append(f"\n🎤 {query_artist} Profile:")
        output.append(f"   Songs: {query_info['num_songs']}")
        output.append(f"   Main themes:")

        for i, topic_idx in enumerate(top_topics[:3]):
            prob = query_dist[topic_idx]
            label = self.topic_labels.get(topic_idx, {}).get(
                'label', f'Topic {topic_idx}')
            words = self.topic_labels.get(topic_idx, {}).get('words', [])[:3]
            output.append(
                f"      {i+1}. Topic {topic_idx}: {label} ({prob:.3f})")
            if words:
                output.append(f"         Keywords: {', '.join(words)}")

        # Show recommendations
        output.append(f"\n🎯 Recommended Artists:")

        for rec in recommendations:
            artist = rec['artist']
            similarity = rec.get('relevance', rec.get('similarity', 0))
            num_songs = rec['num_songs']
            top_topic_idx = rec['top_topics'][0]
            top_topic_label = self.topic_labels.get(
                top_topic_idx, {}).get('label', f'Topic {top_topic_idx}')

            rank = rec.get('rank', recommendations.index(rec) + 1)
            output.append(
                f"   {rank}. {artist} (similarity: {similarity:.3f})")
            output.append(
                f"      {num_songs} songs, main theme: Topic {top_topic_idx}: {top_topic_label}")

            # Show sample songs
            sample_songs = self.artist_info[artist]['sample_songs'][:2]
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
        for i, topic_idx in enumerate(info['top_topics']):
            prob = dist[topic_idx]
            label = self.topic_labels.get(topic_idx, {}).get(
                'label', f'Topic {topic_idx}')
            words = self.topic_labels.get(topic_idx, {}).get('words', [])[:5]
            output.append(f"   {i+1}. Topic {topic_idx}: {label} ({prob:.3f})")
            if words:
                output.append(f"      Keywords: {', '.join(words)}")

        output.append(f"\nSample songs: {', '.join(info['sample_songs'])}")

        return "\n".join(output)

    def predict_new_song_topics(self, lyrics_text, top_n=3):
        """
        Predict topics for new song lyrics
        """
        topic_dist, info = process_new_document(
            lyrics_text, self.lda_model, self.dictionary, handle_oov='warn'
        )

        if topic_dist is None:
            return "❌ Cannot predict topics - insufficient vocabulary coverage"

        # Sort topics by probability
        sorted_topics = sorted(topic_dist, key=lambda x: x[1], reverse=True)

        output = []
        output.append(f"📄 Topic Prediction Results:")
        output.append(f"   Vocabulary coverage: {info['coverage']:.1%}")

        if info['coverage'] < 0.7:
            output.append(
                "   ⚠️ Low coverage may affect prediction reliability")

        output.append(f"\n🎯 Top {top_n} Topics:")
        for i, (topic_id, probability) in enumerate(sorted_topics[:top_n]):
            label = self.topic_labels.get(topic_id, {}).get(
                'label', f'Topic {topic_id}')
            words = self.topic_labels.get(topic_id, {}).get('words', [])[:5]
            # Include topic number in the output
            output.append(
                f"   {i+1}. Topic {topic_id}: {label} ({probability:.3f})")
            if words:
                output.append(f"      Keywords: {', '.join(words)}")

        return "\n".join(output)
