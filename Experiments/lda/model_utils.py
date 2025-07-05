from difflib import SequenceMatcher
import numpy as np
from preprocess import preprocess
from scipy.spatial.distance import cosine


def similarity_ratio(str1, str2):
    """Calculate similarity ratio between two strings"""
    return SequenceMatcher(None, str1, str2).ratio()


def calculate_topic_similarity(dist1, dist2, num_topics=None):
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


def lda_maximal_marginal_relevance(query_dist, candidate_distributions, candidate_info, num_topics,
                                   lambda_param=0.7, top_k=10, ):
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
        relevance_scores[artist] = calculate_topic_similarity(
            query_dist, dist, num_topics=num_topics)

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
                similarity = calculate_topic_similarity(
                    candidate_distributions[candidate],
                    candidate_distributions[selected_artist],
                    num_topics=num_topics
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
        recommendations.append({
            'rank': i + 1,
            'artist': artist,
            'relevance': relevance_scores[artist],
            'num_songs': candidate_info[artist]['num_songs'],
            'top_topics': candidate_info[artist]['top_topics'],
            'sample_songs': candidate_info[artist]['sample_songs']
        })

    return recommendations


def process_new_document(text, model, dictionary, preprocess_func=None, handle_oov='ignore'):
    """
    Process a new document for topic prediction with OOV handling

    Parameters:
    - text: Raw text string
    - model: Trained LDA model
    - dictionary: Gensim dictionary from training
    - preprocess_func: Function to preprocess text (default: use global preprocess)
    - handle_oov: How to handle out-of-vocabulary words
                 'ignore': Skip OOV words (default)
                 'warn': Skip but warn about OOV words
                 'error': Raise error if OOV words found
    """
    if preprocess_func is None:
        # Use the unified preprocessing function
        # Note: We don't filter rare words for new documents since we want to see coverage
        tokens = preprocess(text, token_freq=None, min_freq=1)
    else:
        tokens = preprocess_func(text)

    # Check for OOV words
    vocab_words = set(dictionary.token2id.keys())
    oov_words = [word for word in tokens if word not in vocab_words]
    in_vocab_words = [word for word in tokens if word in vocab_words]

    if oov_words:
        if handle_oov == 'error':
            raise ValueError(f"Out-of-vocabulary words found: {oov_words}")
        elif handle_oov == 'warn':
            print(
                f"⚠️  Warning: {len(oov_words)} out-of-vocabulary words ignored: {oov_words[:5]}{'...' if len(oov_words) > 5 else ''}")

    # Convert to bag-of-words
    doc_bow = dictionary.doc2bow(in_vocab_words)

    if not doc_bow:
        print("⚠️  Warning: No words from document found in vocabulary. Cannot make prediction.")
        return None, {
            'original_tokens': tokens,
            'oov_words': oov_words,
            'in_vocab_words': in_vocab_words,
            'coverage': 0.0
        }

    # Get topic distribution
    topic_distribution = model[doc_bow]

    # Calculate vocabulary coverage
    coverage = len(in_vocab_words) / len(tokens) if tokens else 0.0

    return topic_distribution, {
        'original_tokens': tokens,
        'oov_words': oov_words,
        'in_vocab_words': in_vocab_words,
        'coverage': coverage,
        'doc_bow': doc_bow
    }


def analyze_vocabulary_coverage(new_documents, dictionary, preprocess_func=None):
    """
    Analyze how well the trained vocabulary covers new documents
    """
    if preprocess_func is None:
        preprocess_func = preprocess

    vocab_words = set(dictionary.token2id.keys())

    total_tokens = 0
    covered_tokens = 0
    all_oov_words = set()

    print("📊 Vocabulary Coverage Analysis:")
    print("=" * 40)

    for i, doc in enumerate(new_documents):
        tokens = preprocess_func(doc)
        oov_words = [word for word in tokens if word not in vocab_words]
        in_vocab_words = [word for word in tokens if word in vocab_words]

        total_tokens += len(tokens)
        covered_tokens += len(in_vocab_words)
        all_oov_words.update(oov_words)

        coverage = len(in_vocab_words) / len(tokens) if tokens else 0.0
        print(
            f"Doc {i+1}: {coverage:.1%} coverage ({len(in_vocab_words)}/{len(tokens)} words)")
        if oov_words:
            print(
                f"  OOV: {oov_words[:3]}{'...' if len(oov_words) > 3 else ''}")

    overall_coverage = covered_tokens / total_tokens if total_tokens else 0.0

    print("=" * 40)
    print(f"📈 Overall Coverage: {overall_coverage:.1%}")
    print(f"📖 Vocabulary Size: {len(vocab_words)}")
    print(f"🚫 Unique OOV Words: {len(all_oov_words)}")
    print(f"🔤 Most Common OOV: {list(all_oov_words)[:10]}")

    return {
        'overall_coverage': overall_coverage,
        'vocab_size': len(vocab_words),
        'oov_words': all_oov_words,
        'total_tokens': total_tokens,
        'covered_tokens': covered_tokens
    }


def predict_topics_for_text(text, model, dictionary, top_n=3):
    """
    Get top N topics for a new text with detailed output
    """
    topic_dist, info = process_new_document(
        text, model, dictionary, handle_oov='warn')

    if topic_dist is None:
        return None

    # Sort topics by probability
    sorted_topics = sorted(topic_dist, key=lambda x: x[1], reverse=True)

    print(f"📄 Document Analysis:")
    print(f"   Original text: {text[:100]}{'...' if len(text) > 100 else ''}")
    print(f"   Vocabulary coverage: {info['coverage']:.1%}")
    print(
        f"   Processed tokens: {info['in_vocab_words'][:10]}{'...' if len(info['in_vocab_words']) > 10 else ''}")

    if info['oov_words']:
        print(
            f"   OOV words: {info['oov_words'][:5]}{'...' if len(info['oov_words']) > 5 else ''}")

    print(f"\n🎯 Top {top_n} Topics:")
    for i, (topic_id, probability) in enumerate(sorted_topics[:top_n]):
        print(f"   Topic {topic_id}: {probability:.3f}")
        # Show top words for this topic
        topic_words = model.show_topic(topic_id, topn=5)
        words = [f"{word}({prob:.2f})" for word, prob in topic_words]
        print(f"     Words: {', '.join(words)}")

    return sorted_topics, info


def convert_lda_topic_distributions_to_dense(corpus_topic_distributions, num_topics):
    """
    Convert sparse LDA topic distributions to dense numpy arrays
    """
    print("🔄 Converting LDA topic distributions to dense format...")

    dense_distributions = []

    for topic_dist in corpus_topic_distributions:
        # Create dense vector
        dense_vec = np.zeros(num_topics)

        # Fill in the non-zero probabilities
        for topic_id, prob in topic_dist:
            dense_vec[topic_id] = prob

        dense_distributions.append(dense_vec)

    dense_array = np.array(dense_distributions)
    print(f"✅ Converted to dense format: {dense_array.shape}")
    return dense_array


def aggregate_lda_artist_topic_distributions(df, dense_topic_distributions, method='weighted_average'):
    """
    Aggregate song-level topic distributions to artist level for LDA

    Args:
        df: DataFrame with song information
        dense_topic_distributions: Dense numpy array of topic distributions
        method: Aggregation method ('average', 'weighted_average')

    Returns:
        artist_distributions: Dict of artist -> topic distribution
        artist_info: Dict of artist metadata
    """
    print(f"🎤 Aggregating LDA topic distributions to artist level...")
    print(f"   Method: {method}")

    artist_distributions = {}
    artist_info = {}

    for artist in df['artist'].unique():
        artist_songs = df[df['artist'] == artist]
        song_indices = artist_songs.index.tolist()

        # Get topic distributions for this artist's songs
        artist_song_distributions = dense_topic_distributions[song_indices]

        if method == 'average':
            # Simple average
            artist_dist = np.mean(artist_song_distributions, axis=0)

        elif method == 'weighted_average':
            # Weight by topic confidence (max probability per song)
            weights = np.max(artist_song_distributions, axis=1)
            if weights.sum() > 0:
                weights = weights / weights.sum()  # Normalize weights
                artist_dist = np.average(
                    artist_song_distributions, axis=0, weights=weights)
            else:
                artist_dist = np.mean(artist_song_distributions, axis=0)

        else:  # fallback to average
            artist_dist = np.mean(artist_song_distributions, axis=0)

        # Normalize to ensure it's a proper probability distribution
        if artist_dist.sum() > 0:
            artist_dist = artist_dist / artist_dist.sum()

        artist_distributions[artist] = artist_dist
        artist_info[artist] = {
            'num_songs': len(artist_songs),
            'avg_confidence': np.mean([np.max(dist) for dist in artist_song_distributions]),
            'top_topics': np.argsort(artist_dist)[-3:][::-1],  # Top 3 topics
            'sample_songs': artist_songs['song_title'].head(3).tolist()
        }

    print(
        f"✅ Aggregated distributions for {len(artist_distributions)} artists")
    return artist_distributions, artist_info


def generate_topic_distributions(optimal_model, corpus):
    print("🔄 Generating topic distributions for all songs in the corpus...")
    corpus_topic_distributions = []

    for i, doc_bow in enumerate(corpus):
        topic_dist = optimal_model[doc_bow]
        corpus_topic_distributions.append(topic_dist)

        if (i + 1) % 100 == 0:
            print(f"   Processed {i + 1}/{len(corpus)} documents")

    print(
        f"✓ Generated topic distributions for {len(corpus_topic_distributions)} songs")
    print("This will be used for song similarity calculations.")
    return corpus_topic_distributions
