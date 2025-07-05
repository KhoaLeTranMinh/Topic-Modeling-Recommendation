import nltk
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
# from nltk.tokenize import word_tokenize
from nltk import pos_tag
import re
from collections import Counter
import pandas as pd
from nltk.tokenize import RegexpTokenizer
from pywsd import lemmatize

# Download necessary resources
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')

stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()
# Get the words and apostrophes, such as "don't", "it's", "I'm"
tokenizer = RegexpTokenizer(r"[A-Za-z]+(?:'[A-Za-z]*)?")


def strip_possessive(token):
    """
    Remove possessive endings ('s, ') from tokens
    """
    # Remove common possessive endings
    if token.endswith("'s"):
        return token[:-2]
    elif token.endswith("'"):
        return token[:-1]
    return token


def tokenize_and_clean_possessive(text):
    """
    Tokenize text and clean possessive forms
    """
    # First, tokenize with the basic alphabetic pattern
    raw_tokens = tokenizer.tokenize(text.lower())

    # Then clean each token to remove possessives
    cleaned_tokens = [strip_possessive(token) for token in raw_tokens]

    return cleaned_tokens


def safe_lemmatize(token):
    """
    Lemmatize with exceptions for problematic words
    Expanded list based on common lemmatization issues in song lyrics
    """
    # Comprehensive exceptions list
    exceptions = {
        # Original problematic words
        'ass', 'bass', 'mass', 'class', 'pass', 'glass',

        # Short function words (prone to over-lemmatization)
        'us', 'is', 'as', 'has', 'was', 'yes', 'his',

        # Interjections and exclamations
        'yeah', 'nah', 'yo', 'hey', 'whoa', 'oh', 'ah', 'um', 'hmm',

        # Slang and profanity (keep original form)
        'damn', 'hell', 'shit', 'fuck', 'bitch', 'bastard', 'piss', 'crap',

        # Informal contractions
        'wanna', 'gonna', 'gotta', 'kinda', 'sorta', 'coulda', 'shoulda', 'woulda',
        'lemme', 'gimme', 'tryna', 'bout',

        # Music and song-specific terms
        'vibes', 'beats', 'remix', 'chorus', 'verse', 'bridge', 'hook', 'freestyle',

        # Modern slang (might not be in WordNet properly)
        'sus', 'cap', 'flex', 'stan', 'simp', 'vibe', 'mood', 'facts', 'lit', 'fire',
        'dope', 'sick', 'mad', 'hella', 'salty',

        # Informal address terms
        'mans', 'fam', 'bros', 'homie', 'dude', 'chick', 'guys', 'peeps',

        # Texting abbreviations (should stay as-is)
        'ur', 'u', 'n', 'nd', 'wit', 'da', 'dis', 'dat', 'dem', 'dey'
    }

    if token.lower() in exceptions:
        return token
    else:
        return lemmatize(token)


def is_valid_token(token):
    """
    More intelligent token validation than just isalpha()
    """
    # Keep contractions and possessives
    if "'" in token:
        return True

    # Keep hyphenated words
    if "-" in token and any(c.isalpha() for c in token):
        return True

    # Keep pure alphabetic
    if token.isalpha():
        return True

    # Reject everything else (pure numbers, special chars, etc.)
    return False


def is_english_word(word):
    return bool(wordnet.synsets(word))


def preprocess(text, token_freq=None, min_freq=5):
    """
    Comprehensive preprocessing function for song lyrics

    Parameters:
    - text: Raw lyrics text
    - token_freq: Counter object with token frequencies (for rare word filtering)
    - min_freq: Minimum frequency threshold for keeping words
    - clean_metadata: Whether to clean metadata before processing

    Returns:
    - List of processed tokens
    """
    if not text or pd.isna(text):
        return []

    # Convert to string if needed
    text = str(text)

    # Step 2: Remove structural markers [Intro], [Verse 1], etc.
    text = re.sub(r'\[.*?\]', '', text)

    # Step 3: Lowercase & tokenize and clean possessives (keeping only words and apostrophes)
    tokens = tokenize_and_clean_possessive(text)

    # Step 4: Keep only alphabetic tokens
    tokens = [t for t in tokens if is_valid_token(t)]

    # Step 5: Remove stopwords
    tokens = [t for t in tokens if t not in stop_words]

    # Step 6: Lemmatize with POS tagging
    tokens = [safe_lemmatize(token) for token in tokens]

    # Step 7: Remove non-English words
    tokens = [t for t in tokens if is_english_word(t)]

    # Step 8: Filter rare words (if token_freq is provided)
    if token_freq is not None:
        tokens = [t for t in tokens if token_freq[t] >= min_freq]

    return tokens


def preprocess_corpus(texts, min_freq=5, clean_metadata=True, verbose=True):
    """
    Preprocess an entire corpus of texts with two-pass rare word filtering

    Parameters:
    - texts: List or Series of raw text documents
    - min_freq: Minimum frequency threshold for keeping words
    - clean_metadata: Whether to clean metadata before processing
    - verbose: Whether to print progress information

    Returns:
    - List of processed token lists
    - Counter object with final token frequencies
    """
    if verbose:
        print("🔄 Starting corpus preprocessing...")

    # First pass: basic preprocessing without rare word filtering
    if verbose:
        print("✓ Pass 1: Basic preprocessing (cleaning, tokenizing, lemmatizing)")

    processed_docs = []
    for i, text in enumerate(texts):
        tokens = preprocess(text, token_freq=None, )
        processed_docs.append(tokens)

        if verbose and (i + 1) % 100 == 0:
            print(f"   Processed {i + 1}/{len(texts)} documents")

    # Calculate token frequencies
    if verbose:
        print("✓ Pass 2: Calculating token frequencies")

    all_tokens = [token for doc in processed_docs for token in doc]
    token_freq = Counter(all_tokens)

    if verbose:
        print(f"   Total unique tokens before filtering: {len(token_freq)}")
        print(f"   Total tokens: {len(all_tokens)}")

    # Second pass: filter rare words
    if verbose:
        print(f"✓ Pass 3: Filtering rare words (min_freq = {min_freq})")

    final_docs = []
    for tokenized_doc in processed_docs:
        filtered_tokens_doc = [
            t for t in tokenized_doc if token_freq[t] >= min_freq]
        final_docs.append(filtered_tokens_doc)

    # Final statistics
    if verbose:
        remaining_tokens = [token for doc in final_docs for token in doc]
        remaining_unique = len(set(remaining_tokens))
        print(
            f"   Tokens after filtering: {remaining_unique} unique, {len(remaining_tokens)} total")
        print(f"   Removed {len(token_freq) - remaining_unique} rare words")
        print("✅ Corpus preprocessing complete!")

    return final_docs, token_freq


def preprocess_single_document(text, token_freq=None, min_freq=5):
    """
    Preprocess a single document (for new documents after training)

    Parameters:
    - text: Raw text document
    - token_freq: Pre-computed token frequencies from training corpus
    - min_freq: Minimum frequency threshold
    - clean_metadata: Whether to clean metadata

    Returns:
    - List of processed tokens
    """
    return preprocess(text, token_freq=token_freq, min_freq=min_freq)
