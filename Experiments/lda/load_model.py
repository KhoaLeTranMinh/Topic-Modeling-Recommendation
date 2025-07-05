import os
from gensim.models import LdaModel
from gensim import corpora
import pickle
from typing import Optional
from datetime import datetime


def save_model_and_artifacts(model, dictionary, corpus, documents, coherence_score, num_topics, topic_labels=None, model_dir="saved_models"):
    """
    Save an LDA model and related data to disk.

    Parameters:
    - model: Trained LDA model
    - dictionary: Gensim dictionary
    - corpus: Document corpus (bow format)
    - documents: Original preprocessed documents
    - coherence_score: Model coherence score
    - num_topics: Number of topics in the model
    - topic_labels: Dictionary of topic labels (added feature)
    - model_dir: Directory to save the model

    Returns:
    - model_name: Name of the saved model
    - metadata: Dictionary with model metadata
    """
    # Create model directory if it doesn't exist
    os.makedirs(model_dir, exist_ok=True)

    # Generate timestamp for the model name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_name = f"lda_model_{num_topics}topics_{timestamp}"

    # Save model components
    model.save(os.path.join(model_dir, f"{model_name}.model"))
    dictionary.save(os.path.join(model_dir, f"{model_name}_dictionary.dict"))

    # Save corpus and documents using pickle
    with open(os.path.join(model_dir, f"{model_name}_corpus.pkl"), 'wb') as f:
        pickle.dump(corpus, f)

    with open(os.path.join(model_dir, f"{model_name}_documents.pkl"), 'wb') as f:
        pickle.dump(documents, f)

    # Save topic labels if provided (new feature)
    # if topic_labels:
    #     # Save as pickle for full object preservation
    #     with open(os.path.join(model_dir, f"{model_name}_topic_labels.pkl"), 'wb') as f:
    #         pickle.dump(topic_labels, f)

    #     # Also save as JSON for better interoperability and readability
    #     # with open(os.path.join(model_dir, f"{model_name}_topic_labels.json"), 'w', encoding='utf-8') as f:
    #     #     json.dump(topic_labels, f, indent=2, ensure_ascii=False)

    #     print(f"✅ Topic labels saved ({len(topic_labels)} topics)")

    # Create metadata
    metadata = {
        'coherence_score': coherence_score,
        'num_topics': num_topics,
        'timestamp': timestamp,
        'vocab_size': len(dictionary),
        'num_documents': len(documents),
        'has_topic_labels': topic_labels is not None,
        'model_name': model_name,
        'topic_labels': topic_labels
    }

    # Save metadata
    with open(os.path.join(model_dir, f"{model_name}_metadata.pkl"), 'wb') as f:
        pickle.dump(metadata, f)

    # Also save metadata as JSON for better interoperability
    # with open(os.path.join(model_dir, f"{model_name}_metadata.json"), 'w') as f:
    #     json.dump({k: str(v) if isinstance(v, (datetime, np.ndarray)) else v
    #               for k, v in metadata.items()}, f, indent=2)

    print(f"✅ Model saved as: {model_name}")
    print(f"📁 Model directory: {os.path.abspath(model_dir)}")

    return model_name, metadata


def load_saved_model(model_name, model_dir="../saved_models/"):
    """
    Load an LDA model and related data from disk.

    Parameters:
    - model_name: Name of the model to load
    - model_dir: Directory where models are stored

    Returns:
    - Dictionary with model components
    """
    model_path = os.path.join(model_dir, f"{model_name}.model")
    dictionary_path = os.path.join(model_dir, f"{model_name}_dictionary.dict")
    corpus_path = os.path.join(model_dir, f"{model_name}_corpus.pkl")
    documents_path = os.path.join(model_dir, f"{model_name}_documents.pkl")
    metadata_path = os.path.join(model_dir, f"{model_name}_metadata.pkl")
    topic_labels_path = os.path.join(
        model_dir, f"{model_name}_topic_labels.pkl")

    # Load model components
    model = LdaModel.load(model_path)
    dictionary = corpora.Dictionary.load(dictionary_path)

    with open(corpus_path, 'rb') as f:
        corpus = pickle.load(f)

    with open(documents_path, 'rb') as f:
        documents = pickle.load(f)

    metadata = {}
    if os.path.exists(metadata_path):
        with open(metadata_path, 'rb') as f:
            metadata = pickle.load(f)

    # Load topic labels if available (new feature)
    topic_labels = metadata.get('topic_labels', None)

    print(f"✅ Model '{model_name}' loaded successfully")
    return {
        'model': model,
        'dictionary': dictionary,
        'corpus': corpus,
        'documents': documents,
        'metadata': metadata,
        'topic_labels': topic_labels  # New field in the returned dictionary
    }


def get_latest_model(model_dir: str = "Experiments\saved_models") -> Optional[str]:
    """
    Get the name of the latest LDA model in the model directory.

    Parameters:
    - model_dir: Directory where models are stored

    Returns:
    - Name of the latest model, or None if no models found
    """
    if not os.path.exists(model_dir):
        return None

    # Look for .model files
    model_files = [f for f in os.listdir(model_dir) if f.endswith('.model')]
    if not model_files:
        return None

    # Sort by modification time (newest first)
    model_files.sort(key=lambda x: os.path.getmtime(
        os.path.join(model_dir, x)), reverse=True)

    # Get the base name (remove .model extension)
    latest_model = model_files[0].replace('.model', '')
    return latest_model
# Example of how to save your current model with topic labels


# model_name, metadata = save_model_and_artifacts(
#     optimal_model,
#     dictionary,
#     corpus,
#     documents,
#     coherence_score=coherence_score,
#     num_topics=optimal_model.num_topics,
#     topic_labels=lda_topic_labels  # Include topic labels
# )

# latest_model = get_latest_model()
# loaded = load_saved_model(latest_model)
# model = loaded['model']
# dictionary = loaded['dictionary']
# topic_labels = loaded['topic_labels']
# corpus = loaded['corpus']
# documents = loaded['documents']
# metadata = loaded['metadata']
# coherence_score = metadata.get('coherence_score', None)
# num_topics = metadata.get('num_topics', None)
# vocab_size = metadata.get('vocab_size', None)
"""
# To load the model later:
latest_model = get_latest_model()
loaded = load_saved_model(latest_model)
model = loaded['model']
dictionary = loaded['dictionary']
topic_labels = loaded['topic_labels']
"""
