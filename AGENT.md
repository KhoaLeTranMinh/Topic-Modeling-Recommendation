# Topic Modeling Recommendation System - Agent Guide

## Build/Test Commands
- **Python version**: 3.11.10 (using uv package manager)
- **Install dependencies**: `uv sync` or `pip install -r requirements.txt`
- **GPU setup**: `python setup_gpu.py` (install CUDA PyTorch + check GPU)
- **GPU requirements**: `pip install -r requirements_gpu.txt`
- **Run Jupyter notebooks**: `jupyter lab` or `jupyter notebook`
- **Python execution**: `python script.py` (activate .venv311 if needed)

## Architecture & Structure
- **Experiments/**: Main ML experiments with Jupyter notebooks
  - `lda/`: LDA topic modeling modules (preprocess.py, model_utils.py, run.py)
  - `bertopic/`: BERTopic implementation and saved models
  - `saved_models/`: Trained model artifacts and visualizations
- **Lyrics_extraction/**: Data collection scripts using lyricsgenius API
- **Artist_Dataset/**: Raw data files (CSV, JSON) for artists and songs
- **Core algorithms**: LDA, BERTopic, CTM (Contextualized Topic Models)

## Code Style & Conventions
- **Imports**: Standard library first, then third-party, then local modules
- **Text processing**: NLTK for tokenization, stopwords, lemmatization
- **ML libraries**: scikit-learn, gensim, sentence-transformers, contextualized-topic-models
- **Data handling**: pandas DataFrames, numpy arrays
- **Visualization**: matplotlib, seaborn, pyLDAvis, wordcloud
- **LLM integration**: OpenAI API via langchain for topic labeling
- **File format**: Use .pkl for model persistence, .csv for data export
- **Naming**: snake_case for variables/functions, descriptive variable names
- **Documentation**: Jupyter cells include markdown headers and explanations
