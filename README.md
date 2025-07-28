# 🎵 Utilizing Topic Modeling for Music Recommendation




## 📖 Overview

This research project explores the application of topic modeling techniques to create a lyrics-based music recommendation system. By analyzing the thematic content of song lyrics rather than relying solely on audio features or collaborative filtering, this approach aims to:

- **Reduce popularity bias** in music recommendations
- **Enhance music discovery** for lesser-known artists
- **Provide thematically coherent** suggestions based on lyrical content
- **Maintain recommendation diversity** while preserving semantic relevance

## 🎯 Key Features

### 🔬 **Three Topic Modeling Approaches**
- **Latent Dirichlet Allocation (LDA)** - Classical probabilistic approach
- **BERTopic** - Modern clustering-based method with transformer embeddings
- **Contextualized Topic Models (CTM)** - Neural approach combining ProdLDA with contextualized embeddings

### 📊 **Comprehensive Evaluation**
- Topic coherence analysis using C_V metrics
- Topic diversity measurements
- Recommendation quality assessment
- Interactive visualizations with pyLDAvis

### 🌍 **Multilingual Support**
- English, Spanish, Korean, and other language lyrics
- Cross-lingual semantic understanding
- Cultural and linguistic theme detection

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (recommended package manager)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/KhoaLeTranMinh/Topic-Modeling-Recommendation.git
   cd Topic-Modeling-Recommendation
   ```

2. **Set up virtual environment**
   ```bash
   # Using uv (recommended)
   uv venv --python 3.11
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   
   # Or using standard venv
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   uv sync
   # Or using pip: pip install -r requirements.txt
   ```

### Running the Project

1. **Generate Topic Models**
   ```bash
   cd Experiments
   # Run the Jupyter notebooks to train models:
   # - lda_model_training.ipynb
   # - bertopic_model_training.ipynb  
   # - ctm_model_training.ipynb
   ```

2. **Demo the Recommendation Pipeline**
   ```bash
   # CTM-based recommendations (best performing)
   cd ctm
   python run.py
   
   # LDA-based recommendations
   cd ../lda
   python run.py
   ```

## 📈 Results

### Model Performance Comparison

| Metric | LDA | BERTopic | CTM |
|--------|-----|----------|-----|
| **Topic Coherence (C_V)** | 0.43 | 0.38 | **0.58** |
| **Topic Diversity** | 0.65 | 0.89 | **0.83** |
| **Gini Coefficient** | 0.45 | 0.81 | **0.20** |

**CTM emerges as the superior model**, achieving the highest topic coherence while maintaining excellent diversity and the most balanced topic distribution.

### Key Findings

- **CTM** captures semantic relationships most effectively using contextualized embeddings
- **BERTopic** struggles with dataset size, creating highly imbalanced topic distributions
- **LDA** provides solid baseline performance with interpretable probabilistic framework
- **Multilingual capability** allows discovery of cross-cultural musical themes

## 🎼 Example Recommendation

**Input:** Joji - Sanctuary

**CTM Recommendations:**
1. The Lumineers - Flowers in Your Hair (Similarity: 0.979)
2. Sam Cooke - I Wish You Love (Similarity: 0.978)  
3. Foo Fighters - My Hero (Similarity: 0.963)

*Based on shared themes of "Urban Lifestyle and Challenges" and "Heartbreak and Loneliness"*

## 📁 Project Structure

```
Topic-Modeling-Recommendation/
├── 📊 Experiments/           # Model training notebooks
│   ├── lda_model_training.ipynb
│   ├── bertopic_model_training.ipynb
│   └── ctm_model_training.ipynb
├── 🎵 ctm/                   # CTM recommendation pipeline
│   └── run.py
├── 📝 lda/                   # LDA recommendation pipeline  
│   └── run.py
├── 📖 ThesisReport.tex       # Complete research documentation
├── 🔗 references.bib         # Bibliography
└── 📋 requirements.txt       # Dependencies
```

## 🔬 Research Contributions

1. **Novel Application**: First comprehensive comparison of modern topic modeling approaches for music recommendation
2. **Bias Mitigation**: Demonstrated reduction in popularity bias through lyrics-based approach
3. **Multilingual Analysis**: Cross-cultural theme discovery in music lyrics
4. **Evaluation Framework**: Systematic comparison using coherence, diversity, and recommendation quality metrics

## 📚 Academic Context

This work is part of a **Waseda University Graduation Thesis** that investigates how topic modeling can revolutionize music recommendation systems. The research addresses critical issues in current recommendation algorithms:

- **Cold Start Problem**: Better handling of new artists and songs
- **Popularity Bias**: Reducing over-recommendation of mainstream content
- **Semantic Understanding**: Capturing deeper thematic relationships in music

## 🔮 Future Work

- **Audio-Visual Integration**: Combining lyrics with audio features and visual content
- **Sequential Modeling**: Incorporating listening history and session-aware recommendations  
- **User Studies**: Empirical evaluation with real user feedback
- **Web Deployment**: Interactive web interface for broader accessibility
- **Real-time Learning**: Adaptive models that learn from user interactions

## 📄 Citation

If you use this work in your research, please cite:

```bibtex
@thesis{le2024topic,
  title={Utilizing Topic Modeling for Music Recommendation},
  author={Le Tran Minh Khoa},
  school={Waseda University},
  year={2024},
  type={Graduation Thesis}
}
```

## 🤝 Contributing

Contributions are welcome! Please feel free to:
- 🐛 Report bugs or issues
- 💡 Suggest new features or improvements
- 📖 Improve documentation
- 🔬 Extend the research with new models or datasets

## 📧 Contact

**Khoa Le Tran Minh**
- 🎓 Waseda University
- 📧 Email: [your-email@example.com]
- 🔗 GitHub: [@KhoaLeTranMinh](https://github.com/KhoaLeTranMinh)

---
