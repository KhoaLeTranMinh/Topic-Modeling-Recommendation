# Topic-Based Music Recommendation System: Research Proposal

## Overview

This document outlines a structured approach for building a topic modeling-based music recommendation system using song lyrics. The system will represent songs and artists through topic distributions, provide interpretable recommendations, and ensure diversity in suggestions.

---

## 1. Representing Songs/Artists via Topic Models

The core idea is to treat each song as a "document" whose words are the song's lyrics. After preprocessing, you obtain a topic-distribution vector for each song. To represent an artist, you aggregate the topic distributions of that artist's songs.

### 1.1 Preprocessing Lyrics

- **Clean & tokenize**: Lowercase, remove punctuation, strip stopwords, and apply stemming/lemmatization (e.g., with NLTK or spaCy)
- **Filter rare terms**: Remove words that appear in fewer than 5 songs (or whatever threshold makes sense for your corpus size)
- **Optionally include metadata**: You could append mood-related tags (e.g., "#sad," "#upbeat") if such labels exist in your dataset

### 1.2 Topic Modeling Algorithms

#### a. LDA (Latent Dirichlet Allocation)
- Process each song's bag-of-words into an LDA model (e.g., Gensim's implementation)
- Choose number of topics *K* via coherence optimization (e.g., run LDA for K=10, 20, 30, etc., compute C_v coherence, pick the K with a plateau in coherence)
- **Result**: each song *s* is a probability vector θ_s ∈ ℝ^K (∑θ_s[i]=1)

#### b. BERTopic
- Use a transformer encoder (e.g., `all-MiniLM-L6-v2`) to embed each lyric document
- Cluster embeddings (e.g., HDBSCAN) to form initial clusters; reduce to K clusters or let HDBSCAN pick them
- Apply class-based TF–IDF on the clusters to extract topic keywords
- **Result**: each song is assigned to one or more topics; you can derive a soft distribution

#### c. NMF (Non-negative Matrix Factorization)
- Build a TF–IDF matrix *M* (songs × vocabulary)
- Factorize *M ≈ W·H* where W (songs×K) and H (K×vocab)
- Use K components; each row in W is a K-dimensional vector for that song
- **Advantage**: NMF sometimes yields more coherent topics when lyrics are sparse

#### d. Other Variants / Extensions
- **Correlated Topic Model (CTM)** to model correlations between topics
- **Neural Topic Models** (e.g., ProdLDA, ETM) for potentially better coherence

### 1.3 Naming & Interpreting Topics with an LLM

After fitting (say) LDA, you'll inspect each topic's top 10 words. Send these word-lists to an LLM prompt such as:

```
Here are the top 10 words for topic #3 from my LDA on song lyrics:
  - love, promise, forever, heart, soul, time, night, away, close, memory
Please suggest a concise, human-readable label (e.g., "Romantic Reunion" or "Longing for Love"), and explain in one sentence why this label fits.
```

### 1.4 Aggregating to Artist Level

If an artist A has songs {s₁, s₂, …, sₙ}, and each song has topic distribution θ_{sᵢ}, define:

$$θ_A = \frac{1}{n} \sum_{i=1}^n θ_{s_i}$$

Optionally weight by popularity or recency if you want the artist profile to adapt over time.

---

## 2. Defining "Similarity" & Introducing Diversity

Once each artist (or song) is a K-dimensional probability vector, you need a similarity/distance metric to recommend "nearest neighbors" in topic-space—and then re-rank for diversity.

### 2.1 Distance Metrics on Topic Distributions

#### Cosine similarity:
$$\mathrm{sim}(A, B) = \frac{θ_A \cdot θ_B}{\|θ_A\| \|θ_B\|}$$

#### Jensen–Shannon divergence:
$$\mathrm{JS}(θ_A, θ_B) = \frac{1}{2} \mathrm{KL}(θ_A \| M) + \frac{1}{2} \mathrm{KL}(θ_B \| M)$$
where $M = \frac{1}{2}(θ_A + θ_B)$

#### Hellinger distance:
$$\mathrm{H}(θ_A, θ_B) = \frac{1}{\sqrt{2}}\|\sqrt{θ_A} - \sqrt{θ_B}\|_2$$

### 2.2 Ensuring Recommendation Diversity

#### a. Maximal Marginal Relevance (MMR)
Rank candidates by a combination of relevance and novelty. When building the recommendation list (R = ∅ initially), at each step select:

$$\arg\max_{x \in \mathcal{C} \setminus R} [\lambda\,\mathrm{sim}(q,x) - (1-\lambda)\,\max_{y \in R} \mathrm{sim}(x,y)]$$

Tuning λ∈[0,1] trades off between pure similarity (λ→1) and diversity (λ→0).

#### b. Topic-Cluster Filtering
- Pre-cluster the entire artist set in topic-space
- Sample top artists from top k distinct clusters

#### c. Re-ranking by Novelty
- First retrieve a candidate pool of top 100 most similar artists
- Then impose a post-filter based on topic-cosine similarity thresholds

#### d. Covering Different Thematic Labels
- Ensure the final N artists represent different top-2 topic labels
- Enforce coverage across at least M distinct labels

---

## 3. Architectural Sketch: Python Flask "PlayGround" App

### 3.1 Backend (Flask)

#### Data store:
Precompute and pickle:
1. Song-level topic distributions: `{song_id: [θ₁,…,θ_K]}`
2. Artist-level topic distributions: `{artist_id: [θ₁,…,θ_K]}`
3. Topic labels from LLM: `["Love & Longing","Nostalgia","Party Vibe",…]`
4. Song metadata in SQLite or JSON for lookup

#### API endpoint (e.g., `POST /recommend`)

**Input JSON:**
```json
{ "artist_name": "Adele", "top_k": 10 }
```

**Pipeline:**
1. Lookup artist_id for "Adele"
2. Fetch θ_A
3. Compute similarity with every other artist's θ
4. Rank by similarity, keep top M (e.g., 100) candidates
5. Apply MMR or diversity re-ranking to select final `top_k`
6. For each recommended artist, find their top 2–3 topic labels
7. Fetch 2–3 representative songs per recommended artist
8. Return JSON payload:

```json
{
  "query_artist": "Adele",
  "recommendations": [
    {
      "artist": "Sam Smith",
      "score": 0.87,
      "topics": ["Broken Heart","Soulful Ballads"],
      "example_songs": ["Stay With Me", "Lay Me Down"]
    },
    {
      "artist": "Amy Winehouse",
      "score": 0.84,
      "topics": ["Retro Soul","Nostalgia"],
      "example_songs": ["Back to Black", "Rehab"]
    }
  ]
}
```

### 3.2 Frontend (HTML + simple JS)

A single page with:
1. Input box (autocomplete) for "Artist name"
2. "Recommend" button
3. Results display with:
   - Artist name (link to Spotify/YouTube)
   - Topic labels as colored tags
   - Example songs list
   - Optional bar chart showing topic distributions

### 3.3 Notebook / Visualization Tools

#### pyLDAvis for LDA:
```python
import pyLDAvis.gensim_models as gensimvis
import pyLDAvis
vis_data = gensimvis.prepare(lda_model, corpus, dictionary)
pyLDAvis.save_html(vis_data, 'lda_vis.html')
```

#### BERTopic Visualizations:
```python
from bertopic import BERTopic
topic_model = BERTopic(...)
topics = topic_model.fit_transform(docs)
topic_model.visualize_barchart(top_n_topics=10)     # top words per topic
topic_model.visualize_topics()                      # 2D UMAP plot
```

---

## 4. Alternative or Comparative Methods

### 4.1 Embedding-based (without explicit topics)
- Use Sentence-Transformer to encode lyrics into dense vectors
- Represent artists by averaging song embeddings
- Compute cosine similarity in embedding-space
- **Pros**: Captures semantic nuance that LDA might miss
- **Cons**: Harder to interpret "why" songs are similar

### 4.2 Hybrid LDA+Embeddings
- **Concatenate**: Form vector `[θ_song; E_song]`
- **Autoencoder**: Compress `[θ;E]` into lower-dim vector
- **Similarity**: Use cosine on the latent representation

### 4.3 Genre/Metadata-Aware Collaborative Filtering
- Use LDA topic distributions as side information
- Hybrid approach can mitigate cold-start for new artists

### 4.4 NMF + Clustering vs. LDA + LLM labels
- Test whether LDA vs. BERTopic vs. NMF yields better cohesion and downstream performance

---

## 5. Evaluation Strategy

### 5.1 Topic Model Quality (Intrinsic)

#### 1. Coherence Score (C_v or UMass)
- Compute coherence for LDA topics across different K
- Choose K with coherence plateau

#### 2. Perplexity (for LDA only)
- Track perplexity across held-out dev set

#### 3. Human Judgment (via LLM)
- Verify that ~80-90% of songs match LLM-generated labels
- Use crowdsourcing for validation

### 5.2 Recommendation Performance (Extrinsic)

#### 1. Offline Proxy Data
- Use artist "similarity" ground truth (e.g., Last.fm, Spotify)
- **Hit Rate @ K**: Check if top K contains ground-truth similar artists
- **Mean Reciprocal Rank (MRR)**: Measure ranking quality
- **Recall @ K** and **Precision @ K**: Standard IR metrics

#### 2. Diversity Metrics
- **Intra-list similarity (ILS)**: Average pairwise similarity in recommendations
  $$\mathrm{ILS}(R) = \frac{2}{N(N-1)} \sum_{i<j} \mathrm{sim}(r_i, r_j)$$
- **Coverage**: Fraction of all artists ever recommended
- **Aggregate Topic Coverage**: Count distinct topics in recommendations

#### 3. User-Centric Metrics
- **Playlist Concordance**: Overlap with user playlists
- **Novelty & Serendipity**: Measure rare but relevant recommendations

#### 4. A/B Testing (If Deployable)
- Compare different algorithms with real users
- Collect Likert-scale satisfaction scores

---

## 6. Key References & Further Reading

### LDA & Music Recommendation
- Liu, W., Heinzelman, W. "Topic‐based Music Recommendation Using Lyrics" (ICASSP 2010)
- Hu, X., Chao, W., Zhu, L., Peng, D. "Music Recommendation via Latent Dirichlet Allocation" (IJCAI 2011)

### Topic Modeling with Transformers
- Grootendorst, M. "BERTopic: Neural Topic Modeling with BERT" (arXiv:2203.05794 2022)

### Survey of Content‐Based Music Recommendation
- Schedl, M., et al. "Current Directions in Music Recommender Systems." (IRMUSIC 2018)

### Diversity & Novelty in Recommender Systems
- Zhang, Y., Steck, H., & Muthukrishnan, S. "Predicting the Popularity of Items." (WSDM 2015)
- Vargas, S., Castells, P. "Rank and Relevance in Novelty and Diversity Metrics" (RecSys 2011)

### LLM‐Assisted Topic Labeling
- "Document Topic Extraction with Large Language Models" by Akash Nath (Towards Data Science, 2023)
- Reimers, N., & Gurevych, I. "Sentence‐BERT: Sentence Embeddings using Siamese BERT‐Networks" (EMNLP 2019)

---

## 7. Evaluation: Step‐by‐Step Plan

### 1. Build & Evaluate Topic Models
- Split corpus into 80% train / 20% held-out
- Train LDA on 80%; compute coherence on held-out
- Train BERTopic and compare coherence
- Decide which variant yields more coherent topics

### 2. LLM Labeling & Human Validation
- Collect top 10 words from each topic
- Prompt GPT-4 for labels + justification
- Validate with human annotators (Cohen's κ or majority vote)

### 3. Build Recommendation Backend & Baselines
- **Baseline A**: Top-K cosine on LDA topics (no diversity)
- **Baseline B**: Embedding cosine from Sentence-BERT (no topics)
- **Your Model**: LDA + MMR diversity re-ranking

### 4. Offline Recommendation Evaluation
- Prepare ground truth from playlist co-occurrences
- Measure Recall@10, Precision@10, MRR, ILD
- Compare all baselines and your model

### 5. Qualitative & User Study
- Recruit 10-15 users
- Show randomized recommendation lists
- Collect preferability and diversity ratings

### 6. Visualization & Reporting
- 2D UMAP plots of artist topic distributions
- Bar charts comparing topic distributions
- Interactive visualizations with pyLDAvis/BERTopic

---

## 8. Timeline & Key Steps

| **Step** | **Description** | **Duration** |
|----------|-----------------|--------------|
| 1. Data Acquisition & Cleaning | Gather lyric corpus, map songs → artists, remove duplicates | 1 week |
| 2. Preprocessing & Dictionary Creation | Tokenize, lemmatize, build vocab, filter words | 1 week |
| 3. Train LDA & Evaluate Coherence | Sweep K ∈ {10,15,20,25,30}, pick best via C_v | 1 week |
| 4. Train BERTopic & Evaluate | Fit BERTopic, compute cluster coherence, compare to LDA | 1 week |
| 5. LLM Topic Labeling | Use LLM to label topics; validate via human study | 1–2 weeks |
| 6. Build Flask Backend & Data Pickles | Serialize distributions; implement similarity + diversity | 2 weeks |
| 7. Frontend Mockup | Simple HTML/JS page or Jupyter widget | 1 week |
| 8. Offline Evaluation | Run Recall@K, Precision@K, MRR, ILD metrics | 1 week |
| 9. User Study & Qualitative Analysis | Recruit 10–15 peers, collect feedback, analyze scores | 2 weeks |
| 10. Write‐Up & Visualization | Summarize results, produce charts, draft report | 2–3 weeks |

**Total estimated time: 10–12 weeks** (assuming part-time research)

---

## 9. Additional Tips & Caveats

### 1. Coverage of Lyrics
- Ensure corpus spans multiple genres (pop, rock, hip-hop, R&B, country)
- Filter out extremely short-lyric songs (< 50 words)
- Watch for genre bias in topic discovery

### 2. Scaling to Large Catalogs
- For > 50K songs, consider OnlineLDA or MiniBatchNMF
- Pre-compute BERTopic embeddings in batches

### 3. LLM Costs & Prompts
- Use GPT-3.5 for initial labeling (cheaper)
- Store final labels in CSV to avoid re-querying
- Craft prompts carefully for consistent results

### 4. Hyperparameter Sensitivity
- Tune LDA α ∈ {0.01, 0.1, 0.5} and β ∈ {0.01, 0.1}
- Adjust BERTopic's `min_topic_size` for granularity

### 5. Cold‐Start & New Songs
- Use trained LDA model's `.get_document_topics()` for new songs
- For new artists with one song, use that song's θ as artist θ

---

## 10. Conclusion

By following this approach, you will:

1. **Build multiple modeling pipelines** (LDA, BERTopic, NMF) yielding interpretable topic distributions
2. **Demonstrate LLM-assisted topic labeling** for human-readable cluster names
3. **Create an interactive Flask demo** showing topic-based recommendations
4. **Conduct comprehensive evaluation** comparing approaches and measuring diversity
5. **Contribute to research** on content-based, interpretable, diversity-aware music recommendation

The system addresses key gaps in existing music recommenders by focusing on lyrical content and providing interpretable, diverse recommendations that work well for cold-start scenarios.

---

*Good luck with your research project!*



