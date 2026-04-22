# NLP Pipeline Hyperparameter Tuning Guide

This guide documents all adjustable hyperparameters in the NLP layer for pipeline testing and optimization.

## Table of Contents
- [BERTopic Topic Modeling Pipeline](#bertopic-topic-modeling-pipeline)
- [Named Entity Recognition (NER) Pipeline](#named-entity-recognition-ner-pipeline)
- [Quick Start Command Examples](#quick-start-command-examples)
- [Tuning Recommendations](#tuning-recommendations)

---

## BERTopic Topic Modeling Pipeline

### 1. Embedder (`embedder.py`)

Generates dense sentence embeddings for abstracts.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `model_name` | str | `"all-MiniLM-L6-v2"` | HuggingFace sentence transformer model. Options: `"all-MiniLM-L6-v2"` (fast), `"allenai-specter"` (scientific papers, slower) |
| `batch_size` | int | `64` | Abstracts encoded per forward pass. Reduce if OOM, increase for GPU throughput |
| `cache_path` | str | `"./cache/embeddings.npy"` | Path to cache computed embeddings (skip re-encoding for experiments) |
| `device` | str | auto-detect | Device for inference: `"cpu"` or `"cuda"` |

**Command-line flags:**
- `--embedding_model` 
- `--batch_size`
- `--embedding_cache`

---

### 2. UMAP Reducer (`reducer.py`)

Reduces high-dimensional embeddings before clustering.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `n_components` | int | `5` | Target dimensionality for clustering. Lower (3-5) for better HDBSCAN density estimation |
| `n_neighbors` | int | `15` | Number of neighbors for local manifold approximation. Higher preserves more global structure |
| `min_dist` | float | `0.0` | Minimum distance between points in low-dim space. Keep at 0.0 for tight clusters |
| `metric` | str | `"cosine"` | Distance metric. Use `"cosine"` for embeddings, `"euclidean"` for already-normalized data |
| `random_state` | int | `42` | Random seed for reproducibility |

**Command-line flags:**
- `--n_components`
- `--n_neighbors`

**Impact:**
- **n_components**: 5 is recommended. Too low (<3) loses topic separation. Too high (>10) makes HDBSCAN struggle.
- **n_neighbors**: Controls local vs global structure. 10-20 for most corpora. Lower for tighter, local clusters.

---

### 3. HDBSCAN Clusterer (`clusterer.py`)

Groups papers into topic clusters.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_cluster_size` | int | `10` | **Most important parameter.** Minimum papers to form a topic. Too small → micro-topics. Too large → over-merged topics |
| `min_samples` | int | `None` | Neighborhood size for core points. `None` = uses `min_cluster_size`. Higher = more conservative, more noise |
| `metric` | str | `"euclidean"` | Distance metric. Use `"euclidean"` for UMAP-reduced embeddings |
| `cluster_selection_method` | str | `"eom"` | `"eom"` (varying sizes) or `"leaf"` (uniform sizes) |
| `prediction_data` | bool | `True` | Enable soft assignment for new papers. Keep `True` for production |

**Command-line flags:**
- `--min_cluster_size`
- `--min_samples`

**Tuning guidelines:**
- **Small corpora (<10k papers)**: `min_cluster_size=10-20`
- **Medium corpora (10k-100k papers)**: `min_cluster_size=30-50`
- **Large corpora (>100k papers)**: `min_cluster_size=50-100`

Monitor noise ratio (should be 10-30%). If >30%, decrease `min_cluster_size`.

---

### 4. Topic Modeler (`topic_model.py`)

Core BERTopic configuration.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `top_n_words` | int | `10` | Number of keywords per topic label |
| `nr_topics` | int/str | `None` | Reduce to N topics after fitting. Use integer or `"auto"`. `None` = no reduction |
| `min_topic_size` | int | `10` | Minimum topic size (usually matches `min_cluster_size`) |

**Command-line flags:**
- `--top_n_words`
- `--nr_topics`

---

### 5. Corpus Loader (`loader.py`)

Controls which papers are loaded and processed.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_abstract_length` | int | `100` | Minimum abstract length in characters. Filters out incomplete records |
| `sources` | list[str] | `["arxiv", "s2orc", "openalex"]` | Data sources to load from HDFS |
| `date_from` | str | `None` | Load papers from this date (YYYY-MM-DD) |
| `date_to` | str | `None` | Load papers up to this date (YYYY-MM-DD) |

**Command-line flags:**
- `--min_abstract_length`
- `--sources` (comma-separated: `--sources arxiv,s2orc`)
- `--date_from`
- `--date_to`

---

## Named Entity Recognition (NER) Pipeline

### 6. NER Model (`Bert_ner/ner_model.py`)

Loads and runs BERT-based NER model.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `model_name_or_path` | str | required | HuggingFace NER model. Examples: `"dslim/bert-base-NER"`, custom fine-tuned model |
| `device` | str | auto-detect | `"cpu"` or `"cuda"` |
| `max_length` | int | `512` | Maximum sequence length (BERT limit) |

**Command-line flags:**
- `--model` (in `ner_main.py`)

---

### 7. Entity Extractor (`Bert_ner/extractor.py`)

Controls entity extraction and filtering.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `confidence_threshold` | float | `0.80` | Minimum confidence score for entity extraction. Lower = more entities but noisier |
| `batch_size` | int | `32` | Batch size for NER inference |

**Tuning:**
- Confidence threshold 0.70-0.90 depending on precision vs recall preference
- Higher threshold (0.90) = fewer, higher-quality entities
- Lower threshold (0.70) = more entities, some false positives

---

### 8. NER Pipeline Configuration (`Bert_ner/ner_main.py`)

Full-text and parallelization settings.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `n_jobs` | int | `1` | Number of parallel processes. **WARNING**: >1 on CPU is slow due to model reload overhead. Use `1` for CPU |
| `max_sections` | int | `5` | Max sections per paper in full-text mode. Introduction + methods are most entity-rich |
| `batch_size` | int | `16` | Batch size for NER inference |

**Command-line flags:**
- `--n_jobs`
- `--batch_size`
- `--max_sections`
- `--fulltext` (enables full-text mode)

---

### 9. Tokenizer (`shared/Tokenizer.py`)

spaCy sentence splitting and tokenization.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `model_name` | str | `"en_core_web_sm"` | spaCy model. Options: `"en_core_web_sm"` (fast), `"en_core_web_trf"` (transformer, accurate) |
| `disable` | list[str] | `["ner", "lemmatizer"]` | Pipeline components to disable for speed |
| `batch_size` | int | `64` | Batch size for spaCy processing |

---

### 10. Parallel Executor (`shared/parallelization.py`)

Joblib parallelization for CPU-bound NLP tasks.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `n_jobs` | int | `-2` | CPU cores to use. `-1` = all cores, `-2` = all minus one |
| `backend` | str | `"loky"` | Joblib backend. `"loky"` for CPU tasks, `"threading"` for GIL-releasing ops |
| `verbose` | int | `0` | Joblib verbosity (0 = silent, 10 = detailed) |

---

## Quick Start Command Examples

### BERTopic: Standard Run (arXiv papers only)
```bash
python -m NLP_layer.run_bertopic \
    --input_path /user/research-intelligence/raw \
    --output_path /user/research-intelligence/raw/bertopic \
    --embedding_cache ./cache/embeddings.npy \
    --sources arxiv \
    --min_cluster_size 10 \
    --n_components 5 \
    --n_neighbors 15 \
    --embedding_model all-MiniLM-L6-v2
```

### BERTopic: Large Corpus (100k+ papers)
```bash
python -m NLP_layer.run_bertopic \
    --input_path /user/research-intelligence/raw \
    --output_path /user/research-intelligence/raw/bertopic \
    --embedding_cache ./cache/embeddings.npy \
    --sources arxiv,s2orc,openalex \
    --min_cluster_size 50 \
    --min_samples 30 \
    --n_components 5 \
    --n_neighbors 20 \
    --batch_size 128 \
    --embedding_model all-MiniLM-L6-v2
```

### BERTopic: Re-cluster Without Re-embedding
Embeddings are cached — adjust clustering params without re-encoding:
```bash
python -m NLP_layer.run_bertopic \
    --input_path /user/research-intelligence/raw \
    --output_path /user/research-intelligence/raw/bertopic \
    --embedding_cache ./cache/embeddings.npy \
    --min_cluster_size 20 \
    --nr_topics 50
```

### BERTopic: High-Quality Model (Slower)
```bash
python -m NLP_layer.run_bertopic \
    --input_path /user/research-intelligence/raw \
    --output_path /user/research-intelligence/raw/bertopic \
    --embedding_cache ./cache/specter_embeddings.npy \
    --embedding_model allenai-specter \
    --batch_size 32 \
    --min_cluster_size 30
```

### NER: Abstract-Based Extraction
```bash
python -m NLP_layer.Bert_ner.ner_main \
    --model dslim/bert-base-NER \
    --input_path /user/research-intelligence/raw \
    --output_path /user/research-intelligence/raw/ner \
    --sources arxiv \
    --n_jobs 1 \
    --batch_size 32
```

### NER: Full-Text Extraction
```bash
python -m NLP_layer.Bert_ner.ner_main \
    --model dslim/bert-base-NER \
    --input_path /user/research-intelligence/raw \
    --output_path /user/research-intelligence/raw/ner \
    --fulltext \
    --max_sections 5 \
    --n_jobs 1 \
    --batch_size 16
```

---

## Tuning Recommendations

### Priority Parameters (Tune These First)

**BERTopic:**
1. **`min_cluster_size`** — Most impactful for topic quality
   - Start: 10 for small corpora, 50 for large corpora
   - Goal: 10-30% noise ratio, 50-200 topics for interpretability

2. **`n_neighbors`** — Affects cluster granularity
   - Start: 15
   - Increase to 20-30 for more global, merged topics
   - Decrease to 10-12 for tighter, specialized topics

3. **`embedding_model`** — Quality vs speed tradeoff
   - Fast: `all-MiniLM-L6-v2` (384 dims, 2-3x faster)
   - High-quality: `allenai-specter` (768 dims, fine-tuned on scientific papers)

**NER:**
1. **`confidence_threshold`** — Precision vs recall
   - High precision: 0.85-0.90
   - Balanced: 0.80
   - High recall: 0.70-0.75

---

### Monitoring Metrics

**BERTopic:**
- **Noise ratio**: `(papers with topic_id=-1) / total_papers`
  - Target: 10-30%
  - Too high (>40%): Decrease `min_cluster_size`
  - Too low (<5%): Increase `min_cluster_size` to avoid micro-topics

- **Number of topics**:
  - Target: 50-200 for most corpora
  - Too many (>500): Increase `min_cluster_size`
  - Too few (<20): Decrease `min_cluster_size`

- **Topic coherence**: Manual review of top keywords
  - Keywords should be semantically related
  - If incoherent, try higher `n_neighbors` or different `embedding_model`

**NER:**
- **Entities per paper**: Should average 3-10 for abstracts, 10-30 for full-text
- **Entity distribution**: Check balance across METHOD/DATASET/TASK types
- **False positive rate**: Sample 50-100 entities manually, check accuracy

---

### Common Issues & Solutions

| Issue | Likely Cause | Solution |
|-------|-------------|----------|
| Too many micro-topics (>500 topics) | `min_cluster_size` too small | Increase to 30-50 |
| Very high noise ratio (>40%) | `min_cluster_size` too large | Decrease to 10-20 |
| Topics semantically incoherent | Embeddings not capturing domain semantics | Switch to `allenai-specter` model |
| Embedding step too slow | Large batch size on CPU | Reduce `batch_size` to 32 or use GPU |
| NER extracting too few entities | `confidence_threshold` too high | Lower to 0.75 |
| NER extracting noisy entities | `confidence_threshold` too low | Raise to 0.85-0.90 |
| Out of memory during embedding | `batch_size` too large | Reduce to 16 or 32 |

---

### Experiment Tracking Template

When testing different configurations, track:

```markdown
## Experiment: [Name/Date]

**Corpus:**
- Sources: arxiv, s2orc, openalex
- Papers loaded: X
- Date range: YYYY-MM-DD to YYYY-MM-DD

**Hyperparameters:**
- embedding_model: all-MiniLM-L6-v2
- min_cluster_size: 30
- min_samples: None
- n_components: 5
- n_neighbors: 15

**Results:**
- Topics discovered: X
- Noise ratio: X%
- Avg papers per topic: X
- Embedding time: X min
- Clustering time: X min

**Quality Assessment:**
- Topic coherence: [Good/Fair/Poor]
- Example topics: [List 3-5 best/worst topics]
- Next steps: [Adjustments to try]
```

---

## Advanced: Hyperparameter Search

For systematic tuning, iterate over a grid:

**BERTopic Grid Search:**
```python
min_cluster_sizes = [10, 20, 30, 50]
n_neighbors_list = [10, 15, 20, 30]
n_components_list = [3, 5, 7]

# Run pipeline for each combination, track metrics
```

**Automation script coming soon:** `scripts/hyperparameter_search.py`

---

## Questions or Issues?

If you encounter unexpected behavior or need guidance:
1. Check the docstrings in each module (`embedder.py`, `clusterer.py`, etc.)
2. Review the command-line help: `python -m NLP_layer.run_bertopic --help`
3. Consult the team Slack channel or open a GitHub issue

Last updated: April 2026
