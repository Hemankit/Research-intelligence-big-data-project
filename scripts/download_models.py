#!/usr/bin/env python3
"""
download_models.py
------------------
One-time setup script to download all HuggingFace models required
by the Research Intelligence Pipeline before first use.

Run this once before starting FastAPI:
    python scripts/download_models.py

Models downloaded (~1.5GB total):
    - allenai/scibert_scivocab_cased   (~440MB); Selective full-text analysis
    - dslim/bert-base-NER              (~430MB); Named entity recognition
    - all-MiniLM-L6-v2                 (~90MB); BERTopic sentence embeddings
"""

import sys

def download_scibert():
    print("Downloading allenai/scibert_scivocab_cased (~440MB)...")
    try:
        from transformers import AutoTokenizer, AutoModelForSequenceClassification
        AutoTokenizer.from_pretrained("allenai/scibert_scivocab_cased")
        AutoModelForSequenceClassification.from_pretrained("allenai/scibert_scivocab_cased")
        print("  SciBERT downloaded successfully.")
    except Exception as e:
        print(f"  ERROR downloading SciBERT: {e}")
        return False
    return True

def download_bert_ner():
    print("Downloading dslim/bert-base-NER (~430MB)...")
    try:
        from transformers import AutoTokenizer, AutoModelForTokenClassification
        AutoTokenizer.from_pretrained("dslim/bert-base-NER")
        AutoModelForTokenClassification.from_pretrained("dslim/bert-base-NER")
        print("  bert-base-NER downloaded successfully.")
    except Exception as e:
        print(f"  ERROR downloading bert-base-NER: {e}")
        return False
    return True

def download_sentence_transformer():
    print("Downloading all-MiniLM-L6-v2 (~90MB)...")
    try:
        from sentence_transformers import SentenceTransformer
        SentenceTransformer("all-MiniLM-L6-v2")
        print("  all-MiniLM-L6-v2 downloaded successfully.")
    except Exception as e:
        print(f"  ERROR downloading sentence transformer: {e}")
        return False
    return True

def download_spacy():
    print("Downloading spaCy en_core_web_sm...")
    try:
        import spacy
        try:
            spacy.load("en_core_web_sm")
            print("  en_core_web_sm already installed.")
        except OSError:
            import subprocess
            subprocess.run([sys.executable, "-m", "spacy", "download", "en_core_web_sm"], check=True)
            print("  en_core_web_sm downloaded successfully.")
    except Exception as e:
        print(f"  ERROR downloading spaCy model: {e}")
        return False
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("  Research Intelligence — Model Download Script")
    print("=" * 60)
    print("This will download ~1.5GB of model files.")
    print("Models are cached locally and only downloaded once.")
    print()

    results = {
        "SciBERT (sentence classifier)": download_scibert(),
        "bert-base-NER (entity recognition)": download_bert_ner(),
        "all-MiniLM-L6-v2 (topic embeddings)": download_sentence_transformer(),
        "spaCy en_core_web_sm (tokenizer)": download_spacy(),
    }

    print()
    print("=" * 60)
    print("  Download Summary")
    print("=" * 60)
    all_ok = True
    for model, ok in results.items():
        status = "OK" if ok else "FAILED"
        print(f"  [{status}] {model}")
        if not ok:
            all_ok = False

    print()
    if all_ok:
        print("All models downloaded. You can now start FastAPI:")
        print("  python -m uvicorn api.app:app --port 8000")
    else:
        print("Some models failed to download. Check your internet connection")
        print("and rerun this script before starting FastAPI.")
    print("=" * 60)
