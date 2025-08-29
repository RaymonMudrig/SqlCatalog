import json
import numpy as np
from .paths import ITEMS_PATH, EMB_PATH, CATALOG

def load_items():
    return json.loads(ITEMS_PATH.read_text(encoding="utf-8"))

def load_emb():
    return np.load(EMB_PATH).astype("float32")

def load_catalog():
    return json.loads(CATALOG.read_text(encoding="utf-8"))
