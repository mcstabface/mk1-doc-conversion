import re

_TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")

_STOPWORDS = {
    "the","and","for","are","with","from","that","this",
    "what","when","where","which","who","whom","whose","why","how",
    "did","does","do","is","was","were","be","been","being",
    "regarding","discuss","discussed","about",
}

def tokenize(text: str) -> set[str]:
    raw = _TOKEN_RE.findall(text.lower())
    return {
        token
        for token in raw
        if len(token) >= 3 and token not in _STOPWORDS
    }