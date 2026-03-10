from typing import Dict, List


CONVERTIBLE_TYPES = {"docx", "doc", "rtf"}


def select_convertible_artifacts(artifacts: List[Dict]) -> List[Dict]:
    results: List[Dict] = []

    for artifact in artifacts:
        if artifact["source_type"] in CONVERTIBLE_TYPES:
            results.append(artifact)

    return results