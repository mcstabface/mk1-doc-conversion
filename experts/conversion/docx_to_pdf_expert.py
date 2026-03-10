from pathlib import Path
import subprocess
from typing import Dict
import hashlib


def convert_docx_to_pdf(artifact: Dict, output_dir: Path) -> Path:
    source_path = Path(artifact["physical_path"])

    output_dir.mkdir(parents=True, exist_ok=True)

    logical_path = artifact["logical_path"]
    short_hash = hashlib.sha256(logical_path.encode("utf-8")).hexdigest()[:12]
    output_pdf = output_dir / f"{source_path.stem}__{short_hash}.pdf"

    subprocess.run(
        [
            "libreoffice",
            "--headless",
            "--convert-to",
            "pdf",
            "--outdir",
            str(output_dir),
            str(source_path),
        ],
        check=True,
    )

    generated_pdf = output_dir / f"{source_path.stem}.pdf"

    if generated_pdf != output_pdf:
        generated_pdf.replace(output_pdf)

    return output_pdf