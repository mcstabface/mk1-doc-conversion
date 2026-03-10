from pathlib import Path
import subprocess
from typing import Dict


def convert_docx_to_pdf(artifact: Dict, output_dir: Path) -> Path:
    source_path = Path(artifact["physical_path"])

    output_dir.mkdir(parents=True, exist_ok=True)

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

    output_pdf = output_dir / (source_path.stem + ".pdf")
    return output_pdf