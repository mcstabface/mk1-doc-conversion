from pathlib import Path
import subprocess
from typing import Dict
import hashlib
import shutil
import subprocess


def ensure_libreoffice_available():
    """
    Ensure LibreOffice is available on the system PATH.
    Raises a RuntimeError with a clear message if not found.
    """

    soffice = shutil.which("soffice") or shutil.which("soffice.exe")

    if soffice is None:
        raise RuntimeError(
            "LibreOffice not found.\n"
            "This tool requires LibreOffice for document conversion.\n"
            "Install it from: https://www.libreoffice.org/download/\n"
            "After installation, restart the terminal and try again."
        )

    # Optional: verify it actually runs
    try:
        subprocess.run(
            [soffice, "--version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
    except Exception:
        raise RuntimeError(
            "LibreOffice was detected but could not be executed.\n"
            "Please verify the LibreOffice installation."
        )

    return soffice

def convert_docx_to_pdf(artifact: Dict, output_dir: Path) -> Path:
    source_path = Path(artifact["physical_path"])

    output_dir.mkdir(parents=True, exist_ok=True)

    logical_path = artifact["logical_path"]
    short_hash = hashlib.sha256(logical_path.encode("utf-8")).hexdigest()[:12]
    output_pdf = output_dir / f"{source_path.stem}__{short_hash}.pdf"

    soffice = ensure_libreoffice_available()

    subprocess.run(
        [
            soffice,
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