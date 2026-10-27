
"""
Upload the source CSV files (data/source_csv) to your ADLS Gen2 container.
Do this once; then set SOURCE_MODE=adls in .env and the pipeline reads from ADLS.
Any error is stored in audit.error_log (layer ADLS) and logs/pipeline_errors.log.

    python upload_to_adls.py
"""
import sys

import adls_utils
import config
from db_utils import capture_errors

if __name__ == "__main__":
    try:
        with capture_errors("ADLS", "PL_ADLS_CONNECT"):
            adls_utils.get_container().exists()             # raises with the real reason if it fails
            print(f"ADLS connection OK ({config.ADLS_CONTAINER})")
        with capture_errors("ADLS", "PL_ADLS_UPLOAD"):
            count = adls_utils.upload_folder(config.LOCAL_SOURCE_DIR)
        print(f"\n{count} files uploaded. Files now in {config.ADLS_CONTAINER}/{config.ADLS_FOLDER}:")
        for path in adls_utils.list_files():
            print("  ", path)
    except Exception as e:
        sys.exit(f"\nupload failed: {type(e).__name__}: {e}")
