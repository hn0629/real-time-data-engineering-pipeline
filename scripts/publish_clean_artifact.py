import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.clean_publish.publish_clean_artifact import main


if __name__ == "__main__":
    main()