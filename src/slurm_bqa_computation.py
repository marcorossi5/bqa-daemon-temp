"""Run circuit read from QASM file"""

import argparse
import json
import logging
from contextlib import ContextDecorator
from pathlib import Path

from bqa import run_qa


class TemporaryLoggingLevel(ContextDecorator):
    def __init__(self, logger_name, level):
        self.logger_name = logger_name
        self.level = level
        self.original_level = None

    def __enter__(self):
        logger = logging.getLogger(self.logger_name)
        self.original_level = logger.level
        logger.setLevel(self.level)

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger = logging.getLogger(self.logger_name)
        logger.setLevel(self.original_level)


@TemporaryLoggingLevel("qibo.config", 100)
def load_circuit_from_json(circuit_path: Path) -> qibo.Circuit:
    with open(circuit_path) as f:
        raw = json.load(f)
    return qibo.Circuit.from_dict(raw)


def save_qibo_libs_versions(job_folder: Path):
    versions_dict = {"qibo_version": qibo.__version__}
    try:
        import qibolab

        versions_dict["qibolab_version"] = qibolab.__version__
    except ImportError:
        pass
    versions_path = job_folder / "versions.json"
    versions_path.write_text(json.dumps(versions_dict))


def main(job_folder: Path):
    print("Save libraries versions")
    save_qibo_libs_versions(job_folder)

    print("Load circuit from file")
    bqa_input_path = job_folder / "bqa_input.json"
    with open(bqa_input_path) as f:
        bqa_input = json.load(f)

    # bqa_output = run_qa(raw)
    bqa_output = {"result": 0, **bqa_input}

    output_path = job_folder / "bqa_output.json"
    print(f"Saving results to {output_path.name}")
    with open(output_path, "w") as f:
        json.dump(bqa_output, f)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("job_folder", type=Path)
    args = parser.parse_args()
    main(args.job_folder)
