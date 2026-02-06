import os
import platform
import sys
from pathlib import Path

import chartbook

sys.path.insert(1, "./src/")

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"
OUTPUT_DIR = BASE_DIR / "_output"
OS_TYPE = "nix" if platform.system() != "Windows" else "windows"

os.environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"


def jupyter_execute_notebook(notebook_path):
    return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"

def jupyter_to_html(notebook_path, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --output-dir={output_dir} {notebook_path}"


def mv(from_path, to_path):
    from_path = Path(from_path)
    to_path = Path(to_path)
    to_path.mkdir(parents=True, exist_ok=True)
    if OS_TYPE == "nix":
        command = f"mv {from_path} {to_path}"
    else:
        command = f"move {from_path} {to_path}"
    return command


def task_config():
    def create_dirs():
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return {
        "actions": [create_dirs],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "verbosity": 2,
    }


def task_pull():
    """Pull NYU Call Report data"""
    return {
        "actions": [
            f"python ./src/pull_nyu_call_report.py",
        ],
        "targets": [
            DATA_DIR / "nyu_call_report.parquet",
        ],
        "file_dep": [
            f"./src/pull_nyu_call_report.py",
        ],
        "clean": [],
    }


def task_format():
    """Format data into standardized FTSFR datasets"""
    return {
        "actions": [
            f"python ./src/create_ftsfr_datasets.py",
        ],
        "targets": [
            DATA_DIR / "ftsfr_nyu_call_report_leverage.parquet",
            DATA_DIR / "ftsfr_nyu_call_report_holding_company_leverage.parquet",
            DATA_DIR / "ftsfr_nyu_call_report_cash_liquidity.parquet",
            DATA_DIR / "ftsfr_nyu_call_report_holding_company_cash_liquidity.parquet",
        ],
        "file_dep": [
            f"./src/create_ftsfr_datasets.py",
            DATA_DIR / "nyu_call_report.parquet",
        ],
        "clean": [],
    }


def task_aggregate():
    """Create aggregated leverage datasets by size quartiles"""
    return {
        "actions": [
            "python ./src/create_aggregated_leverage.py",
        ],
        "targets": [
            DATA_DIR / "ftsfr_nyu_call_report_leverage_ew_quartile.parquet",
            DATA_DIR / "ftsfr_nyu_call_report_leverage_vw_quartile.parquet",
        ],
        "file_dep": [
            "./src/create_aggregated_leverage.py",
            DATA_DIR / "nyu_call_report.parquet",
        ],
        "clean": [],
    }


def task_generate_charts():
    """Generate aggregated leverage charts"""
    return {
        "actions": [
            "python ./src/generate_chart.py",
        ],
        "targets": [
            OUTPUT_DIR / "bank_leverage_ew_quartile.html",
            OUTPUT_DIR / "bank_leverage_vw_quartile.html",
        ],
        "file_dep": [
            "./src/generate_chart.py",
            DATA_DIR / "ftsfr_nyu_call_report_leverage_ew_quartile.parquet",
            DATA_DIR / "ftsfr_nyu_call_report_leverage_vw_quartile.parquet",
        ],
        "clean": [],
    }


notebook_tasks = {
    "summary_nyu_call_report_ipynb": {
        "path": "./src/summary_nyu_call_report_ipynb.py",
        "file_dep": [
            DATA_DIR / "ftsfr_nyu_call_report_leverage.parquet",
        ],
        "targets": [],
    },
}
notebook_files = []
for notebook in notebook_tasks.keys():
    pyfile_path = Path(notebook_tasks[notebook]["path"])
    notebook_files.append(pyfile_path)


def task_run_notebooks():
    for notebook in notebook_tasks.keys():
        pyfile_path = Path(notebook_tasks[notebook]["path"])
        notebook_path = pyfile_path.with_suffix(".ipynb")
        yield {
            "name": notebook,
            "actions": [
                f"jupytext --to notebook --output {notebook_path} {pyfile_path}",
                jupyter_execute_notebook(notebook_path),
                jupyter_to_html(notebook_path),
                mv(notebook_path, OUTPUT_DIR),
            ],
            "file_dep": [
                pyfile_path,
                *notebook_tasks[notebook]["file_dep"],
            ],
            "targets": [
                OUTPUT_DIR / f"{notebook}.html",
                *notebook_tasks[notebook]["targets"],
            ],
            "clean": True,
        }


def task_generate_pipeline_site():
    return {
        "actions": [
            "chartbook build -f",
        ],
        "targets": ["docs/index.html"],
        "file_dep": [
            "chartbook.toml",
            *notebook_files,
            OUTPUT_DIR / "bank_leverage_ew_quartile.html",
            OUTPUT_DIR / "bank_leverage_vw_quartile.html",
        ],
    }
