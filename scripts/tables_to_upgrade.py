import subprocess
import json
import argparse


def get_tables_to_upgrade(project, dataset, table_prefix, labels_version):
    # Run bq ls to get tables in prettyjson format
    bq_ls_cmd = [
        "bq",
        "ls",
        "-n",
        "100000",
        "--format=prettyjson",
        f"{project}:{dataset}",
    ]
    result = subprocess.run(bq_ls_cmd, capture_output=True, text=True, check=True)
    tables = json.loads(result.stdout)

    # Filter tables matching prefix and not having the target version label
    filtered = []
    for table in tables:
        table_id = table["tableReference"]["tableId"]
        if (
            table_id.startswith(table_prefix)
            and table.get("labels", {}).get("version", "") != labels_version
        ):
            full_table_id = f"{project}:{dataset}.{table_id}"
            filtered.append(full_table_id)
    return filtered


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Filter BigQuery tables by prefix and version label."
    )
    parser.add_argument(
        "--table_prefix",
        required=True,
        help="Fully qualified table prefix (project:dataset.prefix)",
    )
    parser.add_argument(
        "--labels_version", required=True, help="Target version label to filter out"
    )
    args = parser.parse_args()

    # Parse table_prefix into project, dataset, and prefix
    try:
        project_dataset, prefix = args.table_prefix.split(".", 1)
        project, dataset = project_dataset.split(":", 1)
    except ValueError:
        raise ValueError("table_prefix must be in the format project:dataset.prefix")

    filtered_tables = get_tables_to_upgrade(
        project, dataset, prefix, args.labels_version
    )
    for table in filtered_tables:
        print(table)
