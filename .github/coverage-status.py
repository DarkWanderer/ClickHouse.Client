from pycobertura import Cobertura
import argparse
import os
import requests


github_token = os.getenv("GITHUB_TOKEN")


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--coverage-file", required=True,
                        help="Path of pycobertura coverage XML file")
    parser.add_argument("--coverage-diff-file",
                        help="Path of pycobertura diff .json file")
    parser.add_argument("--repository", required=True,
                        help="owner/name of repository")
    parser.add_argument("--sha", required=True, help="SHA hash of commit")

    return parser.parse_args()


def post_status(repository: str, sha: str, state: str, context: str, description: str):
    url = f"https://api.github.com/repos/{repository}/statuses/{sha}"
    data = {
        "context": context,
        "description": description,
        "state": state,
    }
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {github_token}",
    }

    requests.post(url, json=data, headers=headers).raise_for_status()


if __name__ == "__main__":
    args = parse_args()

    cobertura = Cobertura(args.coverage_file)

    post_status(args.repository, args.sha, "success", "Coverage / Total",
                f"line: {cobertura.line_rate() * 100:.2f}% branch: {cobertura.branch_rate() * 100:.2f}%")
