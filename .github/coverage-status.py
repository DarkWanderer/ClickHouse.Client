from pycobertura import Cobertura
import argparse
import json
import os
import requests


github_token = os.getenv("GITHUB_TOKEN")


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--coverage-file", required=True,
                        help="Path of cobertura XML file or pycobertura .diff.json")
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

    name, ext = os.path.splitext(args.coverage_file)
    if ext == '.xml':
        cobertura = Cobertura(args.coverage_file)
        post_status(args.repository, args.sha, "success", "Coverage / Total",
                    f"line: {cobertura.line_rate() * 100:.2f}% branch: {cobertura.branch_rate() * 100:.2f}%")
    elif ext == '.json':
        with open(args.coverage_file) as f:
            data = json.load(f)
            chnge = data['total']["Cover"]
            stmts = data['total']["Stmts"]
            post_status(args.repository, args.sha, "success", "Coverage / Diff",
                        f"change: {chnge}, statements: {stmts}")
    else:
        raise Exception(f'Unknown file type {ext} for {args.coverage_file}')
