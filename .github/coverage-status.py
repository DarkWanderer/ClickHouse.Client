from pycobertura import Cobertura
import argparse
import os
import requests

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--coverage-file", required=True, help="Name of pycobertura XML file for coverage")
    parser.add_argument("--repository", required=True, help="owner/name of repository")
    parser.add_argument("--sha", required=True, help="SHA hash of commit")

    return parser.parse_args()

if __name__ == "__main__":
    github_token = os.getenv("GITHUB_TOKEN")
    args = parse_args()
    
    cobertura = Cobertura(args.coverage_file)

    url = f"https://api.github.com/repos/{args.repository}/check-runs"
    data = {
        'name': 'coverage',
        'conclusion': 'success',
        'output': {
            'title': 'Total code coverage',
            'summary': f'Coverage: {cobertura.line_rate() * 100:.2f}%'
        }
    }
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {github_token}",
    }

    requests.post(url, json=data, headers=headers).raise_for_status()
