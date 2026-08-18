"""GitHub PR approvals export utility.

This module provides functionality to fetch merged pull requests from a GitHub
repository using the GitHub CLI and generate a PDF report with approval details.

Example:
    `uv run python scripts/pr_approvals_github.py --repo jtviegas/bashutils --branch master --output approvals.pdf`
"""
import json
import subprocess
from typing import Any
import argparse

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import logging


def get_completed_prs(repo: str, branch: str, max_prs: int) -> list[dict[str, Any]]:
    """Fetch all merged pull requests using gh CLI."""
    result = subprocess.run(  # noqa: S603
        [  # noqa: S607
            "gh", "pr", "list",
            "--repo", repo,
            "--state", "merged",
            "--base", branch,
            "--json", "number,title,url,mergeCommit,mergedAt,reviews",
            "--limit", str(max_prs),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    prs: list[dict[str, Any]] = json.loads(result.stdout)
    logging.info("Retrieved %d merged PRs from repository '%s'", len(prs), repo)
    return prs


def get_approval_details(pr: dict[str, Any]) -> list[str]:
    """Extract approval details from PR reviews, keeping the latest state per reviewer."""
    reviews = pr.get("reviews", [])
    # Track the latest review state and timestamp per user (a reviewer may submit multiple reviews)
    latest: dict[str, tuple[str, str]] = {}
    for review in reviews:
        login = review.get("author", {}).get("login", "unknown")
        state = review.get("state", "")
        submitted_at = review.get("submittedAt", "")
        if state in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED"):
            latest[login] = (state, submitted_at)

    lines = []
    for login, (state, submitted_at) in latest.items():
        ts = f" at {submitted_at}" if submitted_at else ""
        if state == "APPROVED":
            lines.append(f"- {login} - Approved{ts}")
        elif state == "CHANGES_REQUESTED":
            lines.append(f"- {login} - Changes Requested{ts}")
        elif state == "DISMISSED":
            lines.append(f"- {login} - Dismissed{ts}")
    return lines


def get_merge_commit_id(pr: dict[str, Any]) -> str:
    """Return the merge commit SHA for a PR."""
    return pr.get("mergeCommit", {}).get("oid", "")


def generate_pdf(prs: list[dict[str, Any]], repo: str, filename: str = "approvals.pdf") -> None:
    """Generate PDF with approval details."""
    c = canvas.Canvas(filename, pagesize=letter)
    _, height = letter
    y = height - 40

    c.setFont("Helvetica-Bold", 14)
    c.drawString(40, y, "GitHub PR Approvals")
    y -= 30
    c.setFont("Helvetica", 10)

    for pr in prs:
        pr_num = pr["number"]
        title = pr.get("title", "")
        pr_url = pr.get("url", f"https://github.com/{repo}/pull/{pr_num}")
        approval_lines = get_approval_details(pr)
        commit_id = get_merge_commit_id(pr)
        merged_at = pr.get("mergedAt", "")

        # PR title and URL
        c.setFont("Helvetica-Bold", 10)
        c.drawString(40, y, f"PR #{pr_num}: {title}")
        c.setFont("Helvetica", 8)
        c.drawString(40, y - 12, pr_url)
        y -= 30
        c.setFont("Helvetica", 10)

        # Approvals
        c.drawString(60, y, "Approvals:")
        y -= 15
        if approval_lines:
            for line in approval_lines:
                c.drawString(80, y, line)
                y -= 15
        else:
            c.drawString(80, y, "- None")
            y -= 15

        # Commit ID and merge timestamp
        c.drawString(60, y, f"Merged: {merged_at}  |  Commit SHA: {commit_id}")
        y -= 25

        # Page break if needed
        if y < 60:
            c.showPage()
            y = height - 40

    c.save()


def main() -> None:
    """Main function to process PRs and generate PDF."""
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="GitHub PR approval export")
    parser.add_argument("--repo", help="Repository to fetch PRs from, format: 'owner/repo'")
    parser.add_argument("--output", default="approvals.pdf", help="Output PDF filename")
    parser.add_argument("--branch", default="main", help="Branch to fetch PRs from")
    parser.add_argument("--maxprs", default="5000", help="Maximum number of PRs to fetch")
    args = parser.parse_args()

    prs = get_completed_prs(args.repo, args.branch, int(args.maxprs))
    if prs:
        generate_pdf(prs, args.repo, args.output)


if __name__ == "__main__":
    main()
