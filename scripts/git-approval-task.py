"""GitHub PR approval export script."""

import json
import os
import subprocess
from typing import Any

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import logging


# Configuration
REPO = os.getenv("GITHUB_REPO")  # format: "owner/repo"
if not REPO:
    raise ValueError("GITHUB_REPO environment variable must be set (format: owner/repo)")

MAIN_BRANCH = os.getenv("GITHUB_MAIN_BRANCH", "master")
PR_MAX = int(os.getenv("GITHUB_PR_MAX", "5000"))  # max PRs to retrieve


def get_completed_prs() -> list[dict[str, Any]]:
    """Fetch all merged pull requests using gh CLI."""
    result = subprocess.run(
        [
            "gh", "pr", "list",
            "--repo", REPO,
            "--state", "merged",
            "--base", MAIN_BRANCH,
            "--json", "number,title,url,mergeCommit,mergedAt,reviews",
            "--limit", str(PR_MAX),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    prs: list[dict[str, Any]] = json.loads(result.stdout)
    logging.info("Retrieved %d merged PRs from repository '%s'", len(prs), REPO)
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


def generate_pdf(prs: list[dict[str, Any]], filename: str = "approvals.pdf") -> None:
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
        pr_url = pr.get("url", f"https://github.com/{REPO}/pull/{pr_num}")
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
    prs = get_completed_prs()
    if prs:
        generate_pdf(prs)


if __name__ == "__main__":
    main()
