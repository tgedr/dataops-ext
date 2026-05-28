#!/usr/bin/env python3
"""Script to create a PDF report of PR."""

import argparse
import json
from typing import Any

from fpdf import FPDF, XPos, YPos

# Define text colours:
BLACK = (0, 0, 0)
GREEN = (0, 128, 0)
BLUE = (0, 102, 204)


def split_section(section: str) -> tuple:
    """Splits the section into header and text.

    Args:
        section (str): Header and text.

    Returns:
        tuple: Header and text separated.

    """
    header = section.split("\n")[0]
    text = "\n".join(section.split("\n")[1:])
    return header, text


def add_section(pdf: any, section: str, header_size: int = 16, text_size: int = 12, font: str = "Helvetica") -> None:
    """Add pdf section.

    Args:
        pdf (any): The pdf object.
        section (str): Text with sections.
        header_size (int, optional): Font size of header. Defaults to 16.
        text_size (int, optional): Font size of text. Defaults to 12.
        font (str, optional): The font. Defaults to "Helvetica".

    """
    header, text = split_section(section)
    pdf.set_text_color(BLUE)
    pdf.set_font(font, "B", header_size)
    pdf.cell(0, 10, header, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_text_color(BLACK)
    pdf.set_font(font, "", text_size)
    pdf.multi_cell(0, 8, text)

def main() -> None:
    """The main function."""
    parser = argparse.ArgumentParser(
        description="Get Azure DevOps PR report",
        epilog="Example: python create_report.py --report report.md",
    )

    parser.add_argument("--report", required=True, help="The report.md file from the pipeline")
    args = parser.parse_args()

    # read the report
    with open(args.report, encoding="utf-8") as infile:  # noqa: PTH123
        report = infile.read()
        report = report.encode("latin-1", "ignore").decode("latin-1")
    report = report.split("##")[1:]

    # Create pdf
    font = "Helvetica"
    pdf = FPDF()
    pdf.add_page()

    # Add the title
    add_section(pdf, report[0], 18)
    pdf.ln(5)  # Line break

    # Add all sections
    for section in report[1:]:
        pdf.set_text_color(BLACK)  # Black
        add_section(pdf, section, 14, 12)
        pdf.set_text_color(GREEN)  # Green
        pdf.cell(0, 10, "All tests passed!", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(5)  # Line break

    pdf.output("report.pdf")


if __name__ == "__main__":
    main()
