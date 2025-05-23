# docs/source/api/test_dataframe.py
"""Tests for the DataFrame API documentation in RST format.

This script validates the RST syntax, links, and structure of the DataFrame
API documentation files.
"""

import os
import sys
from pathlib import Path
from typing import List, Tuple

from docutils.core import publish_doctree, publish_parts
from docutils.parsers.rst import Parser
from docutils.utils import new_document, SystemMessage

def test_rst_syntax(file_path: str) -> List[SystemMessage]:
    """Test if the RST file has valid syntax.
    
    Args:
        file_path: Path to the RST file to test
        
    Returns:
        List of error messages, empty if no errors
    """
    with open(file_path, "r", encoding="utf-8") as rst_file:
        content = rst_file.read()
    
    parser = Parser()
    settings = {}
    document = new_document("test", settings)
    
    # Parse the document and capture any errors/warnings
    parser.parse(content, document)
    
    return [msg for msg in document.traverse(condition=SystemMessage)]


def test_build_rst(file_path: str) -> Tuple[bool, List[str]]:
    """Test if the RST file can be built into HTML without errors.
    
    Args:
        file_path: Path to the RST file to test
        
    Returns:
        Tuple containing (success status, list of error messages)
    """
    with open(file_path, "r", encoding="utf-8") as rst_file:
        content = rst_file.read()
    
    try:
        # Try to build the document to HTML
        publish_parts(
            source=content,
            writer_name="html5",
            settings_overrides={"halt_level": 2}  # Stop at warning level
        )
        return True, []
    except Exception as e:
        return False, [str(e)]


def test_code_blocks(file_path: str) -> List[str]:
    """Test if code blocks in the RST file are properly formatted.
    
    Args:
        file_path: Path to the RST file to test
        
    Returns:
        List of error messages, empty if no errors
    """
    with open(file_path, "r", encoding="utf-8") as rst_file:
        content = rst_file.read()
    
    errors = []
    lines = content.split("\n")
    in_code_block = False
    code_block_indent = 0
    
    for i, line in enumerate(lines, 1):
        if ".. code-block::" in line:
            in_code_block = True
            code_block_indent = len(line) - len(line.lstrip())
        elif in_code_block and line.strip() and not line.startswith(" " * (code_block_indent + 4)):
            # Code block content should be indented by at least 4 spaces
            if not line.strip().startswith(".. "):  # Skip RST directives
                errors.append(f"Line {i}: Code block not properly indented")
                in_code_block = False
        elif in_code_block and not line.strip():
            # Empty line within code block, still in code block
            pass
        elif in_code_block:
            # If line doesn't start with proper indentation, we're out of the code block
            if not line.startswith(" " * (code_block_indent + 4)):
                in_code_block = False
    
    return errors


def test_internal_links(file_path: str) -> List[str]:
    """Test if internal links in the RST file point to valid sections.
    
    Args:
        file_path: Path to the RST file to test
        
    Returns:
        List of error messages, empty if no errors
    """
    with open(file_path, "r", encoding="utf-8") as rst_file:
        content = rst_file.read()
    
    errors = []
    
    # Extract section titles
    section_titles = []
    lines = content.split("\n")
    for i, line in enumerate(lines):
        if i > 0 and len(lines[i-1].strip()) > 0:
            if all(c == "=" for c in line.strip()) or all(c == "-" for c in line.strip()) or all(c == "~" for c in line.strip()):
                section_titles.append(lines[i-1].strip())
    
    # Check if internal links point to valid sections
    tree = publish_doctree(content)
    for node in tree.traverse():
        if node.tagname == "reference" and "refuri" in node.attributes:
            ref_uri = node.attributes["refuri"]
            if ref_uri.startswith("#"):
                link_target = ref_uri[1:]
                # Normalize target by removing spaces and converting to lowercase
                normalized_target = link_target.lower().replace(" ", "-")
                # Check if target exists in section titles
                found = False
                for title in section_titles:
                    if normalized_target == title.lower().replace(" ", "-"):
                        found = True
                        break
                if not found:
                    errors.append(f"Internal link to '#{link_target}' does not match any section title")
    
    return errors


def main():
    """Run all tests on the DataFrame RST documentation."""
    # Get the path to the RST file
    current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    dataframe_rst_path = current_dir / "dataframe.rst"
    
    if not dataframe_rst_path.exists():
        print(f"Error: File not found: {dataframe_rst_path}")
        return 1
    
    # Run tests
    print(f"Testing {dataframe_rst_path}...")
    
    syntax_errors = test_rst_syntax(str(dataframe_rst_path))
    if syntax_errors:
        print("RST syntax errors found:")
        for error in syntax_errors:
            print(f"  - {error}")
    else:
        print("✓ RST syntax is valid")
    
    code_block_errors = test_code_blocks(str(dataframe_rst_path))
    if code_block_errors:
        print("Code block errors found:")
        for error in code_block_errors:
            print(f"  - {error}")
    else:
        print("✓ Code blocks are valid")
    
    link_errors = test_internal_links(str(dataframe_rst_path))
    if link_errors:
        print("Internal link errors found:")
        for error in link_errors:
            print(f"  - {error}")
    else:
        print("✓ Internal links are valid")
    
    build_success, build_errors = test_build_rst(str(dataframe_rst_path))
    if not build_success:
        print("Build errors found:")
        for error in build_errors:
            print(f"  - {error}")
    else:
        print("✓ Document builds successfully")
    
    # Overall result
    if syntax_errors or code_block_errors or link_errors or not build_success:
        print("\n❌ Tests failed")
        return 1
    else:
        print("\n✅ All tests passed")
        return 0


if __name__ == "__main__":
    sys.exit(main())