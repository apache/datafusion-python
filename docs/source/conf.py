# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Documentation generation."""

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))

# -- Project information -----------------------------------------------------

project = "Apache DataFusion in Python"
copyright = "2019-2026, Apache Software Foundation"
author = "Apache Software Foundation"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    # myst_nb is a superset of myst_parser: it provides the MyST markdown
    # parser plus executable `{code-cell}` notebook directives. Do NOT also
    # list "myst_parser" — myst_nb activates it internally and listing both
    # raises an extension conflict.
    "myst_nb",
    "autoapi.extension",
]

# NOTE: .rst stays alongside .md because sphinx-autoapi generates RST
# under autoapi/ and Sphinx needs the suffix to parse it. The human-
# authored docs are all MyST .md now. ".md" is routed through myst-nb so
# pages carrying jupytext/kernelspec front matter execute their
# `{code-cell}` blocks; pages without that front matter render as plain
# MyST markdown. The ".rst" entry is only for the autoapi build artifacts.
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "myst-nb",
}

# Execute notebook code cells at build time and fail the build if any cell
# raises — this replaces the old IPython sphinx directive, whose executed
# examples are now `{code-cell}` blocks. "force" re-executes every build so
# stale cached output can never ship.
nb_execution_mode = "force"
nb_execution_timeout = 120
nb_execution_raise_on_error = True

# Prefer the plain-text repr of a cell's last expression over its rich
# `_repr_html_`. A DataFrame's HTML repr is a self-contained widget (inline
# styles + an injected <script>) built for Jupyter; in the docs theme it
# renders at the wrong width. The text repr is the readable table the old
# IPython directive showed and is stable across datafusion versions.
nb_mime_priority_overrides = [("html", "text/plain", 0)]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

autoapi_dirs = ["../../python"]
autoapi_ignore = ["*tests*"]
autoapi_member_order = "groupwise"
suppress_warnings = ["autoapi.python_import_resolution"]
autoapi_python_class_content = "both"
autoapi_keep_files = False  # set to True for debugging generated files


def autoapi_skip_member_fn(app, what, name, obj, skip, options) -> bool:  # noqa: ARG001
    skip_contents = [
        # Re-exports
        ("class", "datafusion.DataFrame"),
        ("class", "datafusion.SessionContext"),
        ("module", "datafusion.common"),
        # Duplicate modules (skip module-level docs to avoid duplication)
        ("module", "datafusion.col"),
        ("module", "datafusion.udf"),
        # Deprecated
        ("class", "datafusion.substrait.serde"),
        ("class", "datafusion.substrait.plan"),
        ("class", "datafusion.substrait.producer"),
        ("class", "datafusion.substrait.consumer"),
        ("method", "datafusion.context.SessionContext.tables"),
        ("method", "datafusion.dataframe.DataFrame.unnest_column"),
    ]
    # Explicitly skip certain members listed above. These are either
    # re-exports, duplicate module-level documentation, deprecated
    # API surfaces, or private variables that would otherwise appear
    # in the generated docs and cause confusing duplication.
    # Keeping this explicit list avoids surprising entries in the
    # AutoAPI output and gives us a single place to opt-out items
    # when we intentionally hide them from the docs.
    if (what, name) in skip_contents:
        skip = True

    return skip


def setup(sphinx) -> None:
    sphinx.connect("autoapi-skip-member", autoapi_skip_member_fn)


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "pydata_sphinx_theme"

html_theme_options = {
    "use_edit_page_button": False,
    "show_toc_level": 2,
    "logo": {
        "image_light": "_static/images/original.svg",
        "image_dark": "_static/images/original_dark.svg",
        "alt_text": "Apache DataFusion in Python",
    },
    "navbar_start": ["navbar-logo"],
    "navbar_center": ["navbar-nav"],
    "navbar_end": ["navbar-icon-links", "theme-switcher"],
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/apache/datafusion-python",
            "icon": "fa-brands fa-github",
        },
        {
            "name": "Rust API docs (docs.rs)",
            "url": "https://docs.rs/datafusion/latest/datafusion/",
            "icon": "fa-brands fa-rust",
        },
    ],
    # Right-hand "On this page" TOC. A toggle button (added by
    # _static/toc-toggle.js) lets the reader hide the whole sidebar and give
    # the article full width.
    "secondary_sidebar_items": ["page-toc"],
    "collapse_navigation": True,
    "show_nav_level": 2,
}

html_context = {
    "github_user": "apache",
    "github_repo": "datafusion-python",
    "github_version": "main",
    "doc_path": "docs/source",
    "default_mode": "auto",
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

html_favicon = "_static/favicon.svg"

# Copy agent-facing files (llms.txt) verbatim to the site root so they
# resolve at conventional URLs like `https://.../python/llms.txt`.
html_extra_path = ["llms.txt"]

html_css_files = ["theme_overrides.css"]

# Adds a button that hides the right-hand "On this page" sidebar so the
# article can use the full width (see _static/toc-toggle.js).
html_js_files = ["toc-toggle.js"]

html_sidebars = {
    "**": ["sidebar-globaltoc.html"],
}

# tell myst_parser to auto-generate anchor links for headers h1, h2, h3
myst_heading_anchors = 3

# MyST extensions:
# - tasklist: GitHub-style `- [x]` checkboxes
# - colon_fence: `:::{directive}` blocks (needed by execution-metrics.md
#   after the RST -> MyST conversion)
# - deflist: definition lists (used in a couple of converted pages)
myst_enable_extensions = ["tasklist", "colon_fence", "deflist"]
