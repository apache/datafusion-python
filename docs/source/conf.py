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

project = "Apache Arrow DataFusion"
copyright = "2019-2024, Apache Software Foundation"
author = "Apache Software Foundation"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    "myst_parser",
    "IPython.sphinxext.ipython_directive",
    "autoapi.extension",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

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

html_theme_options = {"use_edit_page_button": False, "show_toc_level": 2}

html_context = {
    "github_user": "apache",
    "github_repo": "arrow-datafusion-python",
    "github_version": "main",
    "doc_path": "docs/source",
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

html_logo = "_static/images/2x_bgwhite_original.png"

html_css_files = ["theme_overrides.css"]

html_sidebars = {
    "**": ["docs-sidebar.html"],
}

# tell myst_parser to auto-generate anchor links for headers h1, h2, h3
myst_heading_anchors = 3

# enable nice rendering of checkboxes for the task lists
myst_enable_extensions = ["tasklist"]
