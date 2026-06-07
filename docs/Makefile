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

# Thin wrapper. The mkdocs.yml lives at the repo root; run `mkdocs build`
# from one directory up.

MKDOCS ?= mkdocs

.PHONY: help html serve clean

help:
	@echo "Targets:"
	@echo "  html   - build site to docs/build/html"
	@echo "  serve  - serve site at http://localhost:8000"
	@echo "  clean  - remove docs/build/"

html:
	cd .. && DISABLE_MKDOCS_2_WARNING=true $(MKDOCS) build --strict

serve:
	cd .. && DISABLE_MKDOCS_2_WARNING=true $(MKDOCS) serve

clean:
	rm -rf build/
