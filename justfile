# ============ Variables used in recipes ============

# Load environment variables from config.public.mk
set dotenv-load := true
set dotenv-filename := x'${LINKML_ENVIRONMENT_FILENAME:-config.public.mk}'

# Set shebang line for cross-platform Python recipes
shebang := if os() == 'windows' {
  'py'
} else {
  '/usr/bin/env python3'
}

# Environment variables with defaults
schema_name := env_var_or_default("LINKML_SCHEMA_NAME", "_no_schema_given_")
source_schema_dir := env_var_or_default("LINKML_SCHEMA_SOURCE_DIR", "")
config_yaml := if env_var_or_default("LINKML_GENERATORS_CONFIG_YAML", "") != "" {
  "--config-file " + env_var_or_default("LINKML_GENERATORS_CONFIG_YAML", "")
} else {
  ""
}
gen_doc_args := env_var_or_default("LINKML_GENERATORS_DOC_ARGS", "")
gen_owl_args := env_var_or_default("LINKML_GENERATORS_OWL_ARGS", "")
gen_pydantic_args := env_var_or_default("LINKML_GENERATORS_PYDANTIC_ARGS", "")

# Directory variables
src := "src"
dest := "project"
pymodel := src / schema_name / "datamodel"
source_schema_path := source_schema_dir / schema_name + ".yaml"
docdir := "docs/schema"
merged_schema_path := "docs/schema" / schema_name + ".yaml"

# ============== Project recipes ==============

# List all commands as default
_default: _status
    @just --list

# Initialize a new project
[group('project management')]
setup: _check-config install

# Install project dependencies
[group('project management')]
install:
  uv sync --group dev

# Clean all generated files
[group('project management')]
clean: _clean_project
  rm -rf tmp
  rm -rf {{docdir}}/*.md {{docdir}}/classes {{docdir}}/slots {{docdir}}/enums {{docdir}}/types

# (Re-)Generate project and documentation locally
[group('model development')]
site: gen-project gen-doc

# Run all tests
[group('model development')]
test:
  uv run python -m pytest

# Run linting
[group('model development')]
lint:
  uv run ruff check src/ tests/

# Generate md documentation for the schema
[group('model development')]
gen-doc: _gen-yaml
  uv run gen-doc --subfolder-type-separation {{gen_doc_args}} -d {{docdir}} {{source_schema_path}}

# Build docs and run test server
[group('model development')]
testdoc: gen-doc _serve

# Generate the Python data models (dataclasses & pydantic)
[group('model development')]
gen-python:
  uv run gen-project -d {{pymodel}} -I python {{source_schema_path}}
  uv run gen-pydantic {{gen_pydantic_args}} {{source_schema_path}} > {{pymodel}}/{{schema_name}}_pydantic.py

# Generate project files including Python data model
[group('model development')]
gen-project:
  uv run gen-project {{config_yaml}} -d {{dest}} {{source_schema_path}}
  mv {{dest}}/*.py {{pymodel}}
  uv run gen-pydantic {{gen_pydantic_args}} {{source_schema_path}} > {{pymodel}}/{{schema_name}}_pydantic.py

# ============== Hidden internal recipes ==============

_status: _check-config
  @echo "Project: {{schema_name}}"
  @echo "Source: {{source_schema_path}}"

_check-config:
    #!{{shebang}}
    import os
    schema_name = os.getenv('LINKML_SCHEMA_NAME')
    if not schema_name:
        print('**Project not configured**:\n - See config.public.mk')
        exit(1)
    print('Project-status: Ok')

_gen-yaml:
  -mkdir -p docs/schema
  uv run gen-yaml {{source_schema_path}} > {{merged_schema_path}}

_serve:
  uv run mkdocs serve

_clean_project:
    #!{{shebang}}
    import shutil, pathlib
    for d in pathlib.Path("{{dest}}").iterdir():
        if d.is_dir():
            print(f'removing "{d}"')
            shutil.rmtree(d, ignore_errors=True)
    for d in pathlib.Path("{{pymodel}}").iterdir():
        if d.name == "__init__.py":
            continue
        print(f'removing "{d}"')
        if d.is_dir():
            shutil.rmtree(d, ignore_errors=True)
        else:
            d.unlink()

# ============== Include project-specific recipes ==============

import "project.justfile"
