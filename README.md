# URL Sort

Local AI-powered URL and bookmark organization pipeline.

## Overview

`url_sort` is an automated pipeline for collecting, validating, enriching, classifying, and organizing saved URLs.

The application accepts browser bookmarks, `.url` shortcuts, loose text files containing URLs, and Pushbullet exports. URLs are validated over the network, page metadata is collected, and a locally hosted Ollama model classifies each URL into a predefined category.

The result is a structured, searchable URL library with separate handling for dead and unknown links.

## Architecture

```text
Bookmarks / URL Files / Pushbullet
              │
              ▼
       Import & Normalize
              │
              ▼
        Extract URL
              │
              ▼
       HTTP Validation
              │
       ┌──────┴──────┐
       ▼             ▼
    Dead Link      Live Link
       │             │
       ▼             ▼
  dead_link      Metadata
                     │
                     ▼
              Ollama Classification
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
       Category              unknown
```

## Features

* Browser bookmark import
* `.url` shortcut processing
* Loose-text URL detection
* Bookmark JSON import
* Pushbullet export processing
* Pushbullet API synchronization
* HTTP link validation
* Dead-link detection
* Page title extraction
* Page snippet extraction
* Local Ollama classification
* Structured JSON classification responses
* Few-shot classification examples
* Concurrent network fetching
* Controlled LLM concurrency
* Progress and ETA reporting
* Dead-link audit CSV generation
* Import/archive management
* Unknown-category fallback

## Supported Inputs

The pipeline can process:

* Windows `.url` shortcut files
* Browser bookmark JSON exports
* Loose text files containing URLs
* Title + URL text files
* Pushbullet data exports
* Incremental Pushbullet API data

## Bookmark Processing

Bookmark JSON exports can be placed into the configured import directory.

The application converts bookmark entries into normalized `.url` files so they can pass through the same processing pipeline as other URLs.

Processed source files are archived to prevent repeated imports.

## Pushbullet Integration

The application supports two Pushbullet workflows.

### Historical Import

A Pushbullet data export can be imported and converted into normalized URL records.

### Incremental Synchronization

The Pushbullet API can be used to bring newer link pushes into the pipeline without repeatedly importing the complete historical export.

## URL Validation

Live URLs are fetched before classification.

The fetch stage is intentionally separated from the classification stage because they have different performance characteristics:

* HTTP fetching is network I/O-bound.
* Ollama inference is compute-bound.

This allows the application to use a larger worker pool for network operations while keeping LLM concurrency controlled.

## AI Classification

The application provides the local model with:

* Bookmark title
* URL
* Domain
* Page snippet/context

The model selects exactly one category from the configured taxonomy.

Classification uses a structured JSON response:

```json
{
  "reasoning": "Short description of the page",
  "category": "category-name"
}
```

If the model response cannot be parsed or does not contain a valid category, the URL falls back to `unknown`.

## Configuration

Important configuration values include:

```python
SOURCE_DIR
DESTINATION_DIR
OLLAMA_MODEL
OLLAMA_HOST
REQUEST_TIMEOUT
FETCH_WORKERS
CLASSIFY_WORKERS
```

## Ollama

Start Ollama:

```bash
ollama serve
```

Then pull the configured model:

```bash
ollama pull llama3.2
```

The model can be changed in the configuration section.

## Performance

The pipeline intentionally uses separate worker pools.

```text
HTTP Fetching
    │
    └── many concurrent workers

Ollama Classification
    │
    └── smaller controlled worker pool
```

This prevents slow LLM inference from unnecessarily blocking network processing while avoiding excessive concurrent requests to the local model server.

## Output

URLs are organized into category directories.

Special directories are used for:

```text
dead_link/
unknown/
```

A CSV audit file is also produced for dead links.

## Archive Management

Imported source material is archived after successful conversion.

This prevents the same bookmark export or loose URL file from being processed repeatedly.

## Project Status

🚧 **Active development**

The project is designed as a personal information-management pipeline and is evolving around real-world bookmark and URL collections.

## License

No license has currently been specified for this repository.
