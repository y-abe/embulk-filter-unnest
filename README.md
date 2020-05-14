# Unnest filter plugin for Embulk

Unnest json array column.

## Overview

* **Plugin type**: filter

## Configuration

- **json_column_name**: a column name having json array to be unnested. (string, required)

## Example

```yaml
filters:
  - type: unnest
    json_column_name: hoge
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
