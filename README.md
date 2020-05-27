# embulk-filter-unnest

Embulk plugin for unnesting JSON array.


## Overview

* **Plugin type**: filter

## Configuration

- **json_column_name**: a column name having JSON array to be unnested (json, required)


## Development

### Run Example

```
$ ./gradlew gem
$ embulk run -Ibuild/gemContents/lib ./example/config.yml
```

### Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
