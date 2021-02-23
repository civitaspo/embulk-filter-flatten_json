# Flatten Json filter plugin for Embulk

## Overview

* **Plugin type**: filter

## Configuration

- **json_columns**: column name list to flatten json (string, required)
- **separator**: separator to join keys (string, default: `"."`)
- **array_index_prefix**: prefix of array index when joining keys (string, default: `null`)
  - if set `null` and **separator** option use the default value, the output become like [JSONPath](http://goessner.net/articles/JsonPath/)

## Usage

```yaml
filters:
  - type: flatten_json
    json_columns:
      - json_payload
    separator: "."
    array_index_prefix: "_"
```

filter like below.

#### before

```
c1|c2|c3|json_payload
1|civitaspo|5.5|{"id":5,"address":{"zip_code":"123-4567","city":"Tokyo"},"hobbies":["breakdance","motorbike"]}
2|mori.ogai|8.8|{"id":8,"address":{"zip_code":"891-0123","city":"Edo"},"hobbies":["novel"]}
3|natsume.soseki|9.9|{"id":9,"address":{"zip_code":"456-7891","city":"Edo"},"hobbies":["novel","reading books"]}
```

#### after

```
c1|c2|c3|json_payload
1|civitaspo|5.5|{"id":5,"address.zip_code":"123-4567","address.city":"Tokyo","hobbies._0":"breakdance","hobbies._1":"motorbike"}
2|mori.ogai|8.8|{"id":8,"address.zip_code":"891-0123","address.city":"Edo","hobbies._0":"novel"}
3|natsume.soseki|9.9|{"id":9,"address.zip_code":"456-7891","address.city":"Edo","hobbies._0":"novel","hobbies._1":"reading books"}
```

## Example

```bash
./gradlew gem
embulk run example/config.yml -Ibuild/gemContents/lib
```

## Build

```
$ ./gradlew gem
```
