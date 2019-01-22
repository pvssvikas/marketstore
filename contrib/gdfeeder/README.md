# GD Data Fetcher

This module builds a MarketStore background worker which parses given csv files
for nse market price data to create symbol database.  It runs as a goroutine
behind the MarketStore process and keeps writing to the disk.

## Configuration
gdfeeder.so comes with the server by default, so you can simply configure it
in MarketStore configuration file.

### Options
Name | Type | Default | Description
--- | --- | --- | ---
fetch_at | string | ex(hh:mm): 21:30 | it tries to fetch csv files everyday at configured time

#### Fetch AT
Fetch At parameter is used as sleep configuration, so that module can wakeup at configured time and parse the available csv files; once the file is processed, it is moved to folder processed.

This module expects csv and processed folders as sibling to mkts.yml config file.

### Example
Add the following to your config file:
```
bgworkers:
  - module: gdfeeder.so
    config:
      fetch_at: "21:30"
```


## Build
If you need to change the fetcher, you can build it by:

```
$ make configure
$ make all
```

It installs the new .so file to the first GOPATH/bin directory.


## Caveat
Since this is implemented based on the Go's plugin mechanism, it is supported only
on Linux & MacOS as of Go 1.10