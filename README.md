# StatsD OpenTSDB publisher backend

## Overview
This is a pluggable backend for [StatsD](https://github.com/etsy/statsd), which
publishes stats to OpenTSDB (http://opentsdb.net)

## Installation

    npm install statsd-opentsdb-backend

## Configuration
You have to give basic information about your OpenTSDB server to use
```
{ opentsdbhost: 'localhost'
, opentsdbPort: 4242
}
```

## Dependencies
- none

## Development
- [Bugs](https://github.com/emurphy/statsd-opentsdb-backend/issues)

## Issues
Version 0.1.0 is a minimum viable product that puts metrics to OpenTSDB with one tag (source=statsd).


If you want to contribute:

1. Clone your fork
2. Hack away
3. If you are adding new functionality, document it in the README
4. Push the branch up to GitHub
5. Send a pull request
