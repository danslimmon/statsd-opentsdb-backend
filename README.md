# StatsD OpenTSDB publisher backend

## Overview
This is a pluggable backend for [StatsD](https://github.com/etsy/statsd), which
publishes stats to OpenTSDB (http://opentsdb.net)

## Installation

    npm install statsd-opentsdb-backend

## Configuration
You have to give basic information about your OpenTSDB server to use
```
{ 
    opentsdbHost: 'localhost'
  , opentsdbPort: 4242
  , opentsdbTagPrefix: '_t_'
  , backends: ['statsd-opentsdb-backend']
}
```

Further customization, just like you would graphite (otherwise it just uses statsd defaults):
```
{
  opentsdb: {
      legacyNamespace: false
    , globalPrefix: 'foo'
    , prefixCounter: 'fooCounter'
    , prefixTimer: 'fooTimer'
    , prefixGauge: 'fooGauge'
    , prefixSet: 'fooSet'
  }
}
```

## Tag support
This backend allows you to attach OpenTSDB tags to your metrics. To add a counter
called `gorets` and tag the data `foo=bar`, you'd write the following to statsd:

    gorets._t_foo.bar:261|c

## Dependencies
- none

## Development
- [Bugs](https://github.com/emurphy/statsd-opentsdb-backend/issues)

## Issues
If you want to contribute:

1. Clone your fork
2. Hack away
3. If you are adding new functionality, document it in the README
4. Push the branch up to GitHub
5. Send a pull request
