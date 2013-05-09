# StatsD OpenTSDB publisher backend

## Overview
This is a pluggable backend for [StatsD](https://github.com/etsy/statsd), which
publishes stats to OpenTSDB (http://opentsdb.net)

## Installation

    npm install statsd-opentsdb-backend

## Configuration
You have to give basic information about your OpenTSDB server to use
```
{ opentsdbHost: 'localhost'
, opentsdbPort: 4242
}
```

## Dependencies
- none

## Development
- [Bugs](https://github.com/emurphy/statsd-opentsdb-backend/issues)

## Issues
Version 0.1.0 is a minimum viable product that puts metrics to OpenTSDB with one tag (source=statsd).

Beware that statsd does not support tags. There is an impedance mismatch, in that graphite groups and aggregates via namespaces within the metric name. For example, ```foo.thingies.server1..n``` is aggregated via ```foo.thingies.*```. In OpenTSDB that would be ```foo.thingies``` metric with tag ```host=server1```. There is frankly no elegant way in statsd to derive the tag. A possible work-around would be by metric name convention, e.g. ```foo.thingies.__tags.host=server1.question=IsThisaHack```. Others are diverging towards statsd alternatives, for example [dd-agent](https://github.com/DataDog/dd-agent).

If you want to contribute:

1. Clone your fork
2. Hack away
3. If you are adding new functionality, document it in the README
4. Push the branch up to GitHub
5. Send a pull request
