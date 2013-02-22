/*
 * Flush stats to OpenTSDB (http://opentsdb.net/).
 *
 * To enable this backend, include 'opentsdb' in the backends
 * configuration array:
 *
 *   backends: ['opentsdb']
 *
 * This backend supports the following config options:
 *
 *   opentsdbHost: Hostname of opentsdb server.
 *   opentsdbPort: Port to contact opentsdb server at.
 */

var net = require('net'),
   util = require('util');

var debug;
var flushInterval;
var opentsdbHost;
var opentsdbPort;

// prefix configuration
var globalPrefix;
var prefixPersecond;
var prefixCounter;
var prefixTimer;
var prefixGauge;
var prefixSet;

// set up namespaces
var legacyNamespace = true;
var globalNamespace  = [];
var counterNamespace = [];
var timerNamespace   = [];
var gaugesNamespace  = [];
var setsNamespace     = [];

var opentsdbStats = {};

var post_stats = function opentsdb_post_stats(statString) {
  var last_flush = opentsdbStats.last_flush || 0;
  var last_exception = opentsdbStats.last_exception || 0;
  if (opentsdbHost) {
    try {
      var opentsdb = net.createConnection(opentsdbPort, opentsdbHost);
      opentsdb.addListener('error', function(connectionException){
        if (debug) {
          util.log(connectionException);
        }
      });
      opentsdb.on('connect', function() {
        var ts = Math.round(new Date().getTime() / 1000);
        var namespace = globalNamespace.concat('statsd');
        statString += 'put ' + namespace.join(".") + '.opentsdbStats.last_exception ' + last_exception + ' ' + ts + "\n";
        statString += 'put ' + namespace.join(".") + '.opentsdbStats.last_flush ' + last_flush + ' ' + ts + "\n";
		if (debug) {
			util.log(statString)
		}
        this.write(statString);
        this.end();
        opentsdbStats.last_flush = Math.round(new Date().getTime() / 1000);
      });
    } catch(e){
      if (debug) {
        util.log(e);
      }
      opentsdbStats.last_exception = Math.round(new Date().getTime() / 1000);
    }
  }
}

var flush_stats = function opentsdb_flush(ts, metrics) {
  var suffix = " source=statsd\n";
  var starttime = Date.now();
  var statString = '';
  var numStats = 0;
  var key;
  var timer_data_key;
  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;
  var timer_data = metrics.timer_data;
  var statsd_metrics = metrics.statsd_metrics;

  for (key in counters) {
    var namespace = counterNamespace.concat(key);
    var value = counters[key];

    if (legacyNamespace === true) {
      statString += 'put stats_counts.' + key + ' ' + ts + ' ' + value          + suffix;
    } else {
      statString += 'put ' + namespace.concat('count').join(".") + ' ' + ts + ' ' + value          + suffix;
    }

    numStats += 1;
  }

  for (key in timer_data) {
    if (Object.keys(timer_data).length > 0) {
      for (timer_data_key in timer_data[key]) {
        var namespace = timerNamespace.concat(key);
        var the_key = namespace.join(".");
        statString += 'put ' + the_key + '.' + timer_data_key + ' ' + ts + ' ' + timer_data[key][timer_data_key] + suffix;
      }

      numStats += 1;
    }
  }

  for (key in gauges) {
    var namespace = gaugesNamespace.concat(key);
    statString += 'put ' + namespace.join(".") + ' ' + ts + ' ' + gauges[key] + suffix;
    numStats += 1;
  }

  for (key in sets) {
    var namespace = setsNamespace.concat(key);
    statString += 'put ' + namespace.join(".") + '.count ' + ts + ' ' + sets[key].values().length + suffix;
    numStats += 1;
  }

  var namespace = globalNamespace.concat('statsd');
  if (legacyNamespace === true) {
    statString += 'put statsd.numStats ' + ts + ' ' + numStats + suffix;
    statString += 'put stats.statsd.opentsdbStats.calculationtime ' + ts + ' ' + (Date.now() - starttime) + suffix;
    for (key in statsd_metrics) {
      statString += 'put stats.statsd.' + key + ' ' + ts + ' ' + statsd_metrics[key] + suffix;
    }
  } else {
    statString += 'put ' + namespace.join(".") + '.numStats ' + ts + ' ' + numStats + suffix;
    statString += 'put ' + namespace.join(".") + '.opentsdbStats.calculationtime ' + ts + ' ' + (Date.now() - starttime) + suffix;
    for (key in statsd_metrics) {
      var the_key = namespace.concat(key);
      statString += 'put ' + the_key.join(".") + ' ' + ts + ' ' + statsd_metrics[key] + suffix;
    }
  }

  post_stats(statString);
};

var backend_status = function opentsdb_status(writeCb) {
  for (stat in opentsdbStats) {
    writeCb(null, 'opentsdb', stat, opentsdbStats[stat]);
  }
};

exports.init = function opentsdb_init(startup_time, config, events) {
  debug = config.debug;
  opentsdbHost = config.opentsdbHost;
  opentsdbPort = config.opentsdbPort;
  config.opentsdb = config.opentsdb || {};
  globalPrefix    = config.opentsdb.globalPrefix;
  prefixCounter   = config.opentsdb.prefixCounter;
  prefixTimer     = config.opentsdb.prefixTimer;
  prefixGauge     = config.opentsdb.prefixGauge;
  prefixSet       = config.opentsdb.prefixSet;
  legacyNamespace = config.opentsdb.legacyNamespace;

  // set defaults for prefixes
  globalPrefix  = globalPrefix !== undefined ? globalPrefix : "stats";
  prefixCounter = prefixCounter !== undefined ? prefixCounter : "counters";
  prefixTimer   = prefixTimer !== undefined ? prefixTimer : "timers";
  prefixGauge   = prefixGauge !== undefined ? prefixGauge : "gauges";
  prefixSet     = prefixSet !== undefined ? prefixSet : "sets";
  legacyNamespace = legacyNamespace !== undefined ? legacyNamespace : true;


  if (legacyNamespace === false) {
    if (globalPrefix !== "") {
      globalNamespace.push(globalPrefix);
      counterNamespace.push(globalPrefix);
      timerNamespace.push(globalPrefix);
      gaugesNamespace.push(globalPrefix);
      setsNamespace.push(globalPrefix);
    }

    if (prefixCounter !== "") {
      counterNamespace.push(prefixCounter);
    }
    if (prefixTimer !== "") {
      timerNamespace.push(prefixTimer);
    }
    if (prefixGauge !== "") {
      gaugesNamespace.push(prefixGauge);
    }
    if (prefixSet !== "") {
      setsNamespace.push(prefixSet);
    }
  } else {
      globalNamespace = ['stats'];
      counterNamespace = ['stats'];
      timerNamespace = ['stats', 'timers'];
      gaugesNamespace = ['stats', 'gauges'];
      setsNamespace = ['stats', 'sets'];
  }

  opentsdbStats.last_flush = startup_time;
  opentsdbStats.last_exception = startup_time;

  flushInterval = config.flushInterval;

  events.on('flush', flush_stats);
  events.on('status', backend_status);

  return true;
};
