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
var opentsdbTagPrefix;

// prefix configuration
var globalPrefix;
var prefixPersecond;
var prefixCounter;
var prefixTimer;
var prefixGauge;
var prefixSet;

// suffix appended to all metric writes
var suffix;

// restricted set of timer stats (mean, median, count, lower, upper, std)
var minimalTimerStats;
// only emit the upper value for percentiles
var onlyUpperForPercentile;

var includedPrefixes = [];
var excludedPrefixes = [];

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
        statString += 'put ' + namespace.join(".") + '.opentsdbStats.last_exception ' + ts + ' ' + last_exception + suffix;
        statString += 'put ' + namespace.join(".") + '.opentsdbStats.last_flush ' + ts + ' ' + last_flush + suffix;
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

// Returns a list of "tagname=tagvalue" strings from the given metric name.
function parse_tags(metric_name) {
  var parts = metric_name.split(".");
  var tags = [];
  var current_tag_name = "";
  for (i in parts) {
    var p = parts[i]
    if (p.indexOf(opentsdbTagPrefix) == 0) {
      var tag_name = p.split(opentsdbTagPrefix)[1];
      current_tag_name = tag_name
    } else if (current_tag_name != "") {
      tags.push(current_tag_name + "=" + p);
      current_tag_name = "";
    }
  }

  return tags;
}

// Strips out all tag information from the given metric name
function strip_tags(metric_name) {
  var parts = metric_name.split(".");
  var rslt_parts = [];
  while (parts.length > 0) {
    if (parts[0].indexOf(opentsdbTagPrefix) == 0) {
      parts.shift();
      parts.shift();
      continue;
    }
    rslt_parts.push(parts.shift());
  }

  return rslt_parts.join(".");
}

function output_metric(metric_name) {
  if (includedPrefixes.length > 0) {
    var keep = false;
    for (var i=0; i<includedPrefixes.length; i++) {
      if (metric_name.indexOf(includedPrefixes)==0) {
        keep=true;
        break;
      }
    }
    if (!keep) {
      return false;
    }
  }
  if (excludedPrefixes.length > 0) {
    var keep = true;
    for (var i=0; i<excludedPrefixes.length; i++) {
      if (metric_name.indexOf(excludedPrefixes)==0) {
        keep=false;
        break;
      }
    }
    return keep;
  }
  return true;
}


var flush_stats = function opentsdb_flush(ts, metrics) {
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
    var tags = parse_tags(key);
    var stripped_key = strip_tags(key)

    if (output_metric(stripped_key)) {

      var namespace = counterNamespace.concat(stripped_key);
      var value = counters[key];

	  // Max value TSDB accepts is Long.MAX_VALUE
      if (value > 9223372036854775807) {
        util.log("Trying to write too high a value for counter "+key+" "+tags.join(' ')+": "+value);
      }
	  // Min value TSDB accepts is Long.MIN_VALUE
      else if (value < -9223372036854775808) {
        util.log("Trying to write too low a value for counter "+key+" "+tags.join(' ')+": "+value);
      }
      else {
        if (legacyNamespace === true) {
          statString += 'put stats_counts.' + stripped_key + ' ' + ts + ' ' + value + ' ' + tags.join(' ') + suffix;
        } else {
          statString += 'put ' + namespace.concat('count').join(".") + ' ' + ts + ' ' + value + ' ' + tags.join(' ') + suffix;
        }
        numStats += 1;
      }
    }
  }

  for (key in timer_data) {
    if (Object.keys(timer_data).length > 0) {
      var stripped_key = strip_tags(key);
      if (output_metric(stripped_key)) {
        for (timer_data_key in timer_data[key]) {

          var write = true;
          if (minimalTimerStats) {
            write = false;
            if (timer_data_key.indexOf("mean") != -1) {
              write = true;
            }
            else if (timer_data_key.indexOf("median") != -1) {
              write = true;
            }
            else if (timer_data_key.indexOf("count") != -1) {
              write = true;
            }
            else if (timer_data_key.indexOf("lower") != -1) {
              write = true;
            }
            else if (timer_data_key.indexOf("upper") != -1) {
              write = true;
            }
            else if (timer_data_key.indexOf("std") != -1) {
              write = true;
            }
	      }
          if (onlyUpperForPercentile) {
            if (timer_data_key.indexOf("count_") != -1) {
              write = false;
            }
            if (timer_data_key.indexOf("mean_") != -1) {
              write = false;
            }
            if (timer_data_key.indexOf("sum_") != -1) {
              write = false;
            }
          }
          if (write) {
            var tags = parse_tags(key);

            var namespace = timerNamespace.concat(stripped_key);
            var the_key = namespace.join(".");
            // Max value TSDB accepts is Long.MAX_VALUE
            if (timer_data[key][timer_data_key] > 9223372036854775807) {
              util.log("Trying to write too big a value for timer "+the_key + '.' + timer_data_key+" "+tags.join(' ')+": "+timer_data[key][timer_data_key]);
            }
            else {
              statString += 'put ' + the_key + '.' + timer_data_key + ' ' + ts + ' ' + timer_data[key][timer_data_key] + ' ' + tags.join(' ') + suffix;
            }
          }
        }
        // increment regardless as expect at least one of the measures (e.g. count) to be valid
        numStats += 1;
      }
    }
  }

  for (key in gauges) {
    var tags = parse_tags(key);
    var stripped_key = strip_tags(key);
    if (output_metric(stripped_key)) {

      var namespace = gaugesNamespace.concat(stripped_key);
      
      // Max value TSDB accepts is Long.MAX_VALUE
      if (gauges[key] > 9223372036854775807) {
        util.log("Trying to write too big a value for gauge "+namespace.join(".")+" "+tags.join(' ')+": "+gauges[key]);
      }
      // Min value TSDB accepts is Long.MIN_VALUE
      else if (gauges[key] < -9223372036854775808) {
        util.log("Trying to write too low a value for gauge "+namespace.join(".")+" "+tags.join(' ')+": "+gauges[key]);
      }
      else {
        statString += 'put ' + namespace.join(".") + ' ' + ts + ' ' + gauges[key] + ' ' + tags.join(' ') + suffix;
        numStats += 1;
      }
    }
  }

  for (key in sets) {
    var tags = parse_tags(key);
    var stripped_key = strip_tags(key);
    if (output_metric(stripped_key)) {
    
      var namespace = setsNamespace.concat(stripped_key);
      // Max value TSDB accepts is Long.MAX_VALUE
      if (sets[key].values().length > 9223372036854775807) {
        util.log("Trying to write too big a value for set "+namespace.join(".")+" "+tags.join(' ')+": "+gauges[key]);
      }
      else {
        statString += 'put ' + namespace.join(".") + '.count ' + ts + ' ' + sets[key].values().length + ' ' + tags.join(' ') + suffix;
        numStats += 1;
      }
    }
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
  suffix = config.instance_name ? " statsd_instance=" + config.instance_name + "\n" : " source=statsd\n";
  debug = config.debug;
  opentsdbHost = config.opentsdbHost;
  opentsdbPort = config.opentsdbPort;
  opentsdbTagPrefix = config.opentsdbTagPrefix
  config.opentsdb = config.opentsdb || {};
  globalPrefix    = config.opentsdb.globalPrefix;
  prefixCounter   = config.opentsdb.prefixCounter;
  prefixTimer     = config.opentsdb.prefixTimer;
  prefixGauge     = config.opentsdb.prefixGauge;
  prefixSet       = config.opentsdb.prefixSet;
  legacyNamespace = config.opentsdb.legacyNamespace;
  minimalTimerStats = config.opentsdb.minimalTimerStats || false;
  onlyUpperForPercentile = config.opentsdb.onlyUpperForPercentile || false;
  includedPrefixes = config.opentsdb.includedPrefixes || [];
  excludedPrefixes = config.opentsdb.excludedPrefixes || [];

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
