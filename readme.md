## Overview
pipe your log (e.g. nginx access log) from `tail -n 0 -F` to this tool, it will parse it and send the data to statsd

## Why not logstash
* logstash 1.4.1 can't just reload its config and launches for minutes
* logstash - 10% cpu at ~100rps in log, this tool - 1% (also 160M RES vs 7.5M) (aws c1 xlarge)
* some logic is inconvenient to express with logstash config DSL
* after a downtime logstash will process all records from the last remembered position (bad for e.g. incrementing rps counters in statsd)
* seems to cause some artificial spikes in the resulting data (http://s.fillest.ru/published/logstash_weird.png) - looks like another performance problem

## State
works well but yet contains some hardcoded in-house logic; no docs; not tested for thousands of rps

## License
[The MIT License](http://www.opensource.org/licenses/mit-license.php)