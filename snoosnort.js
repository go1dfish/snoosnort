var RSVP = require('rsvp'), _ = require('underscore');
var events = require('events');

function idRange(earliestId, latestId, max) {
  if (!latestId || (!earliestId && !max)) {return [];}
  var end = parseInt(latestId+'',36), start = parseInt(earliestId+'',36);
  if (max) {start = end - max;} return _.range(start, end)
    .map(function(j) {return j.toString(36);}).sort().reverse().slice(0,max).sort();
}

module.exports = function(reddit, types, schedule) {
  var known = {}; var emitter = new events.EventEmitter();
  emitter.setMaxListeners(100);
  function ingestType(type, poll, depth, keys) {
    knownOfType = known[type] = known[type] || {};
    function ingestItem(item) {
      if (!item.id) {return;} if (knownOfType[item.id]) {return item;}
      knownOfType[item.id] = true;
      emitter.emit(type, item, types[type]);
    }
    var tasks = [schedule.repeat(function() {
      var sorted = _.keys(knownOfType).sort();
      var existing = idRange(_.first(sorted), _.last(sorted), depth).reverse();
      var missing = existing.filter(function(id) {return !knownOfType[id];});
      var names = missing.slice(0, 100).map(function(j) {return type+'_'+j;});
      if (!names.length) {return RSVP.resolve();}
      return byId(names).then(function(items) {items.forEach(ingestItem);
        names.filter(function(j) {return !knownOfType[j.split('_').pop()];})
          .forEach(function(name) {knownOfType[name.split('_').pop()] = 1;});
      });
    })];
    if (poll) {tasks.push(poll(ingestItem));}
    return RSVP.all(tasks);
  }

  function byId(names) {
    return reddit('/api/info').get({ids: names}).then(function(result) {
      return (((result||{}).data||{}).children||[]).map(function(j) {return j.data;});
    });
  }

  var ingests = {
    t1: function(opts) {
      return ingestType('t1', function(ingestItem) {
        return schedule.repeat(function() {
          return reddit('/r/all/comments/').get({limit:100}).then(function(results) {
            if (results && results[1] && results[1].data && results[1].data.children) {
              return results[1].data.children.map(function(j) {return j.data;});
            } else if (results.data && results.data.children) {
              return _.pluck(results.data.children, 'data');
            }
            return [];
          }).then(function(j) {j.forEach(ingestItem);});
        });
      }, opts.depth, ['link_id', 'author']);
    },
    t3: function(opts) {
      return ingestType('t3', function(ingestItem) {
        return schedule.repeat(function() {
          return reddit('/r/all/new').get({limit:100}).then(function(results) {
            if (results && results[1] && results[1].data && results[1].data.children) {
              return results[1].data.children.map(function(j) {return j.data;});
            } else if (results.data && results.data.children) {
              return _.pluck(results.data.children, 'data');
            }
            return [];
          }).then(function(j) {j.forEach(ingestItem);});
        }, 60000);
      }, opts.depth, ['url', 'is_self']);
    }
  };
  emitter.promise = RSVP.all(_.keys(types).map(function(key) {
    var task = ingests[key], opts = types[key] || {};
    if (!task) {task = opts.task;} return task(types[key]);
  })); emitter.known = known;
  return emitter;
};
