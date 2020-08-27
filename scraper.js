'use strict';

var fs = require('fs');
var util = require('util');
var zlib = require('zlib');
var assert = require('assert');
var crypto = require('crypto');

var _ = require('lodash');
var URI = require('urijs');
var YAML = require('js-yaml');
var cheerio = require('cheerio');
var sqlite3 = require('sqlite3').verbose();
var Promise = require('bluebird');
var sortobject = require('deep-sort-object');
const { URLSearchParams } = require('url');
var fetch = require('node-fetch');
fetch.Promise = Promise;
const cookie = require('cookies.txt');

var gcHacks = require('gc-hacks');

var baseUrl = 'https://github.com';
var prevUrl = '';
let cookies = {};

//If you work with thousands of files on GitHub it high probability
//that some of the files are deleted in the process, so it pretty
//normal that a couple of HEAD or GET operations fail. Just log these
//errors without interrupting the entire process and show at the end.
var skippedErrors = [];

cookie.parse('./cookies.txt',function(cookieObj) {
  cookies = cookieObj;
  scrapeSpecs()
  .spread(function (specs, hashes) {
    specs = _.uniqBy(specs, function (spec) {
      return _(spec).pick(['user', 'repo', 'path']).values().join('/');
    });

    updateDB(specs, hashes);

    console.error('Skipped errors:');
    _.each(skippedErrors, function (err) {
      console.error(err);
    });

    console.log('Spec numbers without duplications:');
    _(specs).uniqBy('hash').groupBy('format').mapValues(_.size)
      .each(function (numSpecs, format) {
        console.log(format + ': ' + numSpecs);
      });
  });
});

function updateDB(specs, hashes) {
  var db = new sqlite3.Database('data.sqlite');

  db.serialize(function() {
    var fields = [
      'format',
      'dataFormat',
      'user',
      'repo',
      'path',
      'lastIndexed',
      'indexedCommit',
      'size',
      'hash'
    ];

    var listFields = fields.join(' TEXT, ') + ' TEXT';
    db.run('CREATE TABLE IF NOT EXISTS specs (' + listFields
      + ', PRIMARY KEY(user, repo, path))');

    db.run('DELETE FROM specs');

    console.log('Save "specs" to DB');
    db.parallelize(function() {
      var listFields = '$' + fields.join(', $');
      var statement = db.prepare('INSERT INTO specs VALUES ('+ listFields + ')');
      _.each(specs, function (spec) {
        var row = _.mapKeys(spec, function (value, key) {
          return '$' + key;
        });

        statement.run(row);
      })
      statement.finalize();
    });

    var date = (new Date()).toISOString();

    db.run('CREATE TABLE IF NOT EXISTS hashes ' +
           '(hash TEXT, data BLOB, lastSeen TEXT, PRIMARY KEY(hash))');

    console.log('Save "hashes" to DB');
    db.parallelize(function() {
      var statement = db.prepare('REPLACE INTO hashes VALUES (?,?,?)');
      _.each(hashes, function (data, hash) {
        statement.run([
          hash,
          data,
          date
        ]);
      })
      statement.finalize();
    });

    db.run('CREATE TABLE IF NOT EXISTS specs_archive ' +
           '(date TEXT, specs BLOB, PRIMARY KEY(date))');

    console.log('Append to "specs_archive"');
    db.run('INSERT INTO specs_archive VALUES (?,?)', [
      date,
      zlib.gzipSync(JSON.stringify(specs))
    ]);
  });

  db.close();
}

function scrapeSpecs() {
  //raml in:path extension:raml
  //wadl in:path extension:wadl
  //apib in:path extension:apib
  //
  var queries = [
    'filename:swagger extension:yml -language:yaml',
    'filename:swagger extension:yaml -language:yaml',
    'filename:swagger extension:json -language:json',
    'filename:openapi extension:yml -language:yaml',
    'filename:openapi extension:yaml -language:yaml',
    'filename:openapi extension:json -language:json',
    'swaggerVersion AND info in:file language:YAML',
    'swaggerVersion AND info in:file language:JSON',
    'openapi AND paths in:file language:YAML',
    'openapi AND paths in:file language:JSON',
    'swagger AND paths in:file language:YAML',
    'swagger AND paths in:file language:JSON'
  ];

  return runQueries(queries,
    function (body, entry) {
      if (body === '') {
	console.warn('No body');
        return;
      }

      try {
        entry.spec = JSON.parse(body)
        entry.dataFormat = 'JSON';
      }
      catch (e) {
        try {
          entry.spec = YAML.safeLoad(body,{json:true});
          entry.dataFormat = 'YAML';
        }
        catch (e) {
          //throw Error('Cannot parse: ' + getSpecUrl(entry));
        }
      }

      if (!entry.spec)
        return;

      var spec = entry.spec;
      if (!_.isUndefined(spec.openapi))
        entry.format = 'openapi_3';
      else if (!_.isUndefined(spec.swagger))
        entry.format = 'swagger_2';
      else if (!_.isUndefined(spec.swaggerVersion) && !_.isUndefined(spec.info))
        entry.format = 'swagger_1';
      else
        return;

      return entry;
    });
}

function fetchit(url, options) {
  return new Promise(function(resolve, reject){
    options = Object.assign({ headers: new fetch.Headers() }, options);
    console.log((options.method || 'GET') + ' ' +url);
    if (Object.keys(cookies).length) {
      options.headers.set('cookie',cookie.getCookieString(url));
    }
    if (prevUrl) {
      options.headers.set('referer', prevUrl);
    }
    options.headers.set('user-agent','Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0');
    options.headers.set('accept-language','en-GB,en;q=0.5');
    options.headers.set('accept','text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8');
    //options.headers.set('accept','application/vnd.github.v3.text-match+json');
    options.headers.set('Accept-Encoding','gzip, deflate, br');
    fetch(url, options)
    .then(async function(res){
      if (res.status !== 200) console.warn(res.status);
      if (res.status === 429) {
        const naughty = parseInt(res.headers.get('retry-after'),10);
        if (naughty) {
          console.warn('Waiting for',naughty);
          await Promise.delay(naughty*1000);
          return fetchit(url, options);
        }
      }
      prevUrl = url;
      if (res.headers.get('set-cookie')) {
        const newCookies = res.headers.get('set-cookie');
        //console.log('New cookies in jar:', newCookies.split(', ').length);
      }
      return res.text();
    })
    .then(function(text){
      return resolve(text);
    })
    .catch(function(ex) {
      return reject(ex);
    });
  });
}

function runQueries(queries, iter) {
  var hashes = {};
  var allEntries = [];
  return Promise.each(queries, function (query) {
    return codeSearch(query, function (entries) {
      return Promise.map(entries, function (entry) {
        fs.appendFileSync('./scraper.log',getSpecUrl(entry)+'\n','utf8');
        return fetchit(getSpecUrl(entry))
          .then(function (body) {
            entry.size = Buffer.byteLength(body);

            entry = iter(body, entry)
            if (!entry)
              return;

            var serializeSpec = JSON.stringify(sortobject(entry.spec));
            delete entry.spec;

            entry.hash = hash(serializeSpec);
            if (!hashes[entry.hash])
              hashes[entry.hash] = zlib.gzipSync(serializeSpec);

            allEntries.push(gcHacks.recreateValue(entry));
          })
          .catch(function (error) {
            console.error(error);
            skippedErrors.push(gcHacks.recreateValue(error.toString()));
          });
      }, {concurrency: 5});
    });
  }).return([allEntries, hashes]);
}

function hash(str) {
  return crypto.createHash('md5').update(str).digest('hex');
}

function getSpecUrl(entry) {
  return URI('https://raw.githubusercontent.com').segment([
    entry.user,
    entry.repo,
    entry.indexedCommit
  ].concat(entry.path.split('/'))).href();
}

function codeSearch(query, iter) {
  return runQueryImpl(query)
    .then(function (data) {
       if (!isIncomplete(data))
         return iterateAllResults(data, iter);
       else
         return codeSearchDivideBySize(query, iter);
    });
}

var sizeLimit = 384*1024; //Only files smaller than 384 KB are searchable.
function codeSearchDivideBySize(query, iter) {
  return Promise.coroutine(function* () {

    var begin = 0;
    var step = 1024;

    while (begin <= sizeLimit) {
      var filter = sizeFilter(begin, begin + step);
      var data = yield runQueryImpl(query + filter);

      if (isIncomplete(data)) {
        assert(step !== 1);
        step /= 2;
        continue;
      }

      begin += step+1;
      step *= 2;
      if (data.totalEntries === 0) {
        //try to fast-forward to last query, but keep step power of 2
        while (begin + step < sizeLimit)
          step *= 2;
      }

      yield iterateAllResults(data, iter);
    }
  })();
}

function sizeFilter(begin, end) {
  var sizeFilter = ' size:';
  if (end < sizeLimit)
    sizeFilter += begin + '..' + end;
  else
    sizeFilter += '>=' + begin;
  return sizeFilter;
}

function isIncomplete(data) {
  var searchLimit = 1000; //Github limit on results per query
  return data ? data.totalEntries > searchLimit || data.incomplete : true;
}

function runQueryImpl(query) {
  query = query.replace(/ /g, '+');
  var url = baseUrl + '/search?q=' + query + '&fork=false&type=Code';
  return codeSearchImpl(url, 1);
}

function iterateAllResults(data, iter) {
  return Promise.coroutine(function* () {
    while (true) {
      if (!_.isEmpty(data.entries))
        yield iter(data.entries);

      if (!data.next)
        break;
      data = yield codeSearchImpl(baseUrl + data.next, 1);
    }
  })();
}

function dumpHtml(html) {
  fs.writeFileSync('./invalid.html',html,'utf8');
}

var timeOfLastCall = Date.now()
function codeSearchImpl(url, counter) {
  //Github allow only 10 calls per minute without login
  //and 30 calls per minute after you login
  var delay = 2000 - (Date.now() - timeOfLastCall);
  return Promise.delay(delay >= 0 ? delay : 0)
    .then(function () {
      return fetchit(url)
    })
    .then(function (value) {
      timeOfLastCall = Date.now();
      return value;
    })
    .then(gcHacks.recreateReturnObjectAndGcCollect(function (html) {
      return parseGitHubPage(html);
    }))
    .catch(function(ex){
      if (counter < 10) return codeSearchImpl(url,counter++);
    });
}

function parseGitHubPage(html) {
  var $ = cheerio.load(html);
  if ($('.codesearch-results').length === 0) {
    var container = $('.container-lg');
    if (container.length !== 0) {
      dumpHtml(html);
      throw Error('Github return error: ' + container.text().trim());
    }
    //throw Error('Invalid HTML');
    return {};
  }

  if ($('.blankslate').length > 0)
    return {incomplete: false, totalEntries: 0}; // We couldn’t find any code

  var list = {};

  list.entries = Array.from($('.code-list-item').map(function () {
    //var fileLink = $('.title a:nth-child(2)', this).attr('href');
    var fileLink = $('a:nth-child(2)', this).attr('href');
    if (!fileLink) {
      return undefined;
    }

    var match = fileLink.match(/^\/([^/]+)\/([^/]+)\/blob\/([^/]+)\/(.+)$/);
    if (!match)
      throw Error('Invalid file link: ' + fileLink);

    return {
      user: match[1],
      repo: match[2],
      indexedCommit: match[3],
      path: match[4],
      lastIndexed: $('time', this).attr('datetime') || ''
    };
  }));

  var sortBar = $('.sort-bar h3');
  if (sortBar.length === 0) {
    //One page result
    list.incomplete = false;
    list.totalEntries = list.entries.length;
  }
  else {
    if ($('.octicon-question', sortBar).length > 0)
      list.incomplete = true;//Some results may not be shown

    var text = sortBar.text().match(/ ([0-9,]+) /)[1];
    list.totalEntries = parseInt(text.replace(/,/g, ''));
  }

  var next = $('.next_page')
  if(next.length > 0)
    list.next = next.attr('href');

  return list;
}
