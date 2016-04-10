'use strict';

var fs = require('fs');
var assert = require('assert');

var _ = require('lodash');
var URI = require('urijs');
var YAML = require('js-yaml');
var cheerio = require('cheerio');
var sqlite3 = require('sqlite3').verbose();
var Promise = require('bluebird');

var gcHacks = require('gc-hacks');
var makeRequest = require('makeRequest');

var baseUrl = 'https://github.com';
var parallel_limit = 20;

var searchLimit = 1000; //Github limit on results per query
var _100MB = 100*1024*1024; //GitHub limit on filesize

//If you work with thousands of files on GitHub it high probability
//that some of the files are deleted in the process, so it pretty
//normal than couple HEAD or  GET operations fail. Just log this
//errors with interrupting entire process and show in the end.
var skippedErrors = [];

var db = new sqlite3.Database('data.sqlite');
var fields = [
  'format',
  'user',
  'repo',
  'path',
  'lastIndexed',
  'indexedCommit',
  'size',
  'rawUrl',
  'baseUrls',
  'hash'
];

initDatabase(function () {
  login(process.env.MORPH_GITHUB_USER, process.env.MORPH_GITHUB_PASSWORD)
    .then(function () {
      return scrapeSpecs()
    })
    .then(function (formats) {
      console.error('Skipped errors:');
      _.each(skippedErrors, function (error) {
        console.error(error);
      });

      console.log('Spec numbers without duplications:');
      console.log(JSON.stringify(_.mapValues(formats, _.size), null, 2));
      updateTable(formats);
      db.close();
    })
    .done();
});

function updateTable(formats) {
  _.each(formats, function (hashes, format) {
    _.each(hashes, function (specs, hash) {
      _.each(specs, function (spec) {
        updateRow(_.extend(spec, {
          format: format,
          hash: hash,
          rawUrl: getSpecUrl(spec)
        }));
      });
    });
  });
}

function initDatabase(callback) {
  // Set up sqlite database.
  db.serialize(function() {
    var listFields = fields.join(' TEXT, ') + ' TEXT';
    db.run('CREATE TABLE IF NOT EXISTS specs (' + listFields
      + ', PRIMARY KEY(user, repo, path))');
    callback();
  });
}

function updateRow(row) {
  row = _.mapKeys(row, function (value, key) {
    return '$' + key;
  });

  var listFields = '$' + fields.join(', $');
  var statement = db.prepare('REPLACE INTO specs VALUES ('+ listFields + ')');
  statement.run(row);
  statement.finalize();
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
    'swaggerVersion AND info in:file language:YAML',
    'swaggerVersion AND info in:file language:JSON',
    'swagger AND paths in:file language:YAML',
    'swagger AND paths in:file language:JSON'
  ];

  var formats = {
    swagger_1: {},
    swagger_2: {},
  };

  return runQueries(queries,
    function (body, specs, hash) {
      var spec;
      try {
        spec = JSON.parse(body)
      }
      catch (e) {
        try {
          spec = YAML.safeLoad(body);
        }
        catch (e) {
          skippedErrors.push(Error('Can not parse: ' + getSpecUrl(specs[0])));
          return;
        }
      }

      if (!spec)
        return;

      var format = null;
      var baseUrls = [];

      if (!_.isUndefined(spec.swagger)) {
        format = 'swagger_2';
        if (spec.schemes && spec.host) {
          _.each(spec.schemes, function (schema) {
            if (schema === 'http' && spec.schemes.indexOf('https') === -1)
              return;
            baseUrls.push(schema + '://' + spec.host + (spec.basePath || '/'));
          });
        }
      }
      else if (!_.isUndefined(spec.swaggerVersion) && !_.isUndefined(spec.info)) {
        format = 'swagger_1';
      }

      if (!format)
        return;

      baseUrls = baseUrls.join(',');
      _.each(specs, function (spec) {
        spec.baseUrls = baseUrls;
      });

      formats[format][hash] = specs;
    }).return(formats);
}

function runQueries(queries, iter, callback) {
  var allEntries = [];
  return Promise.each(queries, function (query) {
    return codeSearchAll({query: query}, function (entry) {
      //FIXME: investigate
      //Array.prototype.push.bind(allEntries
      allEntries.push(entry);
    });
  }).then(function () {
    //remove duplicates
    allEntries = _.uniqBy(allEntries, getSpecUrl);
    return groupByHash(allEntries)
  }).then(function (hashes) {
    return forEachSpec(hashes, iter);
  });
}

function forEachSpec(hashes, iter) {
  return Promise.map(_.keys(hashes), function (hash) {
      var specs = hashes[hash];
      //If hash is the same it don't matter which one we take content is the same
      var url = getSpecUrl(specs[0]);
      return makeRequest('get', url)
        .spread(function (response, body) {
          var size = Buffer.byteLength(body);
          _.each(specs, function (spec) {
            spec.size = size;
          });

          iter(body, specs, hash);
        })
        .catch(function (error) {
          skippedErrors.push(error);
        });
    }, {concurrency: parallel_limit});
}

function groupByHash(entries, callback) {
  var hashes = {};
  return Promise.map(entries, function (spec, key) {
      var url = getSpecUrl(spec);
      return makeRequest('head', url)
        .spread(function (response, body) {
          var hash = response.headers['etag'];
          assert(hash, 'Missing hash: ' + url);
          hash = JSON.parse(hash);//remove quotations

          hashes[hash] = hashes[hash] || [];
          hashes[hash].push(spec);
        })
        .catch(function (error) {
          skippedErrors.push(error);
        });
    }, {concurrency: parallel_limit})
    .return(hashes);
}

function getSpecUrl(spec) {
  return URI('https://raw.githubusercontent.com').segment([
    spec.user,
    spec.repo,
    spec.indexedCommit
  ].concat(spec.path.split('/'))).href();
}

function codeSearchAll(options, iter) {
  return codeSearch(options).then(function (firstData) {
    if (!firstData.incomplete && firstData.totalEntries <= searchLimit)
      return _.each(firstData.entries, iter);

    return codeSearchInterval(0, 1024);

    function codeSearchInterval(begin, step) {
      if (begin > _100MB)
        return;

      var end = begin + step;
      return codeSearchLimitSize(options, begin, end).then(function (data) {
        if (data.totalEntries > searchLimit || data.incomplete) {
          assert(step !== 1);
          return codeSearchInterval(begin, step / 2);
        }

        _.each(data.entries, iter);
        begin += step+1;
        step *= 2;
        if (data.totalEntries === 0) {
          //try to fast-forward to last query, but keep step power of 2
          while (begin + step < _100MB)
            step *= 2;
        }
        return codeSearchInterval(begin, step);
      });
    }
  });
}

function codeSearchLimitSize(options, begin, end) {
  var sizeOptions = _.cloneDeep(options);
  if (end < _100MB)
    sizeOptions.query += ' size:"' + begin + '..' + end + '"';
  else
    sizeOptions.query += ' size:>=' + begin
  return codeSearch(sizeOptions);
}

function codeSearch(options) {
  var url =  baseUrl + '/search?';
  _.each(options, function (value, name) {
    assert(['query', 'language', 'order', 'filter'].indexOf(name) !== -1);
    url += name[0] + '=' + value + '&'
  });
  url += 'type=Code';
  url = url.replace(/ /g, '+');

  return codeSearchImpl(url).then(getRestOfEntries);
}

function getRestOfEntries(firstData) {
  if (firstData.totalEntries > searchLimit || firstData.incomplete) {
    //If search incomplete or can't be finish skip rest of the entries.
    firstData.entries = undefined;
    return firstData;
  }

  var allEntries = firstData.entries = [];
  return process(firstData).return(firstData);

  function process(data) {
    Array.prototype.push.apply(allEntries, data.entries);
    if (data.next)
      return codeSearchImpl(baseUrl + data.next).then(process);
    return Promise.resolve();
  }
}

function login(login, password, callback) {
  var loginUrl = baseUrl + '/login';
  return makeRequest('get', loginUrl)
    .spread(function (response, html) {
      var $ = cheerio.load(html);
      var form = $('#login form');
      var method = form.attr('method');
      var url = URI(form.attr('action')).absoluteTo(loginUrl).href();
      var formData = {}

      form.find('input').each(function () {
        formData[$(this).attr('name')] = $(this).attr('value');
      });

      formData.login = login;
      formData.password = password;

      return makeRequest('post',  url, {expectCode: 302, form: formData});
    });
}

function codeSearchImpl(url) {
  //Github allow only 10 calls per minute without login
  //and 30 calls per minute after you login
  return Promise.delay(2000)
    .then(function () {
      return makeRequest('get', url)
    })
    .spread(gcHacks.recreateReturnObjectAndGcCollect(function (response, html) {
      return parseGitHubPage(html);
    }));
}

function parseGitHubPage(html) {
  var $ = cheerio.load(html);
  if ($('.codesearch-results').length === 0) {
    var container = $('.container');
    if (container.length !== 0)
      throw Error('Github return error: ' + container.text().trim());
    throw Error('Invalid HTML');
  }

  if ($('.blankslate').length > 0)
    return {incomplete: false, totalEntries: 0}; // We couldn’t find any code

  var list = {};

  list.entries = Array.from($('.code-list-item').map(function () {
    var fileLink = $('.title a:nth-child(2)', this).attr('href');

    var match = fileLink.match(/^\/([^/]+)\/([^/]+)\/blob\/([^/]+)\/(.+)$/);
    if (!match)
      throw Error('Invalid file link: ' + fileLink);

    return {
      user: match[1],
      repo: match[2],
      indexedCommit: match[3],
      path: match[4],
      lastIndexed: $('time', this).attr('datetime')
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
