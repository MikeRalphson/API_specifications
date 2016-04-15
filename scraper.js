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
  'hash'
];

initDatabase(function () {
  login(process.env.MORPH_GITHUB_USER, process.env.MORPH_GITHUB_PASSWORD)
    .then(scrapeSpecs)
    .then(function (specs) {
      console.error('Skipped errors:');
      _.each(skippedErrors, function (err) {
        console.error(err);
      });

      console.log('Spec numbers without duplications:');
      _(specs).uniqBy('hash').groupBy('format').mapValues(_.size)
        .each(function (numSpecs, format) {
          console.log(format + ': ' + numSpecs);
        });

      _.each(specs, updateRow);
      db.close();
    })
    .done();
});

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

  return runQueries(queries,
    function (body, entry) {
      if (body === '')
        return;

      var spec;
      try {
        spec = JSON.parse(body)
      }
      catch (e) {
        try {
          spec = YAML.safeLoad(body);
        }
        catch (e) {
          throw Error('Can not parse: ' + entry.rawUrl);
        }
      }

      if (!spec)
        return;

      if (!_.isUndefined(spec.swagger))
        entry.format = 'swagger_2';
      else if (!_.isUndefined(spec.swaggerVersion) && !_.isUndefined(spec.info))
        entry.format = 'swagger_1';
      else
        return;

      return entry;
    });
}

function runQueries(queries, iter) {
  var allEntries = [];
  return Promise.each(queries, function (query) {
    return codeSearch(query, function (entries) {
      return Promise.map(entries, function (entry) {
        entry.rawUrl = getSpecUrl(entry);

        return makeRequest('get', entry.rawUrl)
          .spread(function (response, body) {
            var hash = response.headers['etag'];
            assert(hash, 'Missing hash: ' + entry.rawUrl);
            entry.hash = JSON.parse(hash);//remove quotations

            entry.size = Buffer.byteLength(body);

            entry = iter(body, entry)
            if (entry)
              allEntries.push(gcHacks.recreateValue(entry));
          })
          .catch(function (error) {
            console.error(error);
            skippedErrors.push(gcHacks.recreateValue(error.toString()));
          });
      }, {concurrency: 5});
    });
  }).return(allEntries);
}

function getSpecUrl(entry) {
  return URI('https://raw.githubusercontent.com').segment([
    entry.user,
    entry.repo,
    entry.indexedCommit
  ].concat(entry.path.split('/'))).href();
}

function makeSearchUrl(query, begin, end) {
  if (_.isNumber(begin) && _.isNumber(end))
    query += ' size:"' + begin + '..' + end + '"';
  query = query.replace(/ /g, '+');
  return baseUrl + '/search?q=' + query + '&type=Code';
}

function codeSearch(query, iter) {
  return codeSearchImpl(makeSearchUrl(query))
    .then(function (data) {
       if (!isIncomplete(data))
         return iterateAllResults(data, iter);
       else
         return codeSearchDivideBySize(query, iter);
    });
}

function codeSearchDivideBySize(query, iter) {
  return Promise.coroutine(function* () {
    var sizeLimit = 384*1024; //Only files smaller than 384 KB are searchable.

    var begin = 0;
    var step = 1024;

    while (begin <= sizeLimit) {
      var end = begin + step;
      if (end > sizeLimit)
        end = sizeLimit;

      var url = makeSearchUrl(query, begin, end);
      var data = yield codeSearchImpl(url);

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

function isIncomplete(data) {
  var searchLimit = 1000; //Github limit on results per query
  return data.totalEntries > searchLimit || data.incomplete;
}

function iterateAllResults(data, iter) {
  return Promise.coroutine(function* () {
    while (true) {
      if (!_.isEmpty(data.entries))
        yield iter(data.entries);

      if (!data.next)
        break;
      data = yield codeSearchImpl(baseUrl + data.next);
    }
  })();
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

var timeOfLastCall = Date.now()
function codeSearchImpl(url) {
  //Github allow only 10 calls per minute without login
  //and 30 calls per minute after you login
  var delay = 2000 - (Date.now() - timeOfLastCall);
  console.log(delay);
  return Promise.delay(delay >= 0 ? delay : 0)
    .then(function () {
      return makeRequest('get', url)
    })
    .then(function (value) {
      timeOfLastCall = Date.now();
      return value;
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
