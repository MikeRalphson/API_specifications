'use strict';

var fs = require('fs');
var assert = require('assert');
var URL = require('url');

var _ = require('lodash');
//require('request').debug = true;
var async = require('async');
var request = require('request').defaults({jar: true, gzip: true});
var cheerio = require('cheerio');

var baseUrl = 'https://github.com';

login(process.env.MORPH_GITHUB_USER, process.env.MORPH_GITHUB_PASSWORD, function (err) {
  assert(!err, err);

  //raml in:path extension:raml
  //wadl in:path extension:wadl
  //apib in:path extension:apib
  //
  var queries = [
    'filename:swagger extension:yml -language:yaml',
    'filename:swagger extension:yaml -language:yaml',
    'filename:swagger extension:json -language:json',
    'swaggerVersion AND apis in:file language:YAML',
    'swaggerVersion AND apis in:file language:JSON',
    'swagger AND paths in:file language:YAML',
    'swagger AND paths in:file language:JSON'
  ];

  var entries = async.reduce(queries, [],
    function (memo, query, asynCallback) {
      codeSearchAll({query: query}, function (err, entries) {
        if (err)
          return asynCallback(err);
        asynCallback(null, memo.concat(entries));
      });
  },
  function (error, allEntries) {
    assert(!error, error);
    console.log(_.size(allEntries));
    var str = JSON.stringify(allEntries, null, 2);
    fs.writeFileSync('scrape_result.json', str);
  });
});

function codeSearchAll(options, callback) {
  codeSearch(options, function (err, firstData) {
    if (err)
      return callback(err);

    if (firstData.totalEntries <= 1000)
      return getAllEntries(firstData, callback);

    assert(!firstData.incomplete, 'First call is incomplete');
    var allEntries = [];
    var begin = 0;
    var step = 1024;

    codeSearchInterval();

    function codeSearchInterval() {
      var sizeOptions = _.cloneDeep(options);
      var end = begin + step;
      var _100MB = 100*1024*1024; //GitHub limit on filesize
      if (end < _100MB)
        sizeOptions.query += ' size:"' + begin + '..' + end + '"';
      else
        sizeOptions.query += ' size:>=' + begin

      codeSearch(sizeOptions, function (err, data) {
        if (err)
          return callback(err);

        if (data.totalEntries > 1000 || data.incomplete) {
          assert(step !== 1);
          step /= 2;
          codeSearchInterval();
        }
        else {
          begin += step+1;
          step *= 2;

          return getAllEntries(data, function (err, entries) {
            if (err)
              return callback(err);

            allEntries = allEntries.concat(entries);
            var leftEntries = firstData.totalEntries - _.size(allEntries);
            if (leftEntries <= 0 || begin > _100MB)
              return callback(null, allEntries);
            console.log('Left ' + leftEntries);

            codeSearchInterval();
          });
        }
      });
    }
  });
}

function codeSearch(options, callback) {
  //Github allow only 10 calls per minute without login
  //and 30 calls per minute after you login
  setTimeout(function () {
    codeSearchImpl(options, callback);
  }, 2000);
}

function getAllEntries(firstData, callback) {
  if (!firstData.next)
    return callback(null, firstData.entries);

  var allEntries = [];
  process(null, firstData);

  function process(err, data) {
    if (err)
      return callback(err);

    allEntries = allEntries.concat(data.entries);
    if (data.next)
      return codeSearch({next: data.next}, process);
    callback(null, allEntries);
  }
}

function login(login, password, callback) {
  var loginUrl = baseUrl + '/login';
  request.get(loginUrl, function (error, response, html) {
    if (error)
      return callback(error);

    var $ = cheerio.load(html);
    var form = $('#login form');
    var method = form.attr('method');
    var url = URL.resolve(loginUrl, form.attr('action'));
    var formData = {}

    form.find('input').each(function () {
      formData[$(this).attr('name')] = $(this).attr('value');
    });

    formData.login = login;
    formData.password = password;

    request.post(url, {form: formData}, function (error, response, body) {
      callback(error);
    });
  });
}

/**
 * format: https://github.com/search?type=Issues&
 * q={query}&l={language}&o={order}&s={filter}
 */
function set_url(options) {
  var url = baseUrl + '/search?type=Code';
  ['query', 'language', 'order', 'filter'].forEach(function (name) {
    var value = options[name];
    if (value)
      url += '&' + name[0] + '=' + value
  });
  return url;
}

/**
 * issues_search method scrapes a given GitHub repository's issues list
 * @param {object} options - options for running the issue search
 *   query    - 'mentions', 'assignee', 'author' or 'user' (defaults to author)
 *   language - 
 *   order    - 'desc' or 'asc' descending / assending respectively (default desc)
 *   filter   - 'indexed' (used in conjunction with order), '
 * see: README/issues>search
 * @param {function} callback - the callback we should call after scraping
 *  a callback passed into this method should accept two parameters:
 *  @param {objectj} error an error object (set to null if no error occurred)
 *  @param {objects} list - list of (Public) GitHub issues (for the repo)
 */
function codeSearchImpl (options, callback) {
  if(!callback || typeof options === 'function') {
    callback = options;
    return callback(400);
  }
  var url;
  if(options.next) { // if we are parsing the next page of results!
    url = baseUrl + options.next;
  }
  else {
    url = set_url(options || {});   // generate search url
  }

  console.log(' - - - - - - - - - - - - - - - - - - - - - - - - - - search url:')
  console.log(url);

  request.get(url, function (error, response, html) {
    if (error)
      return callback(error);

    var $ = cheerio.load(html);

    if ($('.codesearch-results').length === 0) {
      var container = $('.container');
      if (container.length !== 0)
        return callback(new Error('Github return error: ' + container.text().trim()));
      return callback(new Error('Invalid HTML'));
    }

    var list = {};

    list.entries = Array.from($('.code-list-item').map(function () {
      var fileLink = $('.title a:nth-child(2)', this);
      var result = {
        repository: $('.title a:first-child', this).attr('href'),
        path: fileLink.attr('title'),
        lastIndexed: $('time', this).attr('datetime')
      };

      var indexedFile = fileLink.attr('href');
      var begin = (result.repository + '/blob/').length;
      var end = indexedFile.indexOf('/', begin);
      result.indexedCommit = indexedFile.slice(begin, end);
      return result;
    }));

    if ($('.blankslate').length > 0) {
      //We couldn’t find any code
      list.incomplete = false;
      list.totalEntries = 0;
    }
    else {
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
    }

    var next = $('.next_page')
    if(next.length > 0) {
      list.next = next.attr('href');
    }

    return callback(error, list);
  });
}
