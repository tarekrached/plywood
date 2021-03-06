var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, External, Dataset, TimeRange, $, ply, r } = plywood;

var attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'color', type: 'STRING' },
  { name: 'cut', type: 'STRING' },
  { name: 'isNice', type: 'BOOLEAN' },
  { name: 'tags', type: 'SET/STRING' },
  { name: 'carat', type: 'NUMBER' },
  { name: 'height_bucket', type: 'NUMBER' },
  { name: 'price', type: 'NUMBER', unsplitable: true },
  { name: 'tax', type: 'NUMBER', unsplitable: true },
  { name: 'vendor_id', special: 'unique', unsplitable: true }
];

var context = {
  'diamonds': External.fromJS({
    engine: 'druid',
    version: '0.9.1-legacy-lookups',
    source: 'diamonds',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true,
    filter: $("time").in({
      start: new Date('2015-03-12T00:00:00'),
      end: new Date('2015-03-19T00:00:00')
    })
  })
};


describe("simulate Druid 0.9.1-legacy-lookups", () => {

  it("works on fancy filter .lookup().in()", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.lookup('some_lookup').in(['D', 'C'])"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "lookup": {
          "namespace": "some_lookup",
          "type": "namespace"
        },
        "type": "lookup"
      },
      "type": "in",
      "values": [
        "D",
        "C"
      ]
    });
  });

  it("works on fancy filter .lookup().contains()", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.lookup('some_lookup').contains('hello')"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "lookup": {
          "namespace": "some_lookup",
          "type": "namespace"
        },
        "type": "lookup"
      },
      "query": {
        "caseSensitive": true,
        "type": "contains",
        "value": "hello"
      },
      "type": "search"
    });
  });

  it("works with lookup split (and subsplit)", () => {
    var ex = $("diamonds").split("$tags.lookup(tag_lookup)", 'Tag')
      .sort('$Tag', 'descending')
      .limit(10)
      .apply(
        'Cuts',
        $("diamonds").split("$cut", 'Cut')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "tags",
          "extractionFn": {
            "lookup": {
              "namespace": "tag_lookup",
              "type": "namespace"
            },
            "type": "lookup"
          },
          "outputName": "Tag",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": {
          "metric": {
            "type": "lexicographic"
          },
          "type": "inverted"
        },
        "queryType": "topN",
        "threshold": 10
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "cut",
          "outputName": "Cut",
          "type": "default"
        },
        "filter": {
          "dimension": "tags",
          "extractionFn": {
            "lookup": {
              "namespace": "tag_lookup",
              "type": "namespace"
            },
            "type": "lookup"
          },
          "type": "selector",
          "value": "something"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

});
