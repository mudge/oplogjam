// [start, update]
var testCases = [
    [{}, {a: 1, b: 2}],
    [{}, {$set: {a: 1}}],
    [{a: 0}, {$set: {a: 1}}],
    [{}, {$set: {a: 1, b: 2}}],
    [{a: 0, b: 0}, {$set: {a: 1, b: 2}}],
    [{a: 0, b: 0}, {$unset: {a: ''}}],
    [{a: 0, b: 0}, {$unset: {a: '', b: ''}}],
    [{a: 0, b: 0}, {$unset: {c: ''}}],
    [{a: 0}, {$set: {b: 1}, $unset: {a: ''}}],
    [{}, {$set: {'a.0': 1}}],
    [{a: []}, {$set: {'a.0': 1}}],
    [{a: {}}, {$set: {'a.0': 1}}],
    [{a: []}, {$set: {'a.1': 1}}],
    [{}, {$unset: {'a.0': ''}}],
    [{a: []}, {$unset: {'a.0': ''}}],
    [{a: [1]}, {$unset: {'a.0': ''}}],
    [{a: [1, 2]}, {$unset: {'a.0': ''}}],
    [{a: [1, 2]}, {$unset: {'a.1': ''}}],
    [{a: [1, 2]}, {$unset: {'a.2': ''}}],
    [{a: {}}, {$unset: {'a.0': ''}}],
    [{a: []}, {$unset: {'a.1': ''}}],
    [{}, {$set: {'a.b': 1}}],
    [{}, {$set: {'a.b.c': 1}}],
    [{}, {$set: {'a.b.c': 1, 'a.d': 2}}],
    [{a: {}}, {$set: {'a.b': 1}}],
    [{a: {b: 0}}, {$set: {'a.b': 1}}],
    [{}, {$set: {'a.1.b': 1}}],
    [{a: []}, {$set: {'a.1.b': 1}}],
    [{}, {$set: {'a.1.b.1': 1}}],
    [{a: []}, {$set: {'a.1.b.1': 1}}],
];

db.bar.drop();
db.bar.insert({_id: 1});

function jsontohash(obj) {
    return tojson(obj).replace(/ : /g, ' => ').replace(/null/g, 'nil');
}

testCases.forEach(function (testCase) {
    var start = testCase[0],
        update = testCase[1],
        doc = {_id: 1},
        result;

    for (var key in start) {
        doc[key] = start[key];
    }

    db.bar.update({_id: 1}, start);
    db.bar.update({_id: 1}, update);

    result = db.bar.findOne({_id: 1});

    print(
        "it 'applies " + tojson(update) + " to " + tojson(start) + "' do\n" +
        "  table.insert(id: '1', document: '" + tojson(doc) + "')\n" +
        "  update = build_update(1, " + jsontohash(update) + ")\n" +
        "  update.apply('foo.bar' => table)\n" +
        "\n" +
        "  expect(table.first).to include(:document => Sequel.pg_jsonb(" + jsontohash(result) +"))\n" +
        "end\n"
    );
});
