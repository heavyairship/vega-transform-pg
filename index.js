import { inherits, ingest, Transform } from "vega";
const querystring = require('querystring');
const http = require('http');

/**
 * Generates a function to query data from an Postgres database.
 * @constructor
 * @param {object} params - The parameters for this operator.
 * @param {function(object): *} params.query - The SQL query.
 */
export default function VegaTransformPostgres(params) {
  Transform.call(this, [], params);
}

VegaTransformPostgres.setHttpOptions = function(options) {
  if(options) {
    this._options = options;
    return this;
  }
  return this._options;
}

VegaTransformPostgres.Definition = {
  type: "postgres",
  metadata: { changes: true, source: true },
  params: [{ name: "query", type: "string", required: true }]
};

const prototype = inherits(VegaTransformPostgres, Transform);

prototype.transform = async function(_, pulse) {
  if(!VegaTransformPostgres._options) {
    throw Error("Vega Transform Postgres query missing. Assign it with setQuery.");
  }
  const result = await new Promise((resolve, reject) => {
    console.log(_.query);
    const postData = querystring.stringify({'query': _.query});
    VegaTransformPostgres._options['Content-Length'] = Buffer.byteLength(postData)
    const req = http.request(VegaTransformPostgres._options, res => {
      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        resolve(JSON.parse(data).rows);
      });
    });
    req.on('error', err => {
      console.error(`Error: ${err}`);
      reject();
    });
    req.write(postData);
    req.end();
  });
  console.log(result);
  result.forEach(ingest);
  const out = pulse.fork(pulse.NO_FIELDS & pulse.NO_SOURCE);
  out.rem = this.value;
  this.value = out.add = out.source = result;
  return out;
}
