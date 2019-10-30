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

VegaTransformPostgres.setHttpOptions = function(httpOptions) {
  if(httpOptions) {
    this._httpOptions = httpOptions;
    return this;
  }
  return this._httpOptions;
}

VegaTransformPostgres.setPostgresConnectionString = function(postgresConnectionString) {
  if(postgresConnectionString) {
    this._postgresConnectionString = postgresConnectionString;
    return this;
  }
  return this._postgresConnectionString;
}

VegaTransformPostgres.Definition = {
  type: "postgres", // FixMe: make uppercase
  metadata: { changes: true, source: true },
  params: [
    { name: "query", type: "string", required: false },
    { name: "field", type: "string", required: false },
    { name: "table", type: "string", required: false }, // FixMe: rename to "relation"
    { name: "max_bins", type: "number", required: false },
    { name: "anchor", type: "signal", required: false},
    { name: "step", type: "signal", required: false}
  ]
};

const prototype = inherits(VegaTransformPostgres, Transform);

prototype.transform = async function(_, pulse) {
  if(!VegaTransformPostgres._httpOptions) {
    throw Error("Vega Transform Postgres http options missing. Assign it with setHttpOptions.");
  }
  if(!VegaTransformPostgres._postgresConnectionString) {
    throw Error("Vega Transform Postgres postgres connection string missing. Assign it with setPostgresConnectionString.");
  }
  if(_.query === "bin") {
    if(typeof _.field !== "string") {
      throw Error("Vega Transform Postgres bin query requires field param")
    }
    if(typeof _.table !== "string") {
      throw Error("Vega Transform Postgres bin query requires table param")
    }
    if(typeof _.max_bins !== "number") {
      throw Error("Vega Transform Postgres bin query requires max_bins param")
    }
  }
  const result = await new Promise((resolve, reject) => {
    const postData = querystring.stringify({
      query: _.query,
      postgresConnectionString: VegaTransformPostgres._postgresConnectionString
    });
    VegaTransformPostgres._httpOptions['Content-Length'] = Buffer.byteLength(postData);
    const req = http.request(VegaTransformPostgres._httpOptions, res => {
      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        if(res.statusCode === 400) {
          reject(`${res.statusMessage}: ${data}`);
        } else {
          resolve(JSON.parse(data).rows);
        }
      });
    });
    req.on('error', err => {
      reject(err);
    });
    req.write(postData);
    req.end();
  }).catch(err => {
    console.log(err);
    return [];
  });
  console.log(JSON.stringify(result));
  result.forEach(ingest);
  const out = pulse.fork(pulse.NO_FIELDS & pulse.NO_SOURCE);
  this.value = out.add = out.source = out.rem = result;
  return out;
}