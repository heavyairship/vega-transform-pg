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
    { name: "relation", type: "string", required: true }
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
  if(!this._query) {
    throw Error("Internal error: this._query should be defined");
  }
  const result = await new Promise((resolve, reject) => {
    const postData = querystring.stringify({
      query: this._query,
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
    console.error(err);
    return [];
  });
  result.forEach(ingest);
  const out = pulse.fork(pulse.NO_FIELDS & pulse.NO_SOURCE);
  this.value = out.add = out.source = out.rem = result;
  return out;
}