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

VegaTransformPostgres.Definition = {
  type: "VegaTransformPostgres",
  metadata: { changes: true, source: true },
  params: [{ name: "query", type: "string", required: true }]
};

VegaTransformPostgres.query = function(query) {
  this.query = query;
}

const prototype = inherits(VegaTransformPostgres, Transform);

prototype.transform = async function(_, pulse) {
  return new Promise((resolve, reject) => {
    const postData = querystring.stringify({
      'query': this.query
    });
    const options = {
      hostname: 'localhost',
      port: 3000,
      method: 'POST',
      path: '/query',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData)
      }
    }
    const req = http.request(options, res => {
      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        console.log(JSON.parse(data).rows);
        resolve(data);
      });
    });
    req.on('error', err => {
      console.error(`Error: ${err}`);
      reject();
    });
    req.write(postData);
    req.end();
  });
}