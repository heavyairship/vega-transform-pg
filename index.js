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

function hasFields(node) {
  return node._argval 
    && node._argval.encoders 
    && node._argval.encoders.enter 
    && node._argval.encoders.enter.fields
    && node._argval.encoders.enter.fields.length
}

function hasSourcePathTo(node, dest) {
  if(node.source) {
    if(node.source.id === dest.id) {
      return true;
    }
    return hasSourcePathTo(node.source, dest);
  }
  return false;
}

function isCollectNode(node) {
  const def = node.__proto__.constructor.Definition;
  return def && def.type === "Collect";
}

function upstreamCollectNodeFor(node) {
  if(isCollectNode(node.source)) {
    return node.source;
  }
  return upstreamCollectNodeFor(node.source);
}

function collectFields(node, transform) {
  // FixMe: this function and its subroutines 
  // can be made more efficient. E.g. get the collect
  // node on the way to checking if there is a source path
  // to the transform.
  let out = {};

  if(hasFields(node) && hasSourcePathTo(node, transform)) {
    const upstreamCollectNode = upstreamCollectNodeFor(node);
    out[upstreamCollectNode.id] = {
      collectNode: upstreamCollectNode,
      fields: node._argval.encoders.enter.fields
    }
  }
  
  if(!node._targets) {
    return out;
  }
   
  for(const target of node._targets) {
    out = {...out, ...collectFields(target, transform)};
  }
  return out;
}

function queryFor(fields, table) {
  // FixMe: only projections are implemented right now.
  let out = "SELECT ";
  for(let fieldIdx=0; fieldIdx<fields.length; ++fieldIdx) {
    if(fieldIdx) {
      out += ", ";
    }
    out += fields[fieldIdx];
  }
  out += ` FROM ${table};`
  return out;
}

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

  // FixMe: need to handle multiple entries, not just one.
  // To do that, I have to figure out a way to use async
  // calls in a loop without getting this error:
  // 
  // 'babel-plugin-transform-async-to-promises/helpers' is imported by index.js, but could not be resolved – treating it as an external dependency
  // Error: Could not load babel-plugin-transform-async-to-promises/helpers (imported by /Users/afichman/Desktop/Projects/scalable-vega/node_modules/vega-transform-pg/index.js): ENOENT: no such file or directory, open 'babel-plugin-transform-async-to-promises/helpers'
  //
  // Or, I can just chain promises with then().
  
  const fields = collectFields(this, this);
  const collectNodeId = Object.keys(fields)[0];
  const entry = fields[collectNodeId];
  const query = queryFor(entry.fields, _.table);

  const result = await new Promise((resolve, reject) => {
    const postData = querystring.stringify({
      query: query,
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

  //FixMe: figure out how to make this work at the collect level
  //result.forEach(ingest);
  //entry.collectNode.value = out.add = out.source = out.rem = result;
  //const out = pulse.fork(pulse.NO_FIELDS & pulse.NO_SOURCE);
  
  result.forEach(ingest);
  const out = pulse.fork(pulse.NO_FIELDS & pulse.NO_SOURCE);
  this.value = out.add = out.source = out.rem = result;
  return out;
}