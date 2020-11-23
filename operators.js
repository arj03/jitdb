const bipf = require('bipf')
const traverse = require('traverse')
const promisify = require('promisify-4loc')

function query(...cbs) {
  let res = cbs[0]
  for (let i = 1, n = cbs.length; i < n; i++) res = cbs[i](res)
  return res
}

function fromDB(db) {
  return {
    meta: { db },
  }
}

function toBuffer(value) {
  return Buffer.isBuffer(value) ? value : Buffer.from(value)
}

function offsets(values) {
  return {
    type: 'OFFSETS',
    offsets: values,
  }
}

function seqs(values) {
  return {
    type: 'SEQS',
    seqs: values,
  }
}

function seekFromDesc(desc) {
  const keys = desc.split('.')
  return (buffer) => {
    var p = 0
    for (let key of keys) {
      p = bipf.seekKey(buffer, p, Buffer.from(key))
      if (!~p) return void 0
    }
    return p
  }
}

function slowEqual(seekDesc, value, indexAll) {
  const indexType = seekDesc.replace(/\./g, '_')
  const seek = seekFromDesc(seekDesc)
  return {
    type: 'EQUAL',
    data: {
      seek: seek,
      value: toBuffer(value),
      indexType,
      indexAll,
    },
  }
}

function equal(seek, value, indexType, indexAll) {
  return {
    type: 'EQUAL',
    data: {
      seek: seek,
      value: toBuffer(value),
      indexType,
      indexAll,
    },
  }
}

function gt(value, indexName) {
  return {
    type: 'GT',
    data: {
      value,
      indexName,
    },
  }
}

function gte(value, indexName) {
  return {
    type: 'GTE',
    data: {
      value,
      indexName,
    },
  }
}

function lt(value, indexName) {
  return {
    type: 'LT',
    data: {
      value,
      indexName,
    },
  }
}

function lte(value, indexName) {
  return {
    type: 'LTE',
    data: {
      value,
      indexName,
    },
  }
}

function deferred(task) {
  return {
    type: 'DEFERRED',
    task,
  }
}

function debug() {
  return (ops) => {
    const meta = JSON.stringify(ops.meta, (key, val) =>
      key === 'db' ? void 0 : val
    )
    console.log(
      'debug',
      JSON.stringify(
        ops,
        (key, val) => {
          if (key === 'meta') return void 0
          else if (key === 'task' && typeof val === 'function')
            return '[Function]'
          else if (key === 'value' && val.type === 'Buffer')
            return Buffer.from(val.data).toString()
          else return val
        },
        2
      ),
      meta === '{}' ? '' : 'meta: ' + meta
    )
    return ops
  }
}

function copyMeta(orig, dest) {
  if (orig.meta) {
    dest.meta = orig.meta
  }
}

function updateMeta(orig, key, value) {
  const res = Object.assign({}, orig)
  res.meta[key] = value
  return res
}

function extractMeta(orig) {
  const meta = orig.meta
  return meta
}

function and(...args) {
  const rhs = args.map((arg) => (typeof arg === 'function' ? arg() : arg))
  return (ops) => {
    const res =
      ops && ops.type
        ? {
            type: 'AND',
            data: [ops, ...rhs],
          }
        : rhs.length > 1
        ? {
            type: 'AND',
            data: rhs,
          }
        : rhs[0]
    if (ops) copyMeta(ops, res)
    return res
  }
}

function or(...args) {
  const rhs = args.map((arg) => (typeof arg === 'function' ? arg() : arg))
  return (ops) => {
    const res =
      ops && ops.type
        ? {
            type: 'OR',
            data: [ops, ...rhs],
          }
        : rhs.length > 1
        ? {
            type: 'OR',
            data: rhs,
          }
        : rhs[0]
    if (ops) copyMeta(ops, res)
    return res
  }
}

function descending() {
  return (ops) => updateMeta(ops, 'descending', true)
}

function startFrom(offset) {
  return (ops) => updateMeta(ops, 'offset', offset)
}

function paginate(pageSize) {
  return (ops) => updateMeta(ops, 'pageSize', pageSize)
}

async function executeDeferredOps(ops, meta) {
  // Collect all deferred tasks and their object-traversal paths
  const allDeferred = []
  traverse(ops).forEach(function (val) {
    if (!val) return
    // this.block() means don't traverse inside these, they won't have DEFERRED
    if (this.path.length === 1 && this.key === 'meta') return this.block()
    if (val.type === 'Buffer' && Array.isArray(val.data)) return this.block()
    if (val.type === 'DEFERRED' && val.task) allDeferred.push([this.path, val])
    if (val.type !== 'AND' && val.type !== 'OR') this.block()
  })
  if (allDeferred.length === 0) return ops

  // Execute all deferred tasks and collect the results (and the paths)
  const allResults = await Promise.all(
    allDeferred.map(([path, obj]) =>
      promisify(obj.task)(meta).then((result) => [path, result])
    )
  )

  // Replace all deferreds with their respective results
  allResults.forEach(([path, result]) => {
    result.meta = meta
    if (path.length === 0) ops = result
    else traverse(ops).set(path, result)
  })

  return ops
}

function toCallback(cb) {
  return (rawOps) => {
    const meta = extractMeta(rawOps)
    executeDeferredOps(rawOps, meta)
      .then((ops) => {
        const offset = meta.offset || 0
        if (meta.pageSize)
          meta.db.paginate(ops, offset, meta.pageSize, meta.descending, cb)
        else meta.db.all(ops, offset, meta.descending, cb)
      })
      .catch((err) => {
        cb(err)
      })
  }
}

function toPromise() {
  return async (rawOps) => {
    const meta = extractMeta(rawOps)
    const ops = await executeDeferredOps(rawOps, meta)
    const offset = meta.offset || 0
    return await new Promise((resolve, reject) => {
      const cb = (err, data) => {
        if (err) reject(err)
        else resolve(data)
      }
      if (meta.pageSize)
        meta.db.paginate(ops, offset, meta.pageSize, meta.descending, cb)
      else meta.db.all(ops, offset, meta.descending, cb)
    })
  }
}

function toPullStream() {
  return (rawOps) => {
    const meta = extractMeta(rawOps)
    let offset = meta.offset || 0
    let total = Infinity
    const limit = meta.pageSize || 1
    let ops
    function readable(end, cb) {
      if (end) return cb(end)
      if (offset >= total) return cb(true)
      meta.db.paginate(ops, offset, limit, meta.descending, (err, result) => {
        if (err) return cb(err)
        else {
          total = result.total
          offset += limit
          cb(null, !meta.pageSize ? result.data[0] : result.data)
        }
      })
    }
    return function (end, cb) {
      if (!ops) {
        executeDeferredOps(rawOps, meta)
          .then((_ops) => {
            ops = _ops
            readable(end, cb)
          })
          .catch((err) => {
            cb(err)
          })
      } else {
        readable(end, cb)
      }
    }
  }
}

// `async function*` supported in Node 10+ and browsers (except IE11)
function toAsyncIter() {
  return async function* (rawOps) {
    const meta = extractMeta(rawOps)
    const ops = await executeDeferredOps(rawOps, meta)
    let offset = meta.offset || 0
    let total = Infinity
    const limit = meta.pageSize || 1
    while (offset < total) {
      yield await new Promise((resolve, reject) => {
        meta.db.paginate(ops, offset, limit, meta.descending, (err, result) => {
          if (err) return reject(err)
          else {
            total = result.total
            offset += limit
            resolve(!meta.pageSize ? result.data[0] : result.data)
          }
        })
      })
    }
  }
}

module.exports = {
  fromDB,
  query,

  slowEqual,
  equal,
  gt,
  gte,
  lt,
  lte,
  and,
  or,
  deferred,

  offsets,
  seqs,

  descending,
  startFrom,
  paginate,
  toCallback,
  toPullStream,
  toPromise,
  toAsyncIter,

  debug,
}
