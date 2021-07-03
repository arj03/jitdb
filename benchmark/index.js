const fs = require('fs')
const path = require('path')
const pull = require('pull-stream')
const Log = require('async-append-only-log')
const generateFixture = require('ssb-fixtures')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const multicb = require('multicb')
const ssbKeys = require('ssb-keys')
const TypedFastBitSet = require('typedfastbitset')
const runBenchmark = require('./helpers/run_benchmark')
const JITDB = require('../index')
const {
  query,
  fromDB,
  where,
  and,
  or,
  equal,
  slowEqual,
  count,
  toCallback,
  toPullStream,
  startFrom,
  paginate,
} = require('../operators')
const { seekType, seekAuthor, seekVoteLink } = require('../test/helpers')
const copy = require('../copy-json-to-bipf-async')
const { skipCreate, fixture, test } = require("./helpers/setup_test_functions")

const dir = '/tmp/jitdb-benchmark'
const oldLogPath = path.join(dir, 'flume', 'log.offset')
const newLogPath = path.join(dir, 'flume', 'log.bipf')
const reportPath = path.join(dir, 'benchmark.md')
const indexesDir = path.join(dir, 'indexes')

/**
 * Wait for a file to exist and for writes to that file
 * to complete
 * 
 * @param {string} filepath 
 * @param {Function} cb 
 */
const waitForFile = (filepath, cb) => {
  const maxTime = 5000
  const interval = 500
  let timeUsed = 0
  let fileSize = 0
  function fileReady() {
    fs.stat(filepath, (err, stats) => {
      if (err) {
        if (timeUsed < maxTime) {
          timeUsed += interval
          setTimeout(fileReady, interval)
        } else {
          cb(err)
        }
      } else if (stats.size > fileSize) {
        if (timeUsed < maxTime) {
          fileSize = stats.size
          timeUsed += interval
          setTimeout(fileReady, interval)
        } else {
          cb(new Error(`Timed out waiting for ${filepath} to finish writing`))
        }
      } else {
        cb()
      }
    })
  }
  setTimeout(fileReady, interval)
}

let alice
let bob
if (!skipCreate) {
  rimraf.sync(dir)
  mkdirp.sync(dir)

  const SEED = 'sloop'
  const MESSAGES = 100000
  const AUTHORS = 2000

  fixture('generate fixture with flumelog-offset', (t) => {
    generateFixture({
      outputDir: dir,
      seed: SEED,
      messages: MESSAGES,
      authors: AUTHORS,
      slim: true,
    }).then(() => {
      t.pass(`seed = ${SEED}`)
      t.pass(`messages = ${MESSAGES}`)
      t.pass(`authors = ${AUTHORS}`)
      t.true(fs.existsSync(oldLogPath), 'log.offset was created')
      alice = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
      bob = ssbKeys.loadOrCreateSync(path.join(dir, 'secret-b'))
      fs.appendFileSync(reportPath, '## Benchmark results\n\n')
      fs.appendFileSync(reportPath, '| Part | Speed | Heap Change | Samples |\n|---|---|---|---|\n')
      t.end()
    })
  })

  fixture('move flumelog-offset to async-log', (t) => {
    copy(oldLogPath, newLogPath, (err) => {
      if (err) {
        t.fail(err)
        t.end()
        return
      }
      waitForFile(
        newLogPath,
        (err) => {
          if (err) {
            t.fail(err)
            t.end()
          } else {
            t.pass('log.bipf was created')
            t.end()
          }
        }
      )
    })
  })
} else {
  alice = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  bob = ssbKeys.loadOrCreateSync(path.join(dir, 'secret-b'))
}

let raf
let db

const getJitdbReady = (cb) => {
  raf = Log(newLogPath, { blockSize: 64 * 1024 })
  db = JITDB(raf, indexesDir)
  db.onReady((err) => {
    cb(err)
  })
}

const closeLog = (cb) => {
  if (raf) {
    raf.close((err) => {
      if (err) cb(err)
      else rimraf(indexesDir, cb)
    })
  } else {
    rimraf(indexesDir, cb)
  }
}

test('core indexes', (t) => {
  runBenchmark(
    'Load core indexes',
    getJitdbReady,
    closeLog,
    (err, result) => {
      closeLog((err2) => {
        if (err || err2) {
          t.fail(err || err2)
        } else {
          fs.appendFileSync(reportPath, result.toString())
          t.pass(result)
        }
        t.end()
      })
    }
  )
})

const slowEqualHugeIndexQuery = (cb) => {
  query(
    fromDB(db),
    where(slowEqual('value.content.type', 'post')),
    toCallback((err, msgs) => {
      if (err) {
        cb(err)
      } else if (msgs.length !== 23310) {
        cb(new Error('msgs.length is wrong: ' + msgs.length))
      }
      cb()
    })
  )
}

test('slowEqual one huge index (first run)', (t) => {
  runBenchmark(
    'Query 1 big index with slowEqual (1st run)',
    slowEqualHugeIndexQuery,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady(cb)
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

test('slowEqual one huge index (second run)', (t) => {
  runBenchmark(
    'Query 1 big index with slowEqual (2nd run)',
    slowEqualHugeIndexQuery,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err2) => {
          if (err2) cb(err2)
          else slowEqualHugeIndexQuery(cb)
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

test('slowEqual count one huge index (third run)', (t) => {
  runBenchmark(
    'Query 1 big index with slowEqual (3rd run)',
    (cb) => {
      query(
        fromDB(db),
        where(slowEqual('value.content.type', 'post')),
        count(),
        toCallback((err, total) => {
          if (err) {
            cb(err)
          } else if (total !== 23310) {
            cb(new Error('total is wrong: ' + total))
          }
          cb()
        })
      )
    },
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err2) => {
          if (err2) cb(err2)
          else slowEqualHugeIndexQuery((err3) => {
            if (err3) cb(err3)
            else slowEqualHugeIndexQuery(cb)
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result)
        t.pass(result)
      }
      t.end()
    }
  )
})

const runHugeIndexQuery = (cb) => {
  query(
    fromDB(db),
    where(equal(seekType, 'post', { indexType: 'type' })),
    toCallback((err, msgs) => {
      if (err) {
        cb(err)
      } else if (msgs.length !== 23310) {
        cb(new Error('msgs.length is wrong: ' + msgs.length))
      }
      cb()
    })
  )
}

test('query one huge index (first run)', (t) => {
  runBenchmark(
    'Query 1 big index (1st run)',
    runHugeIndexQuery,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady(cb)
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result.toString())
        t.pass(result)
      }
      t.end()
    }
  )
})

test('query one huge index (second run)', (t) => {
  runBenchmark(
    'Query 1 big index (2nd run)',
    runHugeIndexQuery,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err2) => {
          if (err2) cb(err2)
          else runHugeIndexQuery(cb)
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result.toString())
        t.pass(result)
      }
      t.end()
    }
  )
})

test('count one huge index (third run)', (t) => {
  runBenchmark(
    'Count 1 big index (3rd run)',
    (cb) => {
      query(
        fromDB(db),
        where(equal(seekType, 'post', { indexType: 'type' })),
        count(),
        toCallback((err, total) => {
          if (err) {
            cb(err)
          } else if (total !== 23310) {
            cb(new Error('total is wrong: ' + total))
          }
          cb()
        })
      )
    },
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err2) => {
          if (err2) cb(err2)
          else runHugeIndexQuery((err3) => {
            if (err3) cb(err3)
            else runHugeIndexQuery(cb)
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result.toString())
        t.pass(result)
      }
      t.end()
    }
  )
})

test('create an index twice concurrently', (t) => {
  let done
  runBenchmark(
    'Create an index twice concurrently',
    (cb) => {
      query(
        fromDB(db),
        where(equal(seekType, 'about', { indexType: 'type' })),
        toCallback(done())
      )

      query(
        fromDB(db),
        where(equal(seekType, 'about', { indexType: 'type' })),
        toCallback(done())
      )

      done((err) => {
        if (err) cb(err)
        else cb()
      })
    },
    (cb) => {
      done = multicb({ pluck: 1 })
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady(cb)
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result.toString())
        t.pass(result)
      }
      t.end()
    }
  )
})

const runThreeIndexQuery = (cb) => {
  query(
    fromDB(db),
    where(
      or(
        equal(seekType, 'contact', { indexType: 'type' }),
        equal(seekAuthor, alice.id, {
          indexType: 'author',
          prefix: 32,
          prefixOffset: 1,
        }),
        equal(seekAuthor, bob.id, {
          indexType: 'author',
          prefix: 32,
          prefixOffset: 1,
        })
      )
    ),
    toCallback((err, msgs) => {
      if (err) cb(err)
      else if (msgs.length !== 24606)
        cb(new Error('msgs.length is wrong: ' + msgs.length))
      else cb()
    })
  )
}

test('query three indexes (first run)', (t) => {
  runBenchmark(
    'Query 3 indexes (1st run)',
    runThreeIndexQuery,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady(cb)
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result.toString())
        t.pass(result)
      }
      t.end()
    }
  )
})

test('query three indexes (second run)', (t) => {
  runBenchmark(
    'Query 3 indexes (2nd run)',
    runThreeIndexQuery,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery(cb)
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result.toString())
        t.pass(result)
      }
      t.end()
    }
  )
})

const useContactIndex = (cb) => {
  const filepath = path.join(indexesDir, 'type_contact.index')
  waitForFile(
    filepath,
    (err) => {
      if (err) cb(err)
      else {
        db.indexes['type_contact'] = {
          offset: 0,
          bitset: new TypedFastBitSet(),
          lazy: true,
          filepath,
        }
        cb()
      }
    }
  )
}

test('load two indexes concurrently', (t) => {
  let done
  runBenchmark(
    'Load two indexes concurrently',
    (cb) => {
      query(
        fromDB(db),
        where(
          or(
            equal(seekType, 'contact', { indexType: 'type' }),
            equal(seekAuthor, alice.id, {
              indexType: 'author',
              prefix: 32,
              prefixOffset: 1,
            }),
            equal(seekAuthor, bob.id, {
              indexType: 'author',
              prefix: 32,
              prefixOffset: 1,
            })
          )
        ),
        toCallback(done())
      )

      query(
        fromDB(db),
        where(
          or(
            equal(seekType, 'contact', { indexType: 'type' }),
            equal(seekAuthor, alice.id, {
              indexType: 'author',
              prefix: 32,
              prefixOffset: 1,
            }),
            equal(seekAuthor, bob.id, {
              indexType: 'author',
              prefix: 32,
              prefixOffset: 1,
            })
          )
        ),
        toCallback(done())
      )

      done((err) => {
        if (err) cb(err)
        else cb()
      })
    },
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery((err) => {
            if (err) cb(err)
            else {
              done = multicb({ pluck: 1 })
              useContactIndex(cb)
            }
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result.toString())
        t.pass(result)
      }
      t.end()
    }
  )
})

test('paginate big index with small pageSize', (t) => {
  const TOTAL = 20000
  const PAGESIZE = 5
  const NUMPAGES = TOTAL / PAGESIZE
  runBenchmark(
    `Paginate ${TOTAL} msgs with pageSize=${PAGESIZE}`,
    (cb) => {
      let i = 0
      pull(
        query(
          fromDB(db),
          where(equal(seekType, 'post', { indexType: 'type' })),
          paginate(PAGESIZE),
          toPullStream()
        ),
        pull.take(NUMPAGES),
        pull.drain(
          (msgs) => {
            i++
          },
          (err) => {
            if (err) cb(err)
            else if (i !== NUMPAGES) cb(new Error('wrong number of pages read: ' + i))
            else cb()
          }
        )
      )
    },
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery((err) => {
            if (err) cb(err)
            else {
              done = multicb({ pluck: 1 })
              useContactIndex(cb)
            }
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result.toString())
        t.pass(result)
      }
      t.end()
    }
  )
})

test('paginate big index with big pageSize', (t) => {
  const TOTAL = 20000
  const PAGESIZE = 500
  const NUMPAGES = TOTAL / PAGESIZE
  runBenchmark(
    `Paginate ${TOTAL} msgs with pageSize=${PAGESIZE}`,
    (cb) => {
      let i = 0
      pull(
        query(
          fromDB(db),
          where(equal(seekType, 'post', { indexType: 'type' })),
          paginate(PAGESIZE),
          toPullStream()
        ),
        pull.take(NUMPAGES),
        pull.drain(
          (msgs) => {
            i++
          },
          (err) => {
            if (err) cb(err)
            else if (i !== NUMPAGES) cb(new Error('wrong number of pages read: ' + i))
            else cb()
          }
        )
      )
    },
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery((err) => {
            if (err) cb(err)
            else {
              done = multicb({ pluck: 1 })
              useContactIndex(cb)
            }
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result.toString())
        t.pass(result)
      }
      t.end()
    }
  )
})

const getPrefixMapQueries = () => {
  let rootKey
  return {
    prepareRootKey: (cb) => {
      query(
        fromDB(db),
        paginate(1),
        toCallback((err, { results }) => {
          if (err) cb(err)
          else {
            rootKey = results[0].key
            cb()
          }
        })
      )
    },
    queryMap: (cb) => {
      let i = 0
      pull(
        query(
          fromDB(db),
          where(
            equal(seekVoteLink, rootKey, {
              indexType: 'value_content_vote_link',
              useMap: true,
              prefix: 32,
              prefixOffset: 1,
            })
          ),
          paginate(5),
          toPullStream()
        ),
        pull.drain(
          (msgs) => {
            i++
          },
          (err) => {
            if (err) cb(err)
            else if (i !== 92) cb(new Error('wrong number of pages read: ' + i))
            else cb()
          }
        )
      )
    },
  }
}

test('query a prefix map (first run)', (t) => {
  const { prepareRootKey, queryMap } = getPrefixMapQueries()
  runBenchmark(
    'Query a prefix map (1st run)',
    queryMap,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery((err) => {
            if (err) cb(err)
            else {
              done = multicb({ pluck: 1 })
              useContactIndex(function(err) {
                if (err) cb(err)
                else prepareRootKey(cb)
              })
            }
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result.toString())
        t.pass(result)
      }
      t.end()
    }
  )
})

test('query a prefix map (second run)', (t) => {
  const { prepareRootKey, queryMap } = getPrefixMapQueries()
  runBenchmark(
    'Query a prefix map (2nd run)',
    queryMap,
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery((err) => {
            if (err) cb(err)
            else {
              done = multicb({ pluck: 1 })
              useContactIndex(function(err) {
                if (err) cb(err)
                else prepareRootKey((err3) => {
                  if (err3) cb(err3)
                  else queryMap(cb)
                })
              })
            }
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result.toString())
        t.pass(result)
      }
      t.end()
    }
  )
})

test('paginate ten results', (t) => {
  runBenchmark(
    `Paginate 10 results`,
    (cb) => {
      pull(
        query(
          fromDB(db),
          where(
            and(
              equal(seekType, 'contact', { indexType: 'type' }),
              equal(seekAuthor, alice.id, {
                indexType: 'author',
                prefix: 32,
                prefixOffset: 1,
              })
            )
          ),
          startFrom(0),
          paginate(10),
          toPullStream()
        ),
        pull.take(1),
        pull.collect((err, msgs) => {
          if (err) cb(err)
          else if (msgs[0].length !== 10)
            cb(new Error('msgs.length is wrong: ' + msgs.length))
          else cb()
        })
      )
    },
    (cb) => {
      closeLog((err) => {
        if (err) cb(err)
        else getJitdbReady((err) => {
          if (err) cb(err)
          else runThreeIndexQuery((err) => {
            if (err) cb(err)
            else {
              done = multicb({ pluck: 1 })
              useContactIndex(cb)
            }
          })
        })
      })
    },
    (err, result) => {
      if (err) {
        t.fail(err)
      } else {
        fs.appendFileSync(reportPath, result.toString())
        t.pass(result)
      }
      t.end()
    }
  )
})
