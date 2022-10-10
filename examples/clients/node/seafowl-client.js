#! /usr/bin/env node

/** # DEMO NODEJS SEAFOWL CLIENT
 *  This is a very simple client using only builtin node functions.
 *  To execute a read-only query against a locally running seafowl instance, run:
 *
 *  ```
 *  node seafowl-client.js 'SELECT 1;'
 *  ```
 *
 *  Use the `ENDPOINT` env var to set the endpoint, eg:
 *
 *  ```
 *  ENDPOINT="https://demo.seafowl.io/q" node seafowl-client.js 'SELECT 1;'
 *  ```
 *  
 *  By default, HTTP POST requests are made which can be used to execute
 *  both read-only and write queries. To test the GET endpoint, use the
 *  `-r` command line argument.
 *
 *  ```
 *  node seafowl-client.js -r 'SELECT 1;'
 *  ```
 *
 *  To execute SQL commands from a file, do:
 *
 *  ```
 *  node seafowl-client.js "$(cat test1.sql)"
 *  ```
 *
 *  To submit a password with a write query, set the `PASSWORD` env var:
 *
 *  ```
 *  PASSWORD=25885363 node seafowl-client.js "$(cat test1.sql)"
 *  ```
 */


const crypto = require('crypto');

const trimQuery = sql => sql.trim().replace(/(?:\r\n|\r|\n)/g, " ");

const hash = sql => crypto.createHash('sha256').update(sql).digest('hex');

const request = (endpoint, options={}, cb) => {
    let {protocol, hostname, port, pathname} = new URL(endpoint);
    const mod = protocol === 'https:' ? 'https' : 'http';
    return require(mod).request({
        port: parseInt(port, 10),
        path: pathname,
        hostname,
        ...options
    }, cb);
}

const readQuery = (endpoint, query) => new Promise((resolve, reject) => {
    const {pathname: pathPrefix} = new URL(endpoint);
    let response = "";
    const options = {
      path: `${pathPrefix}/${hash(query)}.csv`,
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'X-Seafowl-Query': encodeURIComponent(query)
      }
    };

    const req = request(endpoint, options, res => {
      console.log(`statusCode: ${res.statusCode}`);
      res.on('data', d => {
        if (d.length === 0) {
            resolve(response)
        } else {
            response += d;
        }
      });
    });

    req.on('error', error => {
      reject(error);
    });

    req.end();
});

const writeQuery = (endpoint, query, password) => new Promise((resolve, reject) => {
    const data = JSON.stringify({query});
    const headers = {
        'Content-Type': 'application/json',
        'Content-Length': data.length,
    };
    if (password) {
        headers['Authorization'] = `Bearer ${password}`
    }
    let response = "";
    const options = {
      method: 'POST',
      headers
    };

    const req = request(endpoint, options, res => {
      console.log(`statusCode: ${res.statusCode}`);

      res.on('data', d => {
        if (d.length === 0) {
            resolve(response)
        } else {
            response += d;
        }
      });
    });

    req.write(data)

    req.on('error', error => {
      reject(error);
    });

    req.end();
});

if (require.main === module) {
    const endpoint = process.env['ENDPOINT'] ?? 'http://localhost:8080/q';
    const args = process.argv.slice(2);
    let result;
    if (args[0]?.trim() === '-r') {
        result = readQuery(endpoint, args.slice(1).join(" "))
    } else {
        result = writeQuery(endpoint, args.join(" "), process.env['PASSWORD'])
    }
    result.then(
        data => process.stdout.write(data),
        error => console.error(error)
    )
}

module.exports = {readQuery, writeQuery}
