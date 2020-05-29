const Papa = require('papaparse');
const { createWriteStream } = require('fs');
const { get } = require('https');
const { pipeline, Transform } = require('stream');
const unzipper = require('unzipper');
const { runInThisContext } = require('vm');

/**
 * The main 'geoname' table has the following fields :
 * ---------------------------------------------------
 * 0  geonameid         : integer id of record in geonames database
 * 1  name              : name of geographical point (utf8) varchar(200)
 * 2  asciiname         : name of geographical point in plain ascii characters, varchar(200)
 * 3  alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
 * 4  latitude          : latitude in decimal degrees (wgs84)
 * 5  longitude         : longitude in decimal degrees (wgs84)
 * 6  feature class     : see http://www.geonames.org/export/codes.html, char(1)
 * 7  feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
 * 8  country code      : ISO-3166 2-letter country code, 2 characters
 * 9  cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
 * 10 admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
 * 11 admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
 * 12 admin3 code       : code for third level administrative division, varchar(20)
 * 13 admin4 code       : code for fourth level administrative division, varchar(20)
 * 14 population        : bigint (8 byte int)
 * 15 elevation         : in meters, integer
 * 16 dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
 * 17 timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
 * 18 modification date : date of last modification in yyyy-MM-dd format
 */

const columnIndexToName = {
  0: 'id',
  1: 'name',
  2: 'asciiName',
  3: 'alternateNames',
  4: 'latitude',
  5: 'longitude',
  6: 'featureClass',
  7: 'featureCode',
  8: 'countryCode',
  9: 'cc2',
  10: 'admin1Code',
  11: 'admin2Code',
  12: 'admin3Code',
  13: 'admin4Code',
  14: 'population',
  15: 'elevation',
  16: 'dem',
  17: 'timezone',
  18: 'modificationDate',
};
exports.columnIndexToName = columnIndexToName;

const columnNameToIndex = Object.keys(columnIndexToName).reduce((acc, key) => {
  acc[columnIndexToName[key]] = key;
  return acc;
}, {});

/**
 * @param {string} filterExpression
 * @param {(string|number|boolean)[]} data
 * @returns {boolean}
 */
function testFilterExpression(filterExpression, data) {
  const filterLines = data.reduce((acc, value, index) => {
    const encodedValue =
      typeof value === 'string' ? JSON.stringify(value) : value;

    return [
      ...acc,
      `const ${columnIndexToName[index]} = ${encodedValue};`,
    ];
  }, []);
  filterLines.push(`return ${filterExpression};`);

  return runInThisContext(
    `(function () {${filterLines.join('\n')}})()`
  );
}

/**
 * Extract and filter data from a geonames file.
 *
 * @param {string} url
 * @param {string} dest
 * @param {string[]} columns
 * @param {string} [filterExpression]
 * @returns {Promise<void>}
 */
exports.extract = async (url, dest, columns, filterExpression) => {
  const columnIndexes = columns.map((name) => columnNameToIndex[name]);
  const parse = Papa.parse(Papa.NODE_STREAM_INPUT, {
    delimiter: '\t',
    skipEmptyLines: true,
    dynamicTyping: true,
  });
  const destStream = createWriteStream(dest, {
    encoding: 'utf8',
  });

  // Append columns
  destStream.write(Papa.unparse([columns], { delimiter: ',' }) + '\n');

  return new Promise((resolve, reject) => {
    const req = get(url, (res) => {
      res
        .pipe(unzipper.Parse())
        .on('entry', (entry) => {
          if (entry.path.includes('readme')) {
            return entry.autodrain();
          }
          pipeline(
            entry,
            parse,
            new Transform({
              objectMode: true,
              transform: (data, _, done) => {
                if (filterExpression) {
                  const passed = testFilterExpression(filterExpression, data);

                  if (!passed) {
                    return done(null, '');
                  }
                }

                const transformed = columnIndexes.map((index) => data[index]);

                done(
                  null,
                  Papa.unparse([transformed], { delimiter: ',' }) + '\n'
                );
              },
            }),
            destStream,
            (err, data) => {
              return err ? reject(err) : console.log(`Processed ${entry.path}`);
            }
          );
        })
        .on('error', (err) => reject(err))
        .on('end', () => {
          console.log('Processing complete');
          resolve();
        });
    });
    req.on('error', (err) => reject(err));
    req.end();
  });
};
