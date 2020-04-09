# Neo for Geonames

Neo is a streaming parser, filter and formatter for Geonames data dumps.

## Getting Started

### Installing

```
npm i neo
```

### CLI Usage

Example: extract id and name from places in Andorra whose names are less than 10 characters long and save them to `data.csv`.

```
npx neo --columns id name \
  --url https://download.geonames.org/export/dump/AD.zip \
  --dest data.csv \
  --filter "name.length < 10"
```

### Programatic usage

```js
const { extract } = require('neo');

const url = 'https://download.geonames.org/export/dump/AD.zip';
const dist = 'data.csv';
const columns = ['id', 'name'];
const filter = 'name.length < 10';

extract(url, dist, columns, filter)
  .then(() => console.log('done'))
  .catch((err) => console.log(err));
```

### License

This project is licensed under the MIT License - see the LICENSE.md file for details.
