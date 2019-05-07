/*
sec,ctn
10800,9091234567
*/
const config = {
  csv: './data/ctn.csv', // path to csv with ctn and timezone
  zone: 'sec', // name of timezone column
  ctn: 'ctn', // name of ctn column
  max_size: 25000, // max rows in a file
  campaign: 'MAY_HOLIDAYS', // name of campaign to distinguish files
  divider: 3600, // 3600 for timezone in seconds
}

const fs = require('fs')
const path = require('path')
const csv = require('csv-parser')

const results = {}
const counter = {}

// clean directory with results
fs.readdir('results', (err, files) => {
  if (err) throw err

  for (const file of files) {
    fs.unlink(path.join('results', file), err => {
      if (err) throw err
    })
  }
})
// create file with ctn array
const printArray = (zone, array) => {
  const file = `${counter[zone]}_GMT${+zone >= 0 ? '+' : ''}${Math.round(
    +zone / config.divider
  )}_${array.length}_${config.campaign}.txt`
  fs.writeFile(`./results/${file}`, array.join('\n'), err => {
    if (err) throw err
    console.log(`The ${file} was succesfully saved!`)
  })
}

// whole process
fs.createReadStream(config.csv)
  .pipe(csv())
  .on('data', data => {
    const zone = data[config.zone]
    if (typeof results[zone] === 'undefined') {
      results[zone] = []
      counter[zone] = 1
    } else if (results[zone].length >= config.max_size) {
      printArray(zone, results[zone])
      results[zone] = []
    }
    results[zone].push(data[config.ctn])
  })
  .on('end', () => {
    // console.log(results)
    Object.keys(results).forEach(zone => {
      printArray(zone, results[zone])
    })
  })
