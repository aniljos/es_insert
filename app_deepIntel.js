const fs = require('fs');
const path = require('path');
const moment = require('moment');
const mappingJson = require('./pyrogram_es_mapping.json');
const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })

// const client = new Client(
//     { 
//         node: 'https://17f1cbeb4d354b0faa7fa0933fecf1d6.us-west-1.aws.found.io:9243',
//         auth: {
//           username: 'elastic',
//           password: 'i8wZXsGHuk9E8sEd340g7KBi'
//         }
      
//     });

const dataset = [];
let transformedDataset = []
function scanDir() {

    //const dirName = "C:\\Finacus\\Elastic Search\\channel2fspl\\channel2fspl";
    //const dirName = "C:\\Finacus\\Elastic Search\\SampleFileProcessed\\SampleFileProcessed";
    const dirName = "C:\\Finacus\\Elastic Search\\InProcess\\JSON\\5";

    return new Promise(function (resolve, reject) {

        fs.readdir(dirName, function (err, files) {
            if (err) {
                console.log("Unable to scan directory...", err);
                reject("Unable to scan directory");
                return;
            }

            files.forEach(function (file) {
                if (file.endsWith(".json")) {
                    console.log(file);
                    const filePath = path.join(dirName, file);
                    // fs.readFile(filePath, 'utf8', function (err, contents) {
                    //     if (err) {
                    //         reject("Cannot read files");
                    //         return console.log("Cannot read files", err);
                    //     }

                    //     data.push(contents);
                        
                    // })

                    const contents = fs.readFileSync(filePath, 'utf8');
                    dataset.push(contents);
                }

            });
            resolve();


        })

    });
}


async function run() {

  await transform();
    //await scanDir();
    // console.log(dataset);
    // console.log(dataset.length);
    await client.indices.create({
        index: "deep-intel-002",
        
        
    }, { ignore: 400 });

    //console.log(transformedDataset.length);
    const body = transformedDataset.flatMap(doc => [{ index: { _index: 'deep-intel-002' } }, doc]);
      const { body: bulkResponse } = await client.bulk({ refresh: true, body });

      if (bulkResponse.errors) {
        const erroredDocuments = []
        // The items array has the same order of the dataset we just indexed.
        // The presence of the `error` key indicates that the operation
        // that we did for the document has failed.
        bulkResponse.items.forEach((action, i) => {
          const operation = Object.keys(action)[0]
          if (action[operation].error) {
            erroredDocuments.push({
              // If the status is 429 it means that you can retry the document,
              // otherwise it's very likely a mapping error, and you should
              // fix the document before to try it again.
              status: action[operation].status,
              error: action[operation].error,
              operation: body[i * 2],
              document: body[i * 2 + 1]
            })
          }
        })
        console.log(erroredDocuments)
      }
    
      const { body: count } = await client.count({ index: 'deep-intel' })
      console.log(count);
}

async function transform(){

  await scanDir();
  console.log("Loaded");

  //clsconsole.log(dataset);

  transformedDataset = dataset.map(item => {
    const obj = JSON.parse(item);
    console.log(obj);
    return {
        ...obj,
        date: moment(obj.date),
    }
  });

  //console.log(JSON.stringify(transformedDataset));

}

//transform();
run().catch(console.log);

// console.log(moment().format());

 
//console.log(typeof dt);
//console.log(dt.format());