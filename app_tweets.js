const { Client } = require('@elastic/elasticsearch')


//const client = new Client({ node: 'http://localhost:9200' })
const client = new Client(
    { 
        node: 'https://17f1cbeb4d354b0faa7fa0933fecf1d6.us-west-1.aws.found.io:9243',
        auth: {
          username: 'elastic',
          password: 'i8wZXsGHuk9E8sEd340g7KBi'
        }
      
    })

async function run(){

    await client.indices.create({
        index: "tweets",
        body: {
            mappings: {
                properties: {
                    id: {type: 'integer'},
                    text: {type: 'text'},
                    user: {type: 'keyword'},
                    date: {
                      type: 'date',
                      format: "yyyy-MM-dd HH:mm:ss"
                    }
                }
            }
        }
    }, {ignore: 400});

    // await client.index({
        
    //     index: 'tweets',
    //     body: {
    //         id: "2",
    //         text: "Hello there!!",
    //         user: "Anil",
    //         time: new Date()
    //     },
    //     refresh: true
    // });

    const dataset = [{
        id: 1,
        text: 'If I fall, don\'t bring me back.',
        user: 'jon',
        date: new Date()
      }, {
        id: 2,
        text: 'Witer is coming',
        user: 'ned',
        date: new Date()
      }, {
        id: 3,
        text: 'A Lannister always pays his debts.',
        user: 'tyrion',
        date: new Date()
      }, {
        id: 4,
        text: 'I am the blood of the dragon.',
        user: 'daenerys',
        date: new Date()
      }, {
        id: 5, // change this value to a string to see the bulk response with errors
        text: 'A girl is Arya Stark of Winterfell. And I\'m going home.',
        user: 'arya',
        date: new Date()
      }];

      const body = dataset.flatMap(doc => [{ index: { _index: 'tweets' } }, doc]);
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
    
      const { body: count } = await client.count({ index: 'tweets' })
      console.log(count)


}

run().catch(console.log);

