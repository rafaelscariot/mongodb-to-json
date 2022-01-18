const { MongoClient } = require("mongodb");
const fs = require("fs");
const path = require("path");

const { DB_NAME, DOCUMENTS_LIMIT } = require("./config");

/* database url and port, if authentication is required, use the following connection format: mongodb://username:password@url:port */
const client = new MongoClient(`mongodb://localhost:27017/${DB_NAME}`);

const process = async (client) => {
  console.time("process");

  await client.connect();

  const collections = await getAllCollections(client);
  const documents = await getAllDocuments(client, collections);

  validateAndSave(documents);

  client.close();

  console.timeEnd("process");
};

const getAllCollections = (client) =>
  new Promise((resolve, reject) => {
    client
      .db()
      .listCollections()
      .toArray()
      .then((colls) => resolve(colls))
      .catch((err) => reject(err));
  });

const getAllDocuments = async (client, collections) => {
  return await Promise.all(
    collections.map((coll) => {
      const currentCollection = client.db().collection(coll.name);
      return currentCollection.find().limit(DOCUMENTS_LIMIT).toArray();
    })
  );
};

const validateAndSave = (documents) => {
  documents.forEach((document) => {
    if (document.length > 0) {
      document.forEach((doc) => {
        const file = path.resolve(__dirname, "./documents.json");
        const data = JSON.stringify(doc) + ",\n";
        fs.appendFile(file, data, (err) => {
          if (err) {
            console.log("Log:" + err.message);
          } else {
            console.log(`Documento ${doc._id} salvo com sucesso`);
          }
        });
      });
    }
  });
};

process(client);
