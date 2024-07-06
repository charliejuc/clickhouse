const { createClient } = require("@clickhouse/client");
const { faker } = require("@faker-js/faker");

const client = createClient({
  host: process.env.CLICKHOUSE_HOST || "http://localhost:8123",
  username: process.env.CLICKHOUSE_USER || "default",
  password: process.env.CLICKHOUSE_PASSWORD || "",
  database: "default",
});

async function createTable() {
  const createTableQuery = `
  CREATE TABLE IF NOT EXISTS transactions (
    id UInt32,
    user_id UInt32,
    amount Float32,
    currency String,
    timestamp DateTime('Europe/Madrid')
  ) ENGINE = MergeTree()
  ORDER BY id;
  `;

  try {
    await client.command({
      query: createTableQuery,
    });
    console.log("Table created successfully");
  } catch (error) {
    console.error("Error creating table:", error);
  }
}

async function insertRandomTransactions(totalCount, batchSize) {
  for (let start = 0; start < totalCount; start += batchSize) {
    const batch = [];
    for (let i = start; i < Math.min(start + batchSize, totalCount); i++) {
      const timestamp = faker.date
        .between({ from: "2022-01-01T00:00:00Z", to: "2022-12-31T23:59:59Z" })
        .toISOString();
      batch.push({
        id: i,
        user_id: faker.number.int({ min: 1, max: 1000 }),
        amount: Number(faker.finance.amount({ min: 1, max: 1000, dec: 2 })),
        currency: faker.finance.currencyCode(),
        timestamp: timestamp.replace(/\..*$/g, "").replace("T", " "),
      });
    }

    try {
      await client.insert({
        table: "transactions",
        values: batch,
        format: "JSONEachRow",
      });
      console.log(`${batch.length} transactions inserted successfully`);
    } catch (error) {
      console.error("Error inserting data:", error);
    }
  }
}

async function aggregateData() {
  const queries = [
    "SELECT COUNT(*) as total_transactions FROM transactions",
    "SELECT currency, SUM(amount) as total_amount FROM transactions GROUP BY currency",
    "SELECT user_id, COUNT(*) as transaction_count FROM transactions GROUP BY user_id ORDER BY transaction_count DESC LIMIT 10",
  ];

  for (const query of queries) {
    try {
      const result = await client.query({
        query,
        format: "JSON",
      });
      const rows = await result.json();
      console.log(`Result for query "${query}":`, rows);
    } catch (error) {
      console.error(`Error executing query "${query}":`, error);
    }
  }
}

async function main() {
  //   await createTable();
  //   const totalCount = 10_000_000;
  //   const batchSize = 50_000;
  //   await insertRandomTransactions(totalCount, batchSize);
  await aggregateData();
}

main();
