const { createClient } = require("@clickhouse/client");
const { faker } = require("@faker-js/faker");

// session is required to update memory limit and other configs
const sessionId = `session_${Date.now()}`;
const client = createClient({
  url: process.env.CLICKHOUSE_URL || "http://localhost:8123",
  username: process.env.CLICKHOUSE_USER || "default",
  password: process.env.CLICKHOUSE_PASSWORD || "",
  database: "default",
  session_id: sessionId,
});

async function createTables() {
  const createMainTableQuery = `
  CREATE TABLE IF NOT EXISTS transactions (
    id UInt32,
    user_id UInt32,
    amount Float32,
    currency String,
    timestamp DateTime('Europe/Madrid')
  ) ENGINE = MergeTree()
  ORDER BY id;
  `;

  const database = "default";
  const table = "transactions";
  const bufferLayers = 8;
  const minTimeSeconds = 10;
  const maxTimeSeconds = 60;
  const minRows = 10_000;
  const maxRows = 200_000;
  const minBytes = 600_000;
  const maxBytes = 1_000_000;
  const createBufferTableQuery = `
  CREATE TABLE IF NOT EXISTS transactions_buffer
  ENGINE = Buffer(${database}, ${table}, ${bufferLayers}, ${minTimeSeconds}, ${maxTimeSeconds}, ${minRows}, ${maxRows}, ${minBytes}, ${maxBytes});
  `;

  try {
    await client.command({ query: createMainTableQuery });
    console.log("Main table created successfully");

    await client.command({ query: createBufferTableQuery });
    console.log("Buffer table created successfully");
  } catch (error) {
    console.error("Error creating tables:", error);
  }
}

async function deleteBufferTable() {
  const dropBufferTableQuery = "DROP TABLE IF EXISTS transactions_buffer";

  try {
    await client.command({ query: dropBufferTableQuery });
    console.log("Buffer table deleted successfully");
  } catch (error) {
    console.error("Error deleting buffer table:", error);
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
        table: "transactions_buffer",
        values: batch,
        format: "JSONEachRow",
      });
      console.log(`${batch.length} transactions inserted successfully`);
    } catch (error) {
      console.error("Error inserting data:", error);
    }
  }
}

// increase max memory limit to 2GB
async function increaseMemoryLimit() {
  const query = "SET max_memory_usage = 2000000000";

  try {
    await client.command({
      query,
    });
    console.log("Memory limit increased successfully");
  } catch (error) {
    console.error("Error increasing memory limit:", error);
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
  await increaseMemoryLimit();
  //   await deleteBufferTable();
  await createTables();
  const totalCount = 100_000;
  const batchSize = 100;
  console.time("insertRandomTransactions");
  await insertRandomTransactions(totalCount, batchSize);
  console.timeEnd("insertRandomTransactions");
  await aggregateData();
}

main();
