const { createClient } = require("@clickhouse/client");

const client = createClient({
  url: process.env.CLICKHOUSE_URL || "http://localhost:8123",
  username: process.env.CLICKHOUSE_USER || "default",
  password: process.env.CLICKHOUSE_PASSWORD || "",
  database: "default",
});

async function testConnection() {
  try {
    const result = await client.query({
      query: "SELECT version()",
      format: "JSON",
    });
    console.log("ClickHouse version:", await result.json());
  } catch (error) {
    console.error("Error executing query:", error);
  }
}

async function createTable() {
  const createTableQuery = `
  CREATE TABLE IF NOT EXISTS test_table (
    id UInt32,
    name String,
    age UInt8
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

async function insertData() {
  const data = [
    { id: 1, name: "John Doe", age: 30 },
    { id: 2, name: "Jane Smith", age: 25 },
    { id: 3, name: "Alice Johnson", age: 28 },
  ];

  try {
    await client.insert({
      table: "test_table",
      values: data,
      format: "JSONEachRow",
    });
    console.log("Data inserted successfully");
  } catch (error) {
    console.error("Error inserting data:", error);
  }
}

async function selectData() {
  const selectQuery = "SELECT * FROM test_table";

  try {
    const result = await client.query({
      query: selectQuery,
      format: "JSON",
    });
    const rows = await result.json();
    console.log("Data selected:", rows);
  } catch (error) {
    console.error("Error selecting data:", error);
  }
}

(async () => {
  await testConnection();
  await createTable();
  await insertData();
  await selectData();
})();
