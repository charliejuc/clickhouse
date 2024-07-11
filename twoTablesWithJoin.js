const { createClient } = require("@clickhouse/client");
const { faker } = require("@faker-js/faker");

const client = createClient({
  url: process.env.CLICKHOUSE_URL || "http://localhost:8123",
  username: process.env.CLICKHOUSE_USER || "default",
  password: process.env.CLICKHOUSE_PASSWORD || "",
  database: "default",
});

async function createTransactionsTable() {
  const createTableQuery = `
  CREATE TABLE IF NOT EXISTS transactions (
    id UInt32,
    user_id UInt32,
    productId UUID,
    amount Decimal(18, 4),
    currency String,
    timestamp DateTime('Europe/Madrid')
  ) ENGINE = MergeTree()
  ORDER BY id;
  `;

  try {
    await client.command({
      query: createTableQuery,
    });
    console.log("Transactions table created successfully");
  } catch (error) {
    console.error("Error creating transactions table:", error);
  }
}

async function createProductsTable() {
  const createProductsTableQuery = `
  CREATE TABLE IF NOT EXISTS products (
    id UUID,
    productName String,
    productCategory String
  ) ENGINE = MergeTree()
  ORDER BY id;
  `;

  try {
    await client.command({
      query: createProductsTableQuery,
    });
    console.log("Products table created successfully");
  } catch (error) {
    console.error("Error creating products table:", error);
  }
}

async function insertRandomProducts(totalCount) {
  const batch = [];
  for (let i = 0; i < totalCount; i++) {
    batch.push({
      id: faker.string.uuid(),
      productName: faker.commerce.productName(),
      productCategory: faker.commerce.department(),
    });
  }

  try {
    await client.insert({
      table: "products",
      values: batch,
      format: "JSONEachRow",
    });
    console.log(`${batch.length} products inserted successfully`);
  } catch (error) {
    console.error("Error inserting products:", error);
  }
}

async function getAllProducts() {
  try {
    const result = await client.query({
      query: "SELECT id FROM products",
      format: "JSON",
    });
    return await result.json();
  } catch (error) {
    console.error("Error fetching products:", error);
    return [];
  }
}

async function insertRandomTransactions(totalCount, batchSize, _products) {
  const products = _products ?? (await getAllProducts());
  const productIds = products.data.map((product) => product.id);

  for (let start = 0; start < totalCount; start += batchSize) {
    const batch = [];
    for (let i = start; i < Math.min(start + batchSize, totalCount); i++) {
      const timestamp = faker.date
        .between({ from: "2022-01-01T00:00:00Z", to: "2022-12-31T23:59:59Z" })
        .toISOString();
      batch.push({
        id: i,
        user_id: faker.number.int({ min: 1, max: 1000 }),
        productId: faker.helpers.arrayElement(productIds),
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

async function createMaterializedViewTargetTable() {
  const createTargetTableQuery = `
  CREATE TABLE IF NOT EXISTS transactions_products_summary (
   productName String,
   productCategory String,
   productId UUID,
   total_amount Decimal(18, 4)
  ) ENGINE = SummingMergeTree()
  PRIMARY KEY (productId)
  ORDER BY (productId);
  `;

  try {
    await client.command({ query: createTargetTableQuery });
    console.log("Target table for materialized view created successfully");
  } catch (error) {
    console.error("Error creating target table:", error);
  }
}

async function createMaterializedView() {
  const createMVQuery = `
  CREATE MATERIALIZED VIEW IF NOT EXISTS mv_transactions_products
  TO transactions_products_summary
  AS
  SELECT
    p.productName as productName,
    p.productCategory as productCategory,
    t.productId as productId,
    SUM(t.amount) AS total_amount
  FROM transactions as t
  LEFT JOIN products as p
  ON t.productId = p.id
  GROUP BY t.productId, p.productName, p.productCategory;
  `;

  try {
    await client.command({ query: createMVQuery });
    console.log("Materialized view created successfully");
  } catch (error) {
    console.error("Error creating materialized view:", error);
  }
}

async function queryWithJoin() {
  const joinQuery = `
  SELECT 
    productId,
    productCategory,
    productName,
    // total_amount
    SUM(total_amount) as total_amount
  FROM transactions_products_summary
  GROUP BY productId, productCategory, productName
  LIMIT 100;
  `;

  try {
    const result = await client.query({
      query: joinQuery,
      format: "JSON",
    });
    const rows = await result.json();
    console.log("Join query result:", rows);
  } catch (error) {
    console.error("Error executing join query:", error);
  }
}

async function dropTables() {
  const dropTableQueries = [
    "DROP TABLE IF EXISTS transactions",
    "DROP TABLE IF EXISTS products",
    "DROP TABLE IF EXISTS transactions_products_summary",
    "DROP TABLE IF EXISTS mv_transactions_products",
  ];

  for (const query of dropTableQueries) {
    try {
      await client.command({ query });
      console.log(`Executed: ${query}`);
    } catch (error) {
      console.error(`Error executing: ${query}`, error);
    }
  }
}

const product = {
  id: "f4e5c1d3-9b8b-4f0b-8e9f-9b1a214e5c1d",
  productName: "Ergonomic Rubber Shirt",
  productCategory: "Books",
};
const productsData = {
  data: [product],
};

async function transactionsFirst() {
  await dropTables();
  await createTransactionsTable();
  await createProductsTable();

  await createMaterializedViewTargetTable();
  await createMaterializedView();

  const totalTransactions = 1_000;
  const batchSize = 500_000;

  await insertRandomTransactions(totalTransactions, batchSize, productsData);

  // insert products after transactions
  try {
    await client.insert({
      table: "products",
      values: productsData.data,
      format: "JSONEachRow",
    });
    console.log("Products inserted successfully");
  } catch (error) {
    console.error("Error inserting products:", error);
  }

  await queryWithJoin();
}

async function normal() {
  await dropTables();
  await createTransactionsTable();
  await createProductsTable();

  await createMaterializedViewTargetTable();
  await createMaterializedView();

  const totalTransactions = 1_000;
  const totalProducts = 2;
  const batchsize = 500_000;

  await insertRandomProducts(totalProducts);
  await insertRandomTransactions(totalTransactions, batchsize);

  await queryWithJoin();
}

async function main() {
  // await normal();
  await transactionsFirst();
}

main();
