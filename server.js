import express from "express";
import cors from "cors";
import mysql from "mysql2/promise";
import {
  DynamoDBClient,
  BatchGetItemCommand,
  QueryCommand,
  TransactWriteItemsCommand,
} from "@aws-sdk/client-dynamodb";

const TABLE = process.env.TABLE_NAME || "event-rsvp-counts";
const dynamo = new DynamoDBClient({ region: process.env.REGION });
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  connectionLimit: 5,
});

const app = express();
app.use(cors());
app.use(express.json());

const wrap = (fn) => (req, res) =>
  fn(req, res).catch((err) => {
    console.error("Error:", err);
    res.status(500).json({ error: err.message });
  });

app.get("/health", (req, res) => res.json({ ok: true }));

app.get("/events", wrap(async (req, res) => {
  const [rows] = await pool.execute(
    `SELECT event_id, title, description, start_at, venue, banner_url, created_at
     FROM events ORDER BY start_at ASC`
  );
  res.json(rows);
}));

app.get("/event/:event_id", wrap(async (req, res) => {
  const [rows] = await pool.execute(
    "SELECT * FROM events WHERE event_id = ?", [req.params.event_id]
  );
  if (rows.length === 0) return res.status(404).json({ message: "Event not found" });
  res.json(rows[0]);
}));

app.get("/stats/:event_id", wrap(async (req, res) => {
  const id = req.params.event_id;
  const keys = ["Yes", "No"].map((r) => ({
    pk: { S: `EVENT#${id}` },
    sk: { S: `RESPONSE#${r}` },
  }));
  const result = await dynamo.send(
    new BatchGetItemCommand({ RequestItems: { [TABLE]: { Keys: keys } } })
  );
  const counts = { Yes: 0, No: 0 };
  for (const item of result.Responses?.[TABLE] || []) {
    counts[item.sk.S.split("#")[1]] = Number(item.count?.N || 0);
  }
  res.json(counts);
}));

app.post("/rsvp", wrap(async (req, res) => {
  const { event_id, full_name, email, response } = req.body;
  if (!event_id || !full_name || !response || !email) {
    return res.status(400).json({
      message: "Missing fields. Email is required to prevent duplicate RSVPs.",
    });
  }
  try {
    await dynamo.send(new TransactWriteItemsCommand({
      TransactItems: [
        {
          Put: {
            TableName: TABLE,
            Item: {
              pk: { S: `EVENT#${event_id}` },
              sk: { S: `RESPONDENT#${email}` },
              full_name: { S: full_name },
              email: { S: email },
              response: { S: response },
              timestamp: { N: String(Date.now()) },
            },
            ConditionExpression: "attribute_not_exists(pk) AND attribute_not_exists(sk)",
          },
        },
        {
          Update: {
            TableName: TABLE,
            Key: { pk: { S: `EVENT#${event_id}` }, sk: { S: `RESPONSE#${response}` } },
            UpdateExpression: "ADD #count :one",
            ExpressionAttributeNames: { "#count": "count" },
            ExpressionAttributeValues: { ":one": { N: "1" } },
          },
        },
      ],
    }));
    res.json({ message: "RSVP recorded!" });
  } catch (err) {
    if (err.name === "TransactionCanceledException" || err.name === "ConditionalCheckFailedException") {
      return res.status(409).json({
        message: "You have already RSVP'd for this event with this email!",
        code: "DUPLICATE_RSVP",
      });
    }
    throw err;
  }
}));

app.get("/attendees/:event_id", wrap(async (req, res) => {
  const result = await dynamo.send(new QueryCommand({
    TableName: TABLE,
    KeyConditionExpression: "pk = :pk AND begins_with(sk, :prefix)",
    ExpressionAttributeValues: {
      ":pk": { S: `EVENT#${req.params.event_id}` },
      ":prefix": { S: "RESPONDENT#" },
    },
  }));
  let attendees = result.Items.map((i) => ({
    full_name: i.full_name?.S,
    email: i.email?.S,
    response: i.response?.S,
    timestamp: parseInt(i.timestamp?.N),
  }));
  if (req.query.response) {
    attendees = attendees.filter((a) => a.response === req.query.response);
  }
  res.json(attendees);
}));

app.use((req, res) => res.status(404).json({ message: "Route not found" }));

app.listen(process.env.PORT || 8080, "0.0.0.0", () =>
  console.log("Listening on", process.env.PORT || 8080)
);