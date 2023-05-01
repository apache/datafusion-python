# DataFusion Book

DataFusion is a blazing fast query engine that lets you run data analyses quickly and reliably.

DataFusion is written in Rust, but also exposes Python and SQL bindings, so you can easily query data in your langauge of choice.  You don't need to know any Rust to be a happy and productive user of DataFusion.

DataFusion lets you run queries faster than pandas.  Let's compare query runtimes for a 5GB CSV file with 100 million rows of data.

Take a look at a few rows of the data:

```
+-------+-------+--------------+-----+-----+-------+----+----+-----------+
| id1   | id2   | id3          | id4 | id5 | id6   | v1 | v2 | v3        |
+-------+-------+--------------+-----+-----+-------+----+----+-----------+
| id016 | id016 | id0000042202 | 15  | 24  | 5971  | 5  | 11 | 37.211254 |
| id039 | id045 | id0000029558 | 40  | 49  | 39457 | 5  | 4  | 48.951141 |
| id047 | id023 | id0000071286 | 68  | 20  | 74463 | 2  | 14 | 60.469241 |
+-------+-------+--------------+-----+-----+-------+----+----+-----------+
```

Suppose you'd like to run the following query: `SELECT id1, sum(v1) AS v1 from the_table GROUP BY id1`.

If you use pandas, then this query will take 43.6 seconds to execute.

It only takes DataFusion 9.8 seconds to execute the same query.

DataFusion is easy to use, powerful, and fast.  Let's learn more!
