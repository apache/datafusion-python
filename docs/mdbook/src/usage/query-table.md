# DataFusion Query Table

DataFusion tables can be queried with SQL or with the Python API.

Let's create a small table and show the different types of queries that can be run.

```python
df = ctx.from_pydict(
    {
        "first_name": ["li", "wang", "ron", "amanda"],
        "age": [25, 75, 68, 18],
        "country": ["china", "china", "us", "us"],
    },
    name="some_people",
)
```

Here's the data in the table:

```
+------------+-----+---------+
| first_name | age | country |
+------------+-----+---------+
| li         | 25  | china   |
| wang       | 75  | china   |
| ron        | 68  | us      |
| amanda     | 18  | us      |
+------------+-----+---------+
```

## DataFusion Filter DataFrame

Here's how to find all individuals that are older than 65 years old in the data with SQL:

```
ctx.sql("select * from some_people where age > 65")

+------------+-----+---------+
| first_name | age | country |
+------------+-----+---------+
| wang       | 75  | china   |
| ron        | 68  | us      |
+------------+-----+---------+
```

Here's how to run the same query with Python:

```python
df.filter(col("age") > lit(65))
```

```
+------------+-----+---------+
| first_name | age | country |
+------------+-----+---------+
| wang       | 75  | china   |
| ron        | 68  | us      |
+------------+-----+---------+
```

## DataFusion Select Columns from DataFrame

Here's how to select the `first_name` and `country` columns from the DataFrame with SQL:

```
ctx.sql("select first_name, country from some_people")


+------------+---------+
| first_name | country |
+------------+---------+
| li         | china   |
| wang       | china   |
| ron        | us      |
| amanda     | us      |
+------------+---------+
```

Here's how to run the same query with Python:

```python
df.select(col("first_name"), col("country"))
```

```
+------------+---------+
| first_name | country |
+------------+---------+
| li         | china   |
| wang       | china   |
| ron        | us      |
| amanda     | us      |
+------------+---------+
```

## DataFusion Aggregation Query

Here's how to run a group by aggregation query:

```
ctx.sql("select country, count(*) as num_people from some_people group by country")

+---------+------------+
| country | num_people |
+---------+------------+
| china   | 2          |
| us      | 2          |
+---------+------------+
```
