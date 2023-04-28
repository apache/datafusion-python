import argparse
from datafusion import SessionContext, RuntimeConfig, SessionConfig
import time

def bench(data_path, query_path):
    with open("results.csv", 'w') as results:
        # register tables
        start = time.time()

        # create context
        runtime = (
            RuntimeConfig()
            .with_disk_manager_os()
            .with_fair_spill_pool(10000000)
        )
        config = (
            SessionConfig()
            .with_create_default_catalog_and_schema(True)
            .with_default_catalog_and_schema("datafusion", "tpch")
            .with_information_schema(True)
        )
        ctx = SessionContext(config, runtime)

        # register tables
        with open("create_tables.sql") as f:
            text = f.read()
            tmp = text.split(';')
            for str in tmp:
                if len(str.strip()) > 0:
                    sql = str.strip().replace('$PATH', data_path)
                    #print(sql)
                    df = ctx.sql(sql)

        end = time.time()
        print("setup,{}".format(round((end-start)*1000,1)))
        results.write("setup,{}\n".format(round((end-start)*1000,1)))

        ctx.sql("SHOW TABLES").show()

        # run queries
        total_time_millis = 0
        for query in range(1, 23):
            with open("{}/q{}.sql".format(query_path, query)) as f:
                text = f.read()
                tmp = text.split(';')
                queries = []
                for str in tmp:
                    if len(str.strip()) > 0:
                        queries.append(str.strip())

                try:
                    start = time.time()
                    for sql in queries:
                        print(sql)
                        df = ctx.sql(sql)
                        # result_set = df.collect()
                        df.show()
                    end = time.time()
                    time_millis = (end - start) * 1000
                    total_time_millis += time_millis
                    print("q{},{}".format(query, round(time_millis,1)))
                    results.write("q{},{}\n".format(query, round(time_millis,1)))
                except Exception as e:
                    print("query", query, "failed", e)
        print("total,{}".format(round(total_time_millis,1)))
        results.write("total,{}\n".format(round(total_time_millis,1)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('data_path')
    parser.add_argument('query_path')
    args = parser.parse_args()
    bench(args.data_path, args.query_path)