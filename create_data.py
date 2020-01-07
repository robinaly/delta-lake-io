#!/usr/bin/env python
import pandas
import numpy

def large():
    for nrow in map(int, [1E3, 1E4, 1E5, 1E6, 1E7]):
        print(nrow)
        df = pandas.DataFrame({
            'e1': numpy.random.rand(nrow), # random float,
            'a': 1, # constant
            'e2': numpy.random.rand(nrow),  # random float
            'b': 2, # constant
            'e3': numpy.random.rand(nrow),  # random float
            'c': numpy.random.choice(5, nrow), # random int 0 <= x < 5
            'd': numpy.random.choice(['house', 'tree', 'road', 'mouse', 'cat'], nrow), # random string
            },
            columns=['e1', 'a', 'e2', 'b', 'e3', 'c', 'd']).sort_values(['a', 'b', 'c', 'd', 'e1', 'e2', 'e3'])
        df.to_parquet('data/benchmark_{}.parequet'.format(nrow), compression='snappy', index=False, use_dictionary=True);
        df.to_csv('data/benchmark_{}.csv.gz'.format(nrow), index=False);

def main():
    nrow = 50
    numpy.random.seed(1234)
    customer_id = numpy.random.randint(1, 20, nrow // 2).cumsum()
    numpy.random.seed(None)
    new_customer_ids = numpy.random.randint(1, 20, nrow // 2).cumsum() +  customer_id.max()
    customer_id = numpy.concatenate((customer_id, new_customer_ids), axis=0)
    df = pandas.DataFrame({
        'customer_id': customer_id,  # random, monotonically increasing id,
        'name': numpy.random.choice(['house', 'tree', 'road', 'mouse', 'cat'], nrow),  # random string
        })
    df.to_csv('data/customers-update.csv', index=False, sep='^');



if __name__ == "__main__":
    main()