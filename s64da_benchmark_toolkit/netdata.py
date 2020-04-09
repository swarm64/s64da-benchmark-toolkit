
import logging

import requests
import pandas


LOG = logging.getLogger()

class Netdata:
    def __init__(self, config):
        self.url = f"{config['url']}/api/v1/data"
        self.metrics = config['metrics']
        self.charts = config['charts']

    def _get_data(self, timerange):
        data = pandas.DataFrame()
        for chart, dimensions in self.charts.items():
            result = requests.get(self.url, params={
                'chart': chart,
                'after': timerange[0],
                'before': timerange[1],
                'dimensions': ','.join(dimensions)
            }).json()

            columns = ['time']
            columns.extend([f'{chart}.{dimension}' for dimension in dimensions])
            columns = [column.replace('.', '_') for column in columns]

            df = pandas.DataFrame(result['data'], columns=columns)
            df = df.set_index('time')
            data = pandas.concat([data, df], axis=1)

        return data

    def write_stats(self, results, output):
        data = {}
        for name, result in results.items():
            # timerange = (int(result.start), int(result.stop))
            timerange = (result[0], result[1])
            df = self._get_data(timerange)
            data[name] = df.agg(self.metrics)

        with open(output, 'w') as output_file:
            for name, df in data.items():
                output_file.write(f'{name}')
                df.to_csv(output_file)
                output_file.write('\n')
