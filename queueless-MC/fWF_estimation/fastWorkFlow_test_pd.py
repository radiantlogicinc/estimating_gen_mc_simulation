import pandas as pd
import markdown
import argparse
from IPython.display import display, Markdown
# from rich.console import Console
# from rich.markdown import Markdown

class fastWorkFlow_test:
    def __init__(self, info_df):
        self.info_df = info_df

    def extractValues(self, control_type, time_span):
        filtered_info_df = self.info_df.query('(control_type == @control_type) and (time_span == @time_span)')
        # filtered_info_df = self.info_df.filter(pl.col('control_type') == control_type, pl.col('time_span') == time_span)
        return filtered_info_df
    
    def README(self):
        # console = Console()
        # with open('fastWorkFlow_test.md') as f:
        #     readme = Markdown( f.read() )
        #     console.print(readme)
        f = open('fastWorkFlow_test.md', 'r')
        htmlmarkdown=markdown.markdown( f.read() )
        display(Markdown(htmlmarkdown))


if __name__ == "__main__":
    ## READ IN DATABASE + PRINT README ##
    test_df = pd.read_json('fastWorkFlow_test_v2.json')
    # test_df = pl.read_json('fastWorkFlow_test_v2.json', schema={"item":pl.String, "control_type":pl.String, "time_span":pl.String, "mean":pl.String})
    # test_df = test_df.with_columns(pl.col('time_span').cast(pl.Int64).alias('time_span'))
    # test_df = test_df.with_columns(pl.col('mean').cast(pl.Float64).alias('mean'))
    instance = fastWorkFlow_test(test_df)
    instance.README()
    # print(readme)

    ## INPUT PARAMETERS ##
    # parser = argparse.ArgumentParser(exit_on_error=False)
    # parser.add_argument('--control_type', type=str, help="Defect type, e.g. ACC03")
    # parser.add_argument('--time_span', type=int, default=1, help="Choose time window for interpreting data: 1 -> daily, 2 -> quarterly, 3 -> all time")
    
    # args = parser.parse_args()
    control_type = input('Select control type from: ["ACC03", "ACC17", "ACC28", "AUTH18", "AUTH42"]')
    time_span = int(input('Select time window from: [1, 2, 3]'))

    filtered_info_df = instance.extractValues(control_type, time_span)
    print(filtered_info_df)