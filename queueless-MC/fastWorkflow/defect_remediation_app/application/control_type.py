import polars as pl

class ControlType:
    """Class for question-answering on defect remediation data"""

    def __init__(self, control_type: str):
        self.control_type = control_type
        
        # info_df = pl.read_json('fastWorkFlow_test_v2.json', schema={"item":pl.String, "control_type":pl.String, "time_span":pl.String, "mean":pl.String})
        # # info_df = info_df.with_columns(pl.col('time_span').cast(pl.Int64).alias('time_span'))
        # info_df = info_df.with_columns(pl.col('mean').cast(pl.Float64).alias('mean'))
        # self.info_df = info_df.filter(pl.col("control_type") == self.control_type)

    def generated_per_day(self, time_span: str):
        """
        Mean # of defects of certain control type being generated per day
        Args: time_span (str): "1" -> daily, "2" -> quarterly, "3" -> all time
        """
        print(time_span)
        info_df = pl.read_json('fastWorkFlow_test_v2.json', schema={"item":pl.String, "control_type":pl.String, "time_span":pl.String, "mean":pl.String})
        # info_df = info_df.with_columns(pl.col('time_span').cast(pl.Int64).alias('time_span'))
        info_df = info_df.with_columns(pl.col('mean').cast(pl.Float64).alias('mean'))
        info_df = info_df.filter(pl.col("control_type") == self.control_type)

        value = info_df.filter(
            pl.col('time_span') == time_span
            )["mean"]
        if time_span == "1":
            print(f"In the last day, {value} {self.control_type} defects were generated.")
        elif time_span == "2":
            print(f"In the last quarter, {value} {self.control_type} defects were generated daily on average.")
        elif time_span == "3":
            print(f"On average, {value} {self.control_type} defects are generated daily.")

    def waiting_in_backlog(self, time_span: str):
        "Mean waiting time of defects of certain control type in backlog before beginning remediation"
        info_df = pl.read_json('fastWorkFlow_test_v2.json', schema={"item":pl.String, "control_type":pl.String, "time_span":pl.String, "mean":pl.String})
        # info_df = info_df.with_columns(pl.col('time_span').cast(pl.Int64).alias('time_span'))
        info_df = info_df.with_columns(pl.col('mean').cast(pl.Float64).alias('mean'))
        info_df = info_df.filter(pl.col("control_type") == self.control_type)
        value = info_df.filter(
            pl.col('time_span') == time_span
            )["mean"]
        if time_span == "1":
            print(f"""In the last day, {self.control_type} defects waited an average of {value} 
                  hours in the backlog before beginning remediation.""")
        elif time_span == "2":
            print(f"""In the last quarter, {self.control_type} defects waited an average of {value} 
                  hours in the backlog before beginning remediation.""")
        elif time_span == "3":
            print(f"""On average, {self.control_type} defects wait {value} 
                  hours in the backlog before beginning remediation.""")