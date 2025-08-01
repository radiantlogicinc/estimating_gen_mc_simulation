import polars as pl

class ControlType:
    """Class for question-answering on defect remediation data"""

    def __init__(self, control_type: str):
        self.control_type = control_type
        
        info_df = pl.read_json('defect_remediation_app_base/application/fastworkflow_df.json', schema={"item":pl.String, "control_type":pl.String, "time_span":pl.String, "mean":pl.String})
        # info_df = info_df.with_columns(pl.col('time_span').cast(pl.Int64).alias('time_span'))
        # info_df = info_df.with_columns(pl.col('mean').cast(pl.Float64).alias('mean'))
        self.info_df = info_df.filter(pl.col("control_type") == self.control_type)
        
        pl.Config.set_tbl_hide_dataframe_shape(True)

    def generated_per_day(self, time_span: str):
        """
        Mean # of defects of certain control type being generated per day
        Args: time_span (str): time span over which to compute average: "day", "month", "all_time"
        """
        value = self.info_df.filter(
            pl.col('item') == f"generated_per_day_{self.control_type}_{time_span}",
            pl.col('time_span') == time_span
            )["mean"][0]
        if time_span == "day":
            if value == "None":
                print(f"In the last day, no {self.control_type} defects were generated.")
            else:
                print(f"In the last day, {value.lower()} {self.control_type} defects were generated.")
        elif time_span == "month":
            if value == "None":
                print(f"In the last month, no {self.control_type} defects were generated.")
            else:
                print(f"In the last month, {value.lower()} {self.control_type} defects were generated daily on average.")
        elif time_span == "all_time":
            if value == "None":
                print(f"No {self.control_type} defects have ever been generated.")
            else:
                print(f"On average, {value.lower()} {self.control_type} defects are generated daily.")

    def remediated_per_day(self, time_span: str):
        """
        Mean # of defects of certain control type being remediated per day
        Args: time_span (str): time span over which to compute average: "day", "month", "all_time"
        """
        value = self.info_df.filter(
            pl.col('item') == f"remediated_per_day_{self.control_type}_{time_span}",
            pl.col('time_span') == time_span
            )["mean"][0]
        
        if time_span == "day":
            if value == "None":
                print(f"In the last day, no {self.control_type} defects were remediated.")
            else:
                print(f"In the last day, {value.lower()} {self.control_type} defects were remediated.")
        elif time_span == "month":
            if value == "None":
                print(f"In the last month, no {self.control_type} defects were remediated.")
            else:
                print(f"In the last month, {value.lower()} {self.control_type} defects were remediated daily on average.")
        elif time_span == "all_time":
            if value == "None":
                print(f"No {self.control_type} defects have ever been remediated.")
            else:
                print(f"On average, {value.lower()} {self.control_type} defects are remediated daily.")

    def waiting_in_backlog(self, time_span: str):
        "Mean waiting time of defects of certain control type in backlog before beginning remediation"
        value = str(float(self.info_df.filter(
            pl.col('item') == f"delta_new_assign_{self.control_type}_{time_span}",
            pl.col('time_span') == time_span
            )["mean"][0]) + float(self.info_df.filter(
            pl.col('item') == f"delta_assign_inprogress_{self.control_type}_{time_span}",
            pl.col('time_span') == time_span
            )["mean"][0]))
        if time_span == "day":
            if value == "None":
                print(f"In the last day, no {self.control_type} defects were waiting in the backlog before beginning remediation.")
            else:
                print(f"In the last day, {self.control_type} defects waited an average of {value} hours in the backlog before beginning remediation.")
        elif time_span == "month":
            if value == "None":
                print(f"In the last month, no {self.control_type} defects were waiting in the backlog before beginning remediation.")
            else:
                print(f"In the last month, {self.control_type} defects waited an average of {value} hours in the backlog before beginning remediation.")
        elif time_span == "all_time":
            if value == "None":
                print(f"No {self.control_type} defects have ever been in the backlog.")
            else:
                print(f"On average, {self.control_type} defects wait {value} hours in the backlog before beginning remediation.")
            
    def total_remediation_time(self, time_span: str):
        "Mean total remediation time of defects of certain control type"
        value = self.info_df.filter(
            pl.col('item') == f"delta_new_closed_{self.control_type}_{time_span}",
            pl.col('time_span') == time_span
            )["mean"][0]
        if time_span == "day":
            if value == "None":
                print(f"In the last day, no {self.control_type} defects were remediated.")
            else:
                print(f"In the last day, {self.control_type} defects needed an average of {value} hours to be remediated.")
        elif time_span == "month":
            if value == "None":
                print(f"In the last month, no {self.control_type} defects were remediated.")
            else:
                print(f"In the last month, {self.control_type} defects needed an average of {value} hours to be remediated.")
        elif time_span == "all_time":
            if value == "None":
                print(f"No {self.control_type} defects have ever been remediated.")
            else:
                print(f"On average, {self.control_type} defects need {value} hours to be remediated.")