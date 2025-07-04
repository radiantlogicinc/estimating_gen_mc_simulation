#### Intro
`fastWorkFlow_test` is a class designed to enable interfacing with fastWorkFlow. 
The goal of `fastWorkFlow_test` is to filter the polars DataFrame `test_df` for values corresponding only to the given parameters `control_type` and `time_span`.

#### Columns

The columns of `test_df` are:

- `test_df['item']` (`pl.String`): the value of interest (see **Rows**)
- `test_df['control_type']` (`pl.String`): the defect control type (see **Input parameters**)
- `test_df['time_span']` (`pl.Int64`): the time window for computing the mean (see **Input parameters**)
- `test_df['mean']` (`pl.Float64`): the mean of `test_df['item']` for the specified time window

#### Rows
The rows of `test_df['item']` correspond to the following values, computed as averages in `test_df['mean']` per control type in `test_df['control_type']` and time span in `test_df['time_span']`:

- `generated_per_day`: the number of defects generated per day on average
- `generated_per_hour`: the number of defects generated per hour on average
- `remediated_per_day`: the number of defects remediated per day on average
- `remediated_per_hour`: the number of defects remediated per hour on average
- `delta_new_assign`: the time between the status change new --> assign, i.e. the amount of time a defect is waiting in the backlog before being assigned to a remediation agent
- `delta_assign_inprogress`: the time between the status change assign --> inprogress, i.e. the amount of time an assigned defect waits before remediation work is begun by the remediation agent
- `delta_inprogress_closed`: the time between the status change inprogrss --> closed, i.e. the duration of the remediation work carried out by the remediation agent
- `delta_new_closed`: the total time elapsed between the statuses new and closed, i.e. the total lifetime of a defect

#### Input parameters
The possible choices for `control_type` are: 

``` 
["ACC03", "ACC17", "ACC28", "AUTH18", "AUTH42"] 
```


The possible choices for `time_span` are:

 ``` 
 [1, 2, 3] 
 ```

where:

- `1` indicates that the column `test_df['mean']` is computed for `test_df['item']` as a *daily snapshot*
- `2` indicates that the column `test_df['mean']` is computed for `test_df['item']` as a *quarterly snapshot*
- `3` indicates that the column `test_df['mean']` is computed for `test_df['item']` *for all time*


#### Methods

The method `fastWorkFlow_test.extractValues(control_type, time_span)` 
extracts only those rows from `info_df` containing `control_type` and `time_span`, returned as the json file `filtered_info.json`:

```
filtered_info_df = info_df.filter(pl.col('control_type') == control_type, pl.col('time_span') == time_span)
json
```

The method `fastWorkFlow_test.README` returns this README.md file.