import polars as pl
from utils.FastWorkflowFunctions import FastWorkflowFunctions

def fastworkflow_build(control_types, deltas_df, incoming_dict, outgoing_dict, timedeltas_dict):
    # with open(path, 'rb') as file:
    #     fastworkflow_df = pl.read_json(file)
    # else:
    fastworkflow_df = pl.DataFrame(schema=[('item',str),('control_type',str),('time_span',str),('mean',str)])
    
    sub_dict = {}
    for control_type in control_types:
        sub_deltas_df = deltas_df.filter(pl.col('Control_Type') == control_type)
        instance = FastWorkflowFunctions(sub_dict, control_type, sub_deltas_df, incoming_dict, outgoing_dict, timedeltas_dict)
        sub_dict = instance.run_fastworkflow_functions()

    for key in sub_dict.keys():
        # fastworkflow_df = pl.concat([fastworkflow_df_df, sub_deltas_df])
        fastworkflow_df.extend(pl.DataFrame(sub_dict[key]))
    
    with open('simulations/fastworkflow_df.json', 'wb') as file:
        fastworkflow_df.sort('item').write_json(file)


            

    