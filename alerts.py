from datetime import datetime, date
from datetime import timedelta

import pandas as pd
from pandas import IndexSlice as idx

from dateutil.relativedelta import relativedelta
import math
import numpy as np
import pandas
import redcap
import params
import tokens


def pregmrs_alert(project_key):
    project = redcap.Project(tokens.URL, tokens.PREGMRS_REDCAP_PROJECTS[project_key])

    # Get all records for each ICARIA REDCap project (TRIAL)
    print("\n[{}] Getting records from the ICARIA TRIAL REDCap projects:".format(datetime.now()))
    print("[{}] Getting all records from {}...".format(datetime.now(),project_key))

    df = project.export_records(format='df', fields=params.LOGIC_FIELDS)
    print(df)

    newborns = df[df['newborn_date'].notna()]
    postpartum_woman = df[df['pmrs_study_group']==2]

    df_records = postpartum_woman.index.get_level_values('record_id').difference(newborns.index.get_level_values('record_id'))
    print(df_records)
    print(postpartum_woman)

    ppw_res = postpartum_woman.reset_index()
    to_be_alert = ppw_res[ppw_res['record_id'].isin(df_records)][['record_id','study_number','pmrs_date']]

    to_be_alert[['pmrs_date']] = to_be_alert[['pmrs_date']].apply(pd.to_datetime)

    alerts1 = to_be_alert[((datetime.today() - to_be_alert['pmrs_date']).dt.days <=4)]
    alerts2 = to_be_alert[((datetime.today() - to_be_alert['pmrs_date']).dt.days <=9)&((datetime.today() - to_be_alert['pmrs_date']).dt.days >4)]
    alerts3 = to_be_alert[((datetime.today() - to_be_alert['pmrs_date']).dt.days >9)]

    all_records = df.index.get_level_values('record_id')
    completed_records = all_records.difference(alerts1['record_id'])
    completed_records = completed_records.difference(alerts2['record_id'])
    completed_records = completed_records.difference(alerts3['record_id'])

    to_import_dict = []
    to_import_df = build_pregmrs_alert(df,completed_records,alerts1['record_id'].values,alerts2['record_id'].values,alerts3['record_id'].values,to_import_dict)
    to_import_dict = [{'record_id': rec_id, 'fu_status': participant.fu_status}
                      for rec_id, participant in to_import_df.iterrows()]
    print(to_import_dict)
    response = project.import_records((to_import_dict))
    print("[PREG-MRS] Alerts setup: {}".format(response.get('count')))

def build_pregmrs_alert(df, completed_records,alerts1, alerts2, alerts3,to_import_dict):

    dfres = df.reset_index()
    data_to_import = pandas.DataFrame(columns=['fu_status'])

    for el in completed_records:
        sn = dfres[(dfres['record_id']==el)&(dfres['redcap_event_name']=='pregmrs_arm_1')]['study_number'].values[0]
        ty = dfres[(dfres['record_id']==el)&(dfres['redcap_event_name']=='pregmrs_arm_1')]['pmrs_study_group'].values[0]
        type = params.type_dict[str(ty)]

        if type == 'PPM':
            nb_date = dfres[(dfres['record_id'] == el) & (dfres['redcap_event_name'] == 'newborn_arm_1')]['newborn_date'].values[0]
            if nb_date != '':
                status = '- COMPLETED'
            else:
                status = ''
        else:
            status = '- COMPLETED'
        final_status = "["+str(sn)+"] "+str(type)+ " "+ status
        data_to_import.loc[el] = final_status
        to_import_dict.append({'record_id': el, 'fu_status':final_status})

    for el in alerts1:
        sn = dfres[(dfres['record_id']==el)&(dfres['redcap_event_name']=='pregmrs_arm_1')]['study_number'].values[0]
        ty = dfres[(dfres['record_id']==el)&(dfres['redcap_event_name']=='pregmrs_arm_1')]['pmrs_study_group'].values[0]
        type = params.type_dict[str(ty)]

        if type == 'PPM':
            status = '- APPOINTMENT'
        else:
            print('WHAT?')
            status = '???'
        final_status = "["+str(sn)+"] "+str(type)+ " "+ status
        data_to_import.loc[el] = final_status
        to_import_dict.append({'record_id': el, 'fu_status':final_status})

    for el in alerts2:
        sn = dfres[(dfres['record_id']==el)&(dfres['redcap_event_name']=='pregmrs_arm_1')]['study_number'].values[0]
        ty = dfres[(dfres['record_id']==el)&(dfres['redcap_event_name']=='pregmrs_arm_1')]['pmrs_study_group'].values[0]
        type = params.type_dict[str(ty)]

        if type == 'PPM':
            status = '- TO BE VISITED'
        else:
            print('WHAT?')
            status = '???'
        final_status = "["+str(sn)+"] "+str(type)+ " "+ status
        data_to_import.loc[el] = final_status
        to_import_dict.append({'record_id': el, 'fu_status':final_status})

    for el in alerts3:
        sn = dfres[(dfres['record_id']==el)&(dfres['redcap_event_name']=='pregmrs_arm_1')]['study_number'].values[0]
        ty = dfres[(dfres['record_id']==el)&(dfres['redcap_event_name']=='pregmrs_arm_1')]['pmrs_study_group'].values[0]
        type = params.type_dict[str(ty)]

        if type == 'PPM':
            status = '- UNREACH'
        else:
            print('WHAT?')
            status = '???'
        final_status = "["+str(sn)+"] "+str(type)+ " "+ status
        data_to_import.loc[el] = final_status
        to_import_dict.append({'record_id': el, 'fu_status':final_status})
    print(data_to_import)
    return data_to_import
    return to_import_dict
