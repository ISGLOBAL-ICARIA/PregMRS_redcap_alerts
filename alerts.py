from datetime import datetime
import pandas as pd
import params

def pregmrs_alert(project):
    """
    :param project_key: Project ID
    :return:
    """
    print("\n[{}] Getting records from the PREG-MRS REDCap project:".format(datetime.now()))

    df = project.export_records(format='df', fields=params.LOGIC_FIELDS)
    newborns = df[df['newborn_date'].notna()]
    postpartum_woman = df[df['pmrs_study_group']==2]

    df_records = postpartum_woman.index.get_level_values('record_id').difference(newborns.index.get_level_values('record_id'))
    #print(df_records)
    #print(postpartum_woman)

    ppw_res = postpartum_woman.reset_index()
    to_be_alert = ppw_res[ppw_res['record_id'].isin(df_records)][['record_id','study_number','pmrs_date']]

    to_be_alert[['pmrs_date']] = to_be_alert[['pmrs_date']].apply(pd.to_datetime)
    try:
        alerts1 = to_be_alert[((datetime.today() - to_be_alert['pmrs_date']).dt.days <=4)]
    except:
        alerts1 = pd.DataFrame(columns=['record_id','study_number','pmrs_date'])
    try:
        alerts2 = to_be_alert[((datetime.today() - to_be_alert['pmrs_date']).dt.days <=9)&((datetime.today() - to_be_alert['pmrs_date']).dt.days >4)]
    except:
        alerts2 = pd.DataFrame(columns=['record_id','study_number','pmrs_date'])
    try:
        alerts3 = to_be_alert[((datetime.today() - to_be_alert['pmrs_date']).dt.days >9)]
    except:
        alerts3 = pd.DataFrame(columns=['record_id','study_number','pmrs_date'])

    all_records = df.index.get_level_values('record_id')
    print(all_records)
    completed_records = all_records.difference(alerts1['record_id'])
    completed_records = completed_records.difference(alerts2['record_id'])
    completed_records = completed_records.difference(alerts3['record_id'])

    to_import_df = build_pregmrs_alert(df,completed_records,alerts1['record_id'].values,alerts2['record_id'].values,alerts3['record_id'].values)
    to_import_dict = [{'record_id': rec_id, 'fu_status': participant.fu_status}
                      for rec_id, participant in to_import_df.iterrows()]
    #print(to_import_dict)
    response = project.import_records((to_import_dict))
    print("[PREG-MRS] Alerts setup: {}".format(response.get('count')))

def build_pregmrs_alert(df, completed_records,alerts1, alerts2, alerts3):

    dfres = df.reset_index()
    data_to_import = pd.DataFrame(columns=['fu_status'])

    for el in completed_records:
        print(el)
        sn = dfres[(dfres['record_id']==el)&(dfres['redcap_event_name']=='pregmrs_arm_1')]['study_number'].values[0]
        ty = dfres[(dfres['record_id']==el)&(dfres['redcap_event_name']=='pregmrs_arm_1')]['pmrs_study_group'].values[0]
        print(sn,ty)
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
    #print(data_to_import)
    return data_to_import
