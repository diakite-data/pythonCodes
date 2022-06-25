import pandas as pd


prod_simbox_profil = pd.read_csv("/Users/youssouf/Downloads/prod_simbox_profil.csv", sep = ";")
prod_simbox_profil = prod_simbox_profil.rename(columns={'prod_simbox_profil.date_id': 'date_id', 'prod_simbox_profil.msisdn': 'msisdn', 'prod_simbox_profil.nbr_dist_profil': 'nbr_dist_profil' } )
prod_simbox_profil['date_id'] = pd.to_datetime(prod_simbox_profil['date_id'])
prod_simbox_profil['date_id'] = prod_simbox_profil['date_id'].dt.strftime('%Y%m%d')
prod_simbox_profil['date_id'] = prod_simbox_profil['date_id'].apply(str)
prod_simbox_profil['msisdn'] = prod_simbox_profil['msisdn'].apply(str)
prod_simbox_profil.head()


prod_imsi = pd.read_csv("/Users/youssouf/Downloads/prod_imsi.csv", sep = ";")
prod_imsi = prod_imsi.rename(columns={'prod_imsi.nbre_imsi': 'nbre_imsi', 'prod_imsi.msisdn': 'msisdn'} )
prod_imsi['msisdn'] = prod_imsi['msisdn'].apply(str)
prod_imsi.head()


prod_simbox_imei = pd.read_csv("/Users/youssouf/Downloads/prod_simbox_imei.csv", sep = ";")
prod_simbox_imei = prod_simbox_imei.rename(columns={'prod_simbox_imei.date_id': 'date_id', 'prod_simbox_imei.msisdn': 'msisdn', 'prod_simbox_imei.nbre_distinct_imei': 'nbre_distinct_imei'   } )
prod_simbox_imei['msisdn'] = prod_simbox_imei['msisdn'].apply(str)
prod_simbox_imei.head()


prod_simbox_statut_om_acti = pd.read_csv("/Users/youssouf/Downloads/prod_simbox_statut_om_acti.csv", sep = ";")
prod_simbox_statut_om_acti = prod_simbox_statut_om_acti.rename(columns={'prod_simbox_statut_om_acti.date_id': 'date_id', 'prod_simbox_statut_om_acti.msisdn': 'msisdn', 'prod_simbox_statut_om_acti.statut_om': 'statut_om', 'prod_simbox_statut_om_acti.statut_activite': 'statut_activite', 'prod_simbox_statut_om_acti.profil' : 'profil'  } )
prod_simbox_statut_om_acti['date_id'] = pd.to_datetime(prod_simbox_statut_om_acti['date_id'])
prod_simbox_statut_om_acti['date_id'] = prod_simbox_statut_om_acti['date_id'].dt.strftime('%Y%m%d')
prod_simbox_statut_om_acti['date_id'] = prod_simbox_statut_om_acti['date_id'].apply(str)
prod_simbox_statut_om_acti['msisdn'] = prod_simbox_statut_om_acti['msisdn'].apply(str)
prod_simbox_statut_om_acti.head()


prod_simbox_conso_data = pd.read_csv("/Users/youssouf/Downloads/prod_simbox_conso_data.csv", sep = ";")
prod_simbox_conso_data = prod_simbox_conso_data.rename(columns={'prod_simbox_conso_data.date_id': 'date_id', 'prod_simbox_conso_data.msisdn': 'msisdn', 'prod_simbox_conso_data.vol_data_conso': 'vol_data_conso'} )
prod_simbox_conso_data['date_id'] = pd.to_datetime(prod_simbox_conso_data['date_id'])
prod_simbox_conso_data['date_id'] = prod_simbox_conso_data['date_id'].dt.strftime('%Y%m%d')
prod_simbox_conso_data['date_id'] = prod_simbox_conso_data['date_id'].apply(str)
prod_simbox_conso_data['msisdn'] = prod_simbox_conso_data['msisdn'].apply(str)
prod_simbox_conso_data.head()


prod_ofnet = pd.read_csv("/Users/youssouf/Downloads/prod_ofnet.csv", sep = ";")
prod_ofnet = prod_ofnet.rename(columns={'prod_ofnet.date_id': 'date_id', 'prod_ofnet.msisdn': 'msisdn', 'prod_ofnet.nbr_dist_sort_of': 'nbr_dist_sort_of', 'prod_ofnet.nbr_call_sort_of' : 'nbr_call_sort_of', 'prod_ofnet.nbr_sms_sort_of':'nbr_sms_sort_of', 'prod_ofnet.duree_call_sort_tot_of' : 'duree_call_sort_tot_of', 'prod_ofnet.duree_call_sort_moy_of' : 'duree_call_sort_moy_of'} )
prod_ofnet['date_id'] = pd.to_datetime(prod_ofnet['date_id'])
prod_ofnet['date_id'] = prod_ofnet['date_id'].dt.strftime('%Y%m%d')
prod_ofnet['date_id'] = prod_ofnet['date_id'].apply(str)
prod_ofnet['msisdn'] = prod_ofnet['msisdn'].apply(str)
prod_ofnet.head()


prod_simbox_night = pd.read_csv("/Users/youssouf/Downloads/prod_simbox_night.csv", sep = ";")
prod_simbox_night = prod_simbox_night.rename(columns={'prod_simbox_night.date_id': 'date_id', 'prod_simbox_night.msisdn': 'msisdn', 'prod_simbox_night.nbr_dist_call_sort_night': 'nbr_dist_call_sort_night', 'prod_simbox_night.nbr_call_sort_night' : 'nbr_call_sort_night', 'prod_simbox_night.duree_tot_sort_night':'duree_tot_sort_night', 'prod_simbox_night.duree_moy_sort_night' : 'duree_moy_sort_night'} )
prod_simbox_night['date_id'] = pd.to_datetime(prod_simbox_night['date_id'])
prod_simbox_night['date_id'] = prod_simbox_night['date_id'].dt.strftime('%Y%m%d')
prod_simbox_night['date_id'] = prod_simbox_night['date_id'].apply(str)
prod_simbox_night['msisdn'] = prod_simbox_night['msisdn'].apply(str)
prod_simbox_night.head()


prod_simbox_complice = pd.read_csv("/Users/youssouf/Downloads/prod_simbox_complice.csv", sep = ";")
prod_simbox_complice = prod_simbox_complice.rename(columns={'prod_simbox_complice.date_id': 'date_id', 'prod_simbox_complice.msisdn': 'msisdn', 'prod_simbox_complice.nbr_dist_call_complice': 'nbr_dist_call_complice', 'prod_simbox_complice.nbr_call_tot_complice' : 'nbr_call_tot_complice', 'prod_simbox_complice.duree_tot_call_complice':'duree_tot_call_complice', 'prod_simbox_complice.duree_moy_call_complice' : 'duree_moy_call_complice'} )
prod_simbox_complice['date_id'] = pd.to_datetime(prod_simbox_complice['date_id'])
prod_simbox_complice['date_id'] = prod_simbox_complice['date_id'].dt.strftime('%Y%m%d')
prod_simbox_complice['date_id'] = prod_simbox_complice['date_id'].apply(str)
prod_simbox_complice['msisdn'] = prod_simbox_complice['msisdn'].apply(str)
prod_simbox_complice.head()


prod_simbox_rechargement_om = pd.read_csv("/Users/youssouf/Downloads/prod_simbox_rechargement_om.csv", sep = ";")
prod_simbox_rechargement_om = prod_simbox_rechargement_om.rename(columns={'prod_simbox_rechargement_om.date_id': 'date_id', 'prod_simbox_rechargement_om.msisdn': 'msisdn', 'prod_simbox_rechargement_om.nbr_tot_rechar_om': 'nbr_tot_rechar_om', 'prod_simbox_rechargement_om.rechar_om_tot' : 'rechar_om_tot', 'prod_simbox_rechargement_om.rechar_om_moy':'rechar_om_moy', 'prod_simbox_rechargement_om.rechar_om_med' : 'rechar_om_med'} )
prod_simbox_rechargement_om['date_id'] = pd.to_datetime(prod_simbox_rechargement_om['date_id'])
prod_simbox_rechargement_om['date_id'] = prod_simbox_rechargement_om['date_id'].dt.strftime('%Y%m%d')
prod_simbox_rechargement_om['date_id'] = prod_simbox_rechargement_om['date_id'].apply(str)
prod_simbox_rechargement_om['msisdn'] = prod_simbox_rechargement_om['msisdn'].apply(str)
prod_simbox_rechargement_om.head()


prod_simbox_om_send = pd.read_csv("/Users/youssouf/Downloads/prod_simbox_om_send.csv", sep = ";")
prod_simbox_om_send = prod_simbox_om_send.rename(columns={'prod_simbox_om_send.date_id': 'date_id', 'prod_simbox_om_send.msisdn': 'msisdn', 'prod_simbox_om_send.nbr_tot_transact_om_send': 'nbr_tot_transact_om_send', 'prod_simbox_om_send.transact_om_tot_send' : 'transact_om_tot_send', 'prod_simbox_om_send.transact_om_moy_send':'transact_om_moy_send', 'prod_simbox_om_send.transact_om_med_send' : 'transact_om_med_send'} )
prod_simbox_om_send['date_id'] = pd.to_datetime(prod_simbox_om_send['date_id'])
prod_simbox_om_send['date_id'] = prod_simbox_om_send['date_id'].dt.strftime('%Y%m%d')
prod_simbox_om_send['date_id'] = prod_simbox_om_send['date_id'].apply(str)
prod_simbox_om_send['msisdn'] = prod_simbox_om_send['msisdn'].apply(str)
prod_simbox_om_send.head()



result = pd.merge(prod_simbox_imei , prod_simbox_profil , on=["msisdn"])
result.head()


