#!/usr/bin/env python
""" Python script to manage different components of the reporting of Serious Adverse Events (SAEs) in the ICARIA
Clinical Trial. These components are: (1) SAE numbering, etc."""

from datetime import datetime
import pandas
import redcap
import tokens

__author__ = "Maximo Ramirez"
__copyright__ = "Copyright 2024, ISGlobal Maternal, Child and Reproductive Health"
__credits__ = ["Maximo Ramirez","Andreu Bofill"]
__license__ = "MIT"
__version__ = "0.0.1"
__date__ = "20210629"
__maintainer__ = "Andreu Bofill"
__email__ = "andreu.bofill@isglobal.org"
__status__ = "Dev"

if __name__ == '__main__':
    URL = tokens.URL
    PROJECTS = tokens.REDCAP_PROJECTS

    for project_key in PROJECTS:
        print(project_key)
        project = redcap.Project(URL, PROJECTS[project_key])

        # Get all SAE records marked as completed (sae_complete = '2')
        print("[{}] Getting SAE records from {}...".format(datetime.now(), project_key))
        df_sae = project.export_records(
            format='df',
            fields=["sae_number", "sae_report_type", "sae_complete"],
            filter_logic="[sae_complete] = '2'"
        )

        if not df_sae.empty:
            # Get all Study Numbers with a registered SAE
            record_ids = df_sae.index.get_level_values('record_id')
            print("[{}] Getting Study Numbers of SAEs from {}...".format(datetime.now(), project_key))
            df_sn = project.export_records(
                format='df',
                records=list(record_ids.drop_duplicates()),
                fields=["study_number"],
                filter_logic="[study_number] != ''"
            )

            # Merge SAE data with Study Numbers to produce SAE Numbers
            df = df_sae.merge(df_sn, on='record_id')
            to_import = []
            previous_sae_number = ''
            previous_index = ''
            for index, row in df.iterrows():
                if index != previous_index:
                    previous_sae_number = ''

               # print(index, row['sae_number'])
                if pandas.isna(row['sae_number']) or "-" not in row['sae_number'].split("ICA-")[1]:
                    if previous_sae_number=='':
                        sae_number = row['study_number'] + "-01"
                    else:
                        if row['sae_report_type'] in [1,4]:
                            next_number = int(previous_sae_number.split("-")[2])+1

                        elif row['sae_report_type'] in [2,3]:
                            next_number = int(previous_sae_number.split("-")[2])
                        sae_number = str(row['study_number']) + "-0" + str(next_number)

                    record_dict = {
                        'record_id': index,
                        'redcap_event_name': 'adverse_events_arm_1',
                        'redcap_repeat_instrument': 'sae',
                        'redcap_repeat_instance': row['redcap_repeat_instance_x'],
                        'sae_number': sae_number
                    }
                    print (index)
                    to_import.append(record_dict)
                    previous_sae_number = sae_number
                else:
                    previous_sae_number = row['sae_number']
                previous_index=index

            # Import new created SAE Numbers
            print("[{}] Importing SAE Numbers into {}...".format(datetime.now(), project_key))
            response = project.import_records(to_import)
            print(response)
        else:
            print("[{}] No SAE completed into {}...".format(datetime.now(), project_key))