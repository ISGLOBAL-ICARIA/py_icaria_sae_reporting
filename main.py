#!/usr/bin/env python
""" Python script to manage different components of the reporting of Serious Adverse Events (SAEs) in the ICARIA
Clinical Trial. These components are: (1) SAE numbering, etc."""

from datetime import datetime
import pandas
import redcap
import tokens

__author__ = "Maximo Ramirez Robles"
__copyright__ = "Copyright 2021, ISGlobal Maternal, Child and Reproductive Health"
__credits__ = ["Maximo Ramirez Robles"]
__license__ = "MIT"
__version__ = "0.0.1"
__date__ = "20210629"
__maintainer__ = "Maximo Ramirez Robles"
__email__ = "maximo.ramirez@isglobal.org"
__status__ = "Dev"

if __name__ == '__main__':
    URL = tokens.URL
    PROJECTS = tokens.REDCAP_PROJECTS

    for project_key in PROJECTS:
        project = redcap.Project(URL, PROJECTS[project_key])

        # Get all SAE records marked as completed (sae_complete = '2')
        print("[{}] Getting SAE records from {}...".format(datetime.now(), project_key))
        df_sae = project.export_records(
            format='df',
            fields=["sae_number", "sae_report_type", "sae_complete"],
            filter_logic="[sae_complete] = '2'"
        )

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
        for index, row in df.iterrows():
            if pandas.isna(row['sae_number']):
                sae_number = row['study_number']
                # First SAE report on the participant. SAE Number = Study Number-01
                if df.index.value_counts()[index] == 1:
                    sae_number = sae_number + "-01"
                # For the consecutive SAE reports, the SAE number will be the Study Number concatenated with the number
                # of initial reports
                # TODO: This is assuming that we are numbering just one initial report by script execution. If there are
                #       more than one initial reports to number. As it is, the script will use the same number for all
                #       the reports.
                else:
                    sae_number = sae_number + "-0" + str(df.loc[index].sae_report_type.value_counts()[1])

                record_dict = {
                    'record_id': index,
                    'redcap_event_name': 'adverse_events_arm_1',
                    'redcap_repeat_instrument': 'sae',
                    'redcap_repeat_instance': row['redcap_repeat_instance_x'],
                    'sae_number': sae_number
                }
                to_import.append(record_dict)

        # Import new created SAE Numbers
        print("[{}] Importing SAE Numbers into {}...".format(datetime.now(), project_key))
        response = project.import_records(to_import)
        print(response)
