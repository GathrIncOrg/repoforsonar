"""
This code is written by Kushagra Behere
on the  Date 21-08-2023
version 1.0.0
"""
import requests as re
import pandas as pd
import xml.etree.ElementTree as ET
from MessageSender import MessageSender, MessageType

klera_meta_out = {
    "Team Area History": {
        "DSTID": "TeamAreaHistory_V1.0.0",
        "Primary Key": {
            "isrowid": True,
            "isvisible": True,
            "datatype": "STRING"
        }
    }
}
klera_in_details = {
    "Instance_Url": {
        "description": "Instance Url",
        "datatype": ["STRING"],
        "argtype": "Param",
        "masked": False,
        "required": True
    },
    "Username": {
        "description": "Username",
        "datatype": ["STRING"],
        "argtype": "Param",
        "masked": False,
        "required": True
    },
    "Password": {
        "description": "Password",
        "datatype": ["STRING"],
        "argtype": "Param",
        "masked": True,
        "required": True
    },
    "Project_Area_Id": {
        "description": "Project Area Id",
        "datatype": ["STRING"],
        "argtype": "Data",
        "multiplerows": True,
        "masked": True,
        "required": True
    },
    "Team_Area_Id": {
        "description": "Team Area Id",
        "datatype": ["STRING"],
        "argtype": "Data",
        "multiplerows": True,
        "masked": True,
        "required": True
    },
    "Start_Time": {
        "description": "Start Time",
        "datatype": ["STRING"],
        "argtype": "Data",
        "multiplerows": True,
        "masked": True,
        "required": True
    }
}

for team_area_id, project_area_id, start_time in zip(Team_Area_Id, Project_Area_Id, Start_Time):
    flag=1
    url = Instance_Url+f"/process/project-areas/{project_area_id}/team-areas/{team_area_id}/history?startTime={start_time}&pageSize=100"
    auth=(Username,Password)
    headers={'OSLC-Core-Version':'2.0'}
    kleradict={}

    while flag:
        response = re.get(url,auth=auth, headers=headers)

        history = ET.fromstring(response.text)
        
        for key in list(history.attrib.keys()):
            if(key.rsplit('{',1)[1]=="next-page-url"):
                flag = 1
                url=history.attrib[key]
            else:
                flag=0

        for history_entry in history:
            for history_detail in history_entry:
                
                try:
                    count=1
                    for activity in history_detail:
                    
                        kleradict.setdefault("History Entry Date", []).append(str(history_entry.attrib[list(history_entry.attrib.keys())[0].rsplit('}',1)[0]+'}date']))
                
                        kleradict.setdefault("History Entry Username", []).append(history_entry.attrib[list(history_entry.attrib.keys())[0].rsplit('}',1)[0]+'}user-name'])
                        kleradict.setdefault("Project Area Id", []).append(project_area_id)
                        kleradict.setdefault("Team Area Id", []).append(team_area_id)
                        kleradict.setdefault("Searched Before", []).append(str(start_time))
                        kleradict.setdefault("Primary Key", []).append(history_detail.attrib[list(history_detail.attrib.keys())[0].rsplit('}',1)[0]+'}url']+str(count))
                        count=count+1

                        
                        try:
                            kleradict.setdefault("History Detail Label", []).append(history_detail.attrib[list(history_detail.attrib.keys())[0].rsplit('}',1)[0]+'}label'])
                        except:
                            kleradict.setdefault("History Detail Label", []).append(None)

                        try:
                            kleradict.setdefault("History Detail Url", []).append(history_detail.attrib[list(history_detail.attrib.keys())[0].rsplit('}',1)[0]+'}url'])
                        except:
                            kleradict.setdefault("History Detail Url", []).append(None)

                        try:
                            kleradict.setdefault("History Detail Type", []).append(history_detail.attrib[list(history_detail.attrib.keys())[0].rsplit('}',1)[0]+'}type'])
                        except:
                            kleradict.setdefault("History Detail Type", []).append(None)

                        try:
                            kleradict.setdefault("History Detail Deferred", []).append(history_detail.attrib[list(history_detail.attrib.keys())[0].rsplit('}',1)[0]+'}deferred'])
                        except:
                            kleradict.setdefault("History Detail Deferred", []).append(None)

        
                        try:
                            kleradict.setdefault("Operation Type",[]).append(activity.tag.rsplit('}',1)[1])
                        except:
                            kleradict.setdefault("Operation Type",[]).append(None)

                        try:
                            kleradict.setdefault("Title", []).append(activity.attrib[list(activity.attrib.keys())[0].rsplit('}',1)[0]+'}title'])
                        except:
                            kleradict.setdefault("Title", []).append(None)

                        try:
                            kleradict.setdefault('Value',[]).append(str(activity.text))
                        except:
                            kleradict.setdefault('Value',[]).append(None)

                        try:
                            kleradict.setdefault("Scope", []).append(activity.attrib[list(activity.attrib.keys())[0].rsplit('}',1)[0]+'}scope'])
                        except:
                            kleradict.setdefault("Scope", []).append(None)

                        temp_list=['Old Value','New Value','Difference']
                        for item in activity:
                            if(item.tag=="old-value"):
                                kleradict.setdefault("Old Value",[]).append(str(item.text))
                                temp_list.remove('Old Value')

                            elif(item.tag=="new-value"):
                                kleradict.setdefault("New Value",[]).append(str(item.text))
                                temp_list.remove('New Value')

                            elif(item.tag=="difference"):
                                kleradict.setdefault("Difference",[]).append(str(item.text))
                                temp_list.remove('Difference')
                        
                        for rem in temp_list:
                            kleradict.setdefault(rem,[]).append(None)

                        if(len(kleradict['Project Area Id'])>100):

                    
                            out_df = pd.DataFrame(data=kleradict)
                            out_dict = {'Team Area History': out_df}
                            klera_dst = [out_dict]
                            data_block = {}
                            data_block['klera_dst'] = klera_dst
                            data_block['klera_meta_out'] = klera_meta_out
                            klera_message_block = {}

                            klera_message_block['message_type'] = MessageType.DATA
                            klera_message_block['data_block'] = data_block

                            klera_message_sender.push_data_to_queue(klera_message_block)

                            kleradict.clear()

                except Exception as e:
                    pass

            


if(kleradict):
    out_df = pd.DataFrame(data=kleradict)
    out_dict = {'Team Area History': out_df}
    klera_dst = [out_dict]
    data_block = {}
    data_block['klera_dst'] = klera_dst
    data_block['klera_meta_out'] = klera_meta_out
    klera_message_block = {}

    klera_message_block['message_type'] = MessageType.DATA
    klera_message_block['data_block'] = data_block

    klera_message_sender.push_data_to_queue(klera_message_block)
    
    kleradict.clear()