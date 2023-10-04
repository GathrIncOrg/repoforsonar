"""
This code is written by Kushagra Behere
on the  Date 21-08-2023
version 1.0.1
"""
import requests as re
import pandas as pd
import xml.etree.ElementTree as ET
from MessageSender import MessageSender, MessageType

klera_meta_out = {
    "Team Areas": {
        "DSTID": "TeamAreas_V1.0.0",
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
    }
}

# Inputs #
# Instance_Url=""
# Username=""
# Password=""
# Project_Area_Id=""

kleradict={}
auth=(Username,Password)
headers={'OSLC-Core-Version':'2.0'}

for project_area_id in Project_Area_Id:
    url = Instance_Url+f"/process/project-areas/{project_area_id}/team-areas"

    key_list=['summary','description','url','modified','parent-url','archived','project-area-url']

    response = re.get(url,auth=auth,headers=headers)

    team_areas = ET.fromstring(response.text)

    for team_area in team_areas:
        temp_key_list=key_list[:]
        kleradict.setdefault('Project Area Id',[]).append(project_area_id)
        for item in team_area:
            if(item.tag.rsplit('}',1)[1] in temp_key_list):
                if(item.tag.rsplit('}',1)[1]=="url"):
                    kleradict.setdefault("Team Area Id",[]).append(item.text.rsplit('/',1)[1])
                    kleradict.setdefault('Primary Key',[]).append(project_area_id+item.text.rsplit('/',1)[1])

                kleradict.setdefault(item.tag.rsplit('}',1)[1],[]).append(item.text)
                temp_key_list.remove(item.tag.rsplit('}',1)[1])
        for rem in temp_key_list:
            kleradict.setdefault(rem,[]).append(None)
        if(len(kleradict['Project Area Id'])>100):
            out_df = pd.DataFrame(data=kleradict)
            out_dict = {'Team Areas': out_df}
            klera_dst = [out_dict]

            data_block = {}
            data_block['klera_dst'] = klera_dst
            data_block['klera_meta_out'] = klera_meta_out

            klera_message_block = {}

            klera_message_block['message_type'] = MessageType.DATA
            klera_message_block['data_block'] = data_block

            klera_message_sender.push_data_to_queue(klera_message_block)
            kleradict.clear()

if(kleradict):
    out_df = pd.DataFrame(data=kleradict)
    out_dict = {'Team Areas': out_df}
    klera_dst = [out_dict]
    kleradict.clear()
    