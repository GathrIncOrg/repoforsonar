"""
This code is written by Kushagra Behere
on the  Date 21-08-2023
version 1.0.1
"""
import requests as re
import pandas as pd
import xml.etree.ElementTree as ET
from MessageSender import MessageSender, MessageType

# Inputs #
# Instance_Url= ""
# Username= ""
# Password= ""
# Project_Area_Id= ""

klera_meta_out = {
    "Project Area Members": {
        "DSTID": "ProjectAreaMembers_V1.0.0",
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

kleradict={}
auth=(Username,Password)
headers={'OSLC-Core-Version':'2.0'}

for project_area_id in Project_Area_Id:

    url = Instance_Url+f"/process/project-areas/{project_area_id}/members"


    response = re.get(url,auth=auth,headers=headers)

    members = ET.fromstring(response.text)

    for member in members:
        user_url=""
        for item in member:
            if(item.tag.rsplit('}',1)[1]=="url"):
                user_url=item.text.rsplit('/',1)[1]
            if(item.tag.rsplit('}',1)[1]=="role-assignments"):
                for ra in item:
                    kleradict.setdefault('role',[]).append(ra[1].text.rsplit('/',1)[1])
                    kleradict.setdefault('Primary Key',[]).append(ra[0].text)
                    kleradict.setdefault('Username',[]).append(user_url)

                    kleradict.setdefault('Project Area Id',[]).append(project_area_id)
                    
        

        if(len(kleradict['Project Area Id'])>100):
            out_df = pd.DataFrame(data=kleradict)
            out_dict = {'Project Area Members': out_df}
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
    out_dict = {'Project Area Members': out_df}
    klera_dst = [out_dict]

    kleradict.clear()
